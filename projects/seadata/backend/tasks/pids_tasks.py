import os
from pathlib import Path
from typing import Dict, List

from restapi.connectors import redis, sqlalchemy
from restapi.connectors.celery import CeleryExt, Task
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods

TIMEOUT = 1800


def recursive_list_files(batch_path: str) -> List[str]:

    data: List[str] = []
    # list the content of the batch dir
    batch_dir_content = []
    path = Path(batch_path)
    for el in path.iterdir():
        batch_dir_content.append(el.name)
    for current in batch_dir_content:
        file = Path(batch_path, current)
        # log.debug(f"current: {current}, file: {file}")
        if file.is_file():
            data.append(str(file))
        else:
            data.extend(recursive_list_files(str(file)))

    return data


@CeleryExt.task(idempotent=False)
def cache_batch_pids(
    self: Task[[str], Dict[str, int]], batch_path: str
) -> Dict[str, int]:

    log.info("Task cache_batch_pids working on: {}", batch_path)

    stats = {
        "total": 0,
        "skipped": 0,
        "cached": 0,
        "errors": 0,
    }

    r = redis.get_instance().r

    try:
        data = recursive_list_files(batch_path)
        log.info("Found {} files", len(data))
    except BaseException as e:
        log.error(e)

    for file in data:

        stats["total"] += 1

        pid = r.get(file)
        if pid is not None:
            stats["skipped"] += 1
            log.debug(
                "{}: file {} already cached with PID: {}",
                stats["total"],
                file,
                pid,
            )
            self.update_state(state="PROGRESS", meta=stats)
            continue

        try:
            # get the PID from the database
            db = sqlalchemy.get_instance()
            # get the entry from the database
            dataobject = db.DataObject
            dataobject_entry = dataobject.query.filter(dataobject.path == file).first()

            if not dataobject_entry:
                stats["errors"] += 1
                log.warning(
                    "{}: file {} has not an entry in database",
                    stats["total"],
                    file,
                )
                self.update_state(state="PROGRESS", meta=stats)
                continue

            pid = dataobject_entry.uid
        except BaseException as e:
            log.error(e)

        if pid is None:
            stats["errors"] += 1
            log.warning(
                "{}: file {} has not a PID assigned",
                stats["total"],
                file,
                pid,
            )
            self.update_state(state="PROGRESS", meta=stats)
            continue

        r.set(pid, file)
        r.set(file, pid)
        log.debug("{}: file {} cached with PID {}", stats["total"], file, pid)
        stats["cached"] += 1
        self.update_state(state="PROGRESS", meta=stats)

    self.update_state(state="COMPLETED", meta=stats)
    log.info(stats)
    return stats


@CeleryExt.task(idempotent=False)
def inspect_pids_cache(self: Task[[], None]) -> None:

    log.info("Inspecting cache...")
    counter = 0
    cache: Dict[str, Dict[str, int]] = {}
    r = redis.get_instance().r

    for key in r.scan_iter("*"):
        folder = os.path.dirname(r.get(key))

        prefix = str(key).split("/")[0]
        if prefix not in cache:
            cache[prefix] = {}

        if folder not in cache[prefix]:
            cache[prefix][folder] = 1
        else:
            cache[prefix][folder] += 1

        counter += 1
        if counter % 10000 == 0:
            log.info("{} pids inspected...", counter)

    for prefix in cache:
        for pid_path in cache[prefix]:
            log.info(
                "{} pids with prefix {} from path: {}",
                cache[prefix][pid_path],
                prefix,
                pid_path,
            )
    log.info("Total PIDs found: {}", counter)
