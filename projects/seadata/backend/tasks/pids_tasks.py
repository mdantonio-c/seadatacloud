import os
from pathlib import Path
from typing import Dict, List

from celery.app.task import Task
from restapi.connectors import redis
from restapi.connectors.celery import CeleryExt
from restapi.utilities.logs import log
from restapi.utilities.processes import start_timeout, stop_timeout
from seadata.connectors import irods

TIMEOUT = 180


def recursive_list_files(imain: irods.IrodsPythonExt, irods_path: str) -> List[str]:

    data: List[str] = []
    for current in imain.list(irods_path):
        ifile = str(Path(irods_path, current))
        if imain.is_dataobject(ifile):
            data.append(ifile)
        else:
            data.extend(recursive_list_files(imain, ifile))

    return data


@CeleryExt.task()
def cache_batch_pids(self: Task, irods_path: str) -> Dict[str, int]:

    log.info("Task cache_batch_pids working on: {}", irods_path)

    stats = {
        "total": 0,
        "skipped": 0,
        "cached": 0,
        "errors": 0,
    }

    r = redis.get_instance().r
    with irods.get_instance() as imain:

        try:
            start_timeout(TIMEOUT)
            data = recursive_list_files(imain, irods_path)
            log.info("Found {} files", len(data))
            stop_timeout()
        except BaseException as e:
            log.error(e)

        for ifile in data:

            stats["total"] += 1

            pid = r.get(ifile)
            if pid is not None:
                stats["skipped"] += 1
                log.debug(
                    "{}: file {} already cached with PID: {}",
                    stats["total"],
                    ifile,
                    pid,
                )
                self.update_state(state="PROGRESS", meta=stats)
                continue

            try:
                start_timeout(TIMEOUT)
                metadata = imain.get_metadata(ifile)
                pid = metadata.get("PID")
                stop_timeout()
            except BaseException as e:
                log.error(e)

            if pid is None:
                stats["errors"] += 1
                log.warning(
                    "{}: file {} has not a PID assigned",
                    stats["total"],
                    ifile,
                    pid,
                )
                self.update_state(state="PROGRESS", meta=stats)
                continue

            r.set(pid, ifile)
            r.set(ifile, pid)
            log.debug("{}: file {} cached with PID {}", stats["total"], ifile, pid)
            stats["cached"] += 1
            self.update_state(state="PROGRESS", meta=stats)

        self.update_state(state="COMPLETED", meta=stats)
        log.info(stats)
    return stats


@CeleryExt.task()
def inspect_pids_cache(self: Task) -> None:

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
