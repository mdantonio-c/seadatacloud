import os
from typing import Any, Dict, Optional

from flask import Response, stream_with_context
from irods import exception as iexceptions
from irods.access import iRODSAccess
from irods.rule import Rule
from irods.ticket import Ticket
from restapi.exceptions import RestApiException
from restapi.utilities.logs import log

DEFAULT_CHUNK_SIZE = 1_048_576


class IrodsException(RestApiException):
    pass


class IrodsPythonClient:

    prc: Any
    anonymous_user = "anonymous"

    def exists(self, path):
        if self.is_collection(path):
            return True
        if self.is_dataobject(path):
            return True
        return False

    def is_collection(self, path):
        try:
            return self.prc.collections.exists(path)
        except iexceptions.CAT_SQL_ERR as e:
            log.error("is_collection({}) raised CAT_SQL_ERR ({})", path, str(e))
            return False

    def is_dataobject(self, path):
        try:
            self.prc.data_objects.get(path)
            return True
        except iexceptions.CollectionDoesNotExist:
            return False
        except iexceptions.DataObjectDoesNotExist:
            return False

    def get_dataobject(self, path):
        try:
            return self.prc.data_objects.get(path)
        except (iexceptions.CollectionDoesNotExist, iexceptions.DataObjectDoesNotExist):
            raise IrodsException(f"{path} not found or no permissions")

    def list(
        self,
        path: Optional[str] = None,
        recursive: bool = False,
        detailed: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """List the files inside an iRODS path/collection"""

        if path is None:
            path = self.get_user_home()

        if self.is_dataobject(path):
            raise IrodsException("Cannot list a Data Object; you may get it instead.")

        try:
            data: Dict[str, Dict[str, Any]] = {}
            root = self.prc.collections.get(path)

            for coll in root.subcollections:

                row: Dict[str, Any] = {}
                key = coll.name
                row["name"] = coll.name
                row["objects"] = {}
                if recursive:
                    row["objects"] = self.list(
                        path=coll.path,
                        recursive=recursive,
                        detailed=detailed,
                    )
                row["path"] = os.path.dirname(coll.path)
                row["object_type"] = "collection"
                if detailed:
                    row["owner"] = "-"

                data[key] = row

            for obj in root.data_objects:

                row = {}
                key = obj.name
                row["name"] = obj.name
                row["path"] = os.path.dirname(obj.path)
                row["object_type"] = "dataobject"

                if detailed:
                    row["owner"] = obj.owner_name
                    row["content_length"] = obj.size
                    row["created"] = obj.create_time
                    row["last_modified"] = obj.modify_time

                data[key] = row

            return data
        except iexceptions.CollectionDoesNotExist:
            raise IrodsException(f"Not found (or no permission): {path}")

        # replicas = []
        # for line in lines:
        #     replicas.append(re.split("\s+", line.strip()))
        # return replicas

    def create_empty(self, path, directory=False, ignore_existing=False):

        if directory:
            return self.create_directory(path, ignore_existing)
        else:
            return self.create_file(path, ignore_existing)

    def create_directory(self, path, ignore_existing=False):

        # print("TEST", path, ignore_existing)
        try:

            ret = self.prc.collections.create(path, recurse=ignore_existing)
            log.debug("Created irods collection: {}", path)
            return ret

        except iexceptions.CAT_UNKNOWN_COLLECTION:
            raise IrodsException("Unable to create collection, invalid path")

        except iexceptions.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
            if not ignore_existing:
                raise IrodsException(
                    "Irods collection already exists",
                    status_code=409,
                )
            else:
                log.debug("Irods collection already exists: {}", path)

        except (iexceptions.CAT_NO_ACCESS_PERMISSION, iexceptions.SYS_NO_API_PRIV):
            raise IrodsException(f"You have no permissions on path {path}")

        return None

    def create_file(self, path, ignore_existing=False):

        try:

            ret = self.prc.data_objects.create(path)
            log.debug("Create irods object: {}", path)
            return ret

        except iexceptions.CAT_NO_ACCESS_PERMISSION:
            raise IrodsException("CAT_NO_ACCESS_PERMISSION")

        except iexceptions.SYS_INTERNAL_NULL_INPUT_ERR:
            raise IrodsException(f"Unable to create object, invalid path: {path}")

        except iexceptions.OVERWRITE_WITHOUT_FORCE_FLAG:
            if not ignore_existing:
                raise IrodsException("Irods object already exists", status_code=400)
            log.debug("Irods object already exists: {}", path)

        return False

    def put(self, local_path, irods_path):
        # NOTE: this action always overwrite
        return self.prc.data_objects.put(local_path, irods_path)

    def move(self, src_path, dest_path):

        try:
            if self.is_collection(src_path):
                self.prc.collections.move(src_path, dest_path)
                log.debug("Renamed collection: {}->{}", src_path, dest_path)
            else:
                self.prc.data_objects.move(src_path, dest_path)
                log.debug("Renamed irods object: {}->{}", src_path, dest_path)
        except iexceptions.CAT_RECURSIVE_MOVE:
            raise IrodsException("Source and destination path are the same")
        except iexceptions.SAME_SRC_DEST_PATHS_ERR:
            raise IrodsException("Source and destination path are the same")
        except iexceptions.CAT_NO_ROWS_FOUND:
            raise IrodsException("Invalid source or destination")
        except iexceptions.CAT_NAME_EXISTS_AS_DATAOBJ:
            # raised from both collection and data objects?
            raise IrodsException("Destination path already exists")
        except BaseException as e:
            log.error("{}({})", e.__class__.__name__, e)
            raise IrodsException("System error; failed to move.")

    def remove(self, path, recursive=False, force=False, resource=None):
        try:
            if self.is_collection(path):
                self.prc.collections.remove(path, recurse=recursive, force=force)
                log.debug("Removed irods collection: {}", path)
            else:
                self.prc.data_objects.unlink(path, force=force)
                log.debug("Removed irods object: {}", path)
        except iexceptions.CAT_COLLECTION_NOT_EMPTY:

            if recursive:
                raise IrodsException("Error deleting non empty directory")
            else:
                raise IrodsException(
                    "Cannot delete non empty directory without recursive flag"
                )
        except iexceptions.CAT_NO_ROWS_FOUND:
            raise IrodsException("Irods delete error: path not found")

        # FIXME: remove resource
        # if resource is not None:
        #     com = 'itrim'
        #     args = ['-S', resource]

        # Try with:
        # self.prc.resources.remove(name, test=dryRunTrueOrFalse)

    def write_file_content(self, path, content, position=0):
        try:
            obj = self.prc.data_objects.get(path)
            with obj.open("w+") as handle:

                if position > 0 and handle.seekable():
                    handle.seek(position)

                if handle.writable():

                    # handle.write('foo\nbar\n')
                    a_buffer = bytearray()
                    a_buffer.extend(map(ord, content))
                    handle.write(a_buffer)
                handle.close()
        except iexceptions.DataObjectDoesNotExist:
            raise IrodsException("Cannot write to file: not found")

    def open(self, absolute_path, destination):

        try:
            obj = self.prc.data_objects.get(absolute_path)

            # TODO: could use io package?
            with obj.open("r") as handle:
                with open(destination, "wb") as target:
                    for line in handle:
                        target.write(line)
            return True

        except iexceptions.DataObjectDoesNotExist:
            raise IrodsException("Cannot read path: not found or permssion denied")
        except iexceptions.CollectionDoesNotExist:
            raise IrodsException("Cannot read path: not found or permssion denied")
        return False

    ############################################
    # ############ ACL Management ##############
    ############################################

    def enable_inheritance(self, path: str, zone: Optional[str] = None) -> None:

        if zone is None:
            zone = self.get_current_zone()

        key = "inherit"
        ACL = iRODSAccess(access_name=key, path=path, user_zone=zone)
        try:
            self.prc.permissions.set(ACL)  # , recursive=False)
            log.debug("Enabled {} to {}", key, path)
        except iexceptions.CAT_INVALID_ARGUMENT:
            if not self.is_collection(path) and not self.is_dataobject(path):
                raise IrodsException("Cannot set Inherit: path not found")
            raise IrodsException("Cannot set Inherit")

    def create_collection_inheritable(self, ipath, user, permissions="own"):

        # Create the directory
        self.create_empty(ipath, directory=True, ignore_existing=True)
        # This user will own the directory
        self.set_permissions(ipath, permission=permissions, userOrGroup=user)
        # Let the permissions scale to subelements
        self.enable_inheritance(ipath)

    def set_permissions(
        self, path, permission=None, userOrGroup=None, zone=None, recursive=False
    ):

        if zone is None:
            zone = self.get_current_zone()

        # If not specified, remove permission
        if permission is None:
            permission = "null"

        try:

            ACL = iRODSAccess(
                access_name=permission, path=path, user_name=userOrGroup, user_zone=zone
            )
            self.prc.permissions.set(ACL, recursive=recursive)

            log.debug("Grant {}={} to {}", userOrGroup, permission, path)
            return True

        except iexceptions.CAT_INVALID_USER:
            raise IrodsException("Cannot set ACL: user or group not found")
        except iexceptions.CAT_INVALID_ARGUMENT:
            if not self.is_collection(path) and not self.is_dataobject(path):
                raise IrodsException("Cannot set ACL: path not found")
            else:
                raise IrodsException("Cannot set ACL")

        return False

    def get_user_home(self, user=None, append_user=True):

        zone = self.get_current_zone(prepend_slash=True)

        home = self.variables.get("home", "home")
        if home.startswith(zone):
            home = home[len(zone) :]
        home = home.lstrip("/")

        if not append_user:
            user = ""
        elif user is None:
            # current logged user
            user = self.prc.username

        return os.path.join(zone, home, user)

    def get_current_zone(self, prepend_slash=False, suffix=None):
        zone = self.prc.zone
        if prepend_slash or suffix:
            zone = f"/{zone}"
        if suffix:
            return f"{zone}/{suffix}"
        else:
            return zone

    def get_metadata(self, path):

        try:
            if self.is_collection(path):
                obj = self.prc.collections.get(path)
            else:
                obj = self.prc.data_objects.get(path)

            data = {}
            units = {}
            for meta in obj.metadata.items():
                name = meta.name
                data[name] = meta.value
                units[name] = meta.units

            return data, units
        except (iexceptions.CollectionDoesNotExist, iexceptions.DataObjectDoesNotExist):
            raise IrodsException("Cannot extract metadata, object not found")

    def remove_metadata(self, path, key):
        if self.is_collection(path):
            obj = self.prc.collections.get(path)
        else:
            obj = self.prc.data_objects.get(path)
        tmp = None
        for meta in obj.metadata.items():
            if key == meta.name:
                tmp = meta
                break
        # print(tmp)
        if tmp is not None:
            obj.metadata.remove(tmp)

    def set_metadata(self, path, **meta):
        try:
            if self.is_collection(path):
                obj = self.prc.collections.get(path)
            else:
                obj = self.prc.data_objects.get(path)

            for key, value in meta.items():
                obj.metadata.add(key, value)
        except iexceptions.CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
            raise IrodsException("This metadata already exist")
        except iexceptions.DataObjectDoesNotExist:
            raise IrodsException("Cannot set metadata, object not found")

    def rule(self, name, body, inputs, output=False):

        import textwrap

        # A bit completed to use {}.format syntax...
        rule_body = textwrap.dedent(
            """\
            %s {{
                %s
        }}"""
            % (name, body)
        )

        outname = None
        if output:
            outname = "ruleExecOut"
        myrule = Rule(self.prc, body=rule_body, params=inputs, output=outname)
        try:
            raw_out = myrule.execute()
        except BaseException as e:
            msg = f"Irule failed: {e.__class__.__name__}"
            log.error(msg)
            log.warning(e)
            raise e
        else:
            log.debug("Rule {} executed: {}", name, raw_out)

            # retrieve out buffer
            if output and len(raw_out.MsParam_PI) > 0:
                out_array = raw_out.MsParam_PI[0].inOutStruct
                # print("out array", out_array)

                import re

                file_coding = "utf-8"

                buf = out_array.stdoutBuf.buf
                if buf is not None:
                    # it's binary data (BinBytesBuf) so must be decoded
                    buf = buf.decode(file_coding)
                    buf = re.sub(r"\s+", "", buf)
                    buf = re.sub(r"\\x00", "", buf)
                    buf = buf.rstrip("\x00")
                    log.debug("Out buff: {}", buf)

                err_buf = out_array.stderrBuf.buf
                if err_buf is not None:
                    err_buf = err_buf.decode(file_coding)
                    err_buf = re.sub(r"\s+", "", err_buf)
                    log.debug("Err buff: {}", err_buf)

                return buf

            return raw_out

        #  EXAMPLE FOR IRULE: METADATA RULE
        # object_path = "/sdcCineca/home/httpadmin/tmp.txt"
        # test_name = 'paolo2'
        # inputs = {  # extra quotes for string literals
        #     '*object': f'"{object_path}"',
        #     '*name': f'"{test_name}"',
        #     '*value': f'"{test_name}"',
        # }
        # body = \"\"\"
        #     # add metadata
        #     *attribute.*name = *value;
        #     msiAssociateKeyValuePairsToObj(*attribute, *object, "-d")
        # \"\"\"
        # output = imain.irule('test', body, inputs, 'ruleExecOut')

    def ticket(self, path):
        ticket = Ticket(self.prc)
        # print("TEST", self.prc, path)
        ticket.issue("read", path)
        return ticket

    def ticket_supply(self, code):
        # use ticket for access
        ticket = Ticket(self.prc, code)
        ticket.supply()

    def test_ticket(self, path):
        # self.ticket_supply(code)

        try:
            with self.prc.data_objects.open(path, "r") as obj:
                log.debug(obj.__class__.__name__)
        except iexceptions.SYS_FILE_DESC_OUT_OF_RANGE:
            return False
        else:
            return True

    def stream_ticket(self, path, headers=None):
        obj = self.prc.data_objects.open(path, "r")
        return Response(
            stream_with_context(self.read_in_chunks(obj, DEFAULT_CHUNK_SIZE)),
            headers=headers,
        )

    # def list_tickets(self, user=None):

    #     try:
    #         data = self.prc.query(
    #             # models.Ticket.id,
    #             models.Ticket.string,
    #             models.Ticket.type,
    #             models.User.name,
    #             models.DataObject.name,
    #             models.Ticket.uses_limit,
    #             models.Ticket.uses_count,
    #             models.Ticket.expiration,
    #         ).all()
    #         # ).filter(User.name == user).one()

    #         # for obj in data:
    #         #     print("TEST", obj)
    #         #     # for _, grp in obj.items():

    #     except iexceptions.NoResultFound:
    #         return None
    #     else:
    #         return data
