from restapi.connectors import sqlalchemy
from restapi.env import Env
from restapi.services.authentication import Role
from restapi.utilities.logs import log
from restapi.utilities.uuid import getUUID


class Initializer:
    def __init__(self) -> None:

        sql = sqlalchemy.get_instance()

        users = Env.get("SEADATA_PRIVILEGED_USERS", "").replace(" ", "").split(",")

        roles = [Role.USER, Role.STAFF]
        if not users:  # pragma: no cover
            log.info("No privileged user found")
        else:
            for username in users:
                if username:
                    try:
                        log.info("Creating user {}", username)
                        userdata = {
                            "uuid": getUUID(),
                            "email": username,
                            "name": username,
                            # password parameters will not be used witj b2safe users
                            "password": username,
                            "surname": "iCAT",
                            "authmethod": "irods",
                        }
                        user = sql.User(**userdata)
                        for r in roles:
                            role = sql.Role.query.filter_by(name=r.value).first()
                            user.roles.append(role)
                        sql.session.add(user)
                        sql.session.commit()
                        log.info("User {} created with roles: {}", username, roles)
                    except BaseException as e:
                        log.error("Errors creating user {}: {}", username, str(e))

    # This method is called after normal initialization if TESTING mode is enabled
    def initialize_testing_environment(self) -> None:
        pass
