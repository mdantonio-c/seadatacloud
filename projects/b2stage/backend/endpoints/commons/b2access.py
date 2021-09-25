"""
B2ACCESS utilities
"""
import os
from datetime import datetime
from typing import Dict, List, Optional

import pytz
from b2stage.connectors import irods
from restapi.rest.definition import EndpointResource, Response, ResponseContent
from restapi.utilities.logs import log


class B2accessUtilities(EndpointResource):

    ext_auth = None

    def __init__(self):

        super().__init__()

    def irods_user(self, username, session):

        user = self.auth.get_user(username)

        if user is not None:
            log.debug("iRODS user already cached: {}", username)
            user.session = session
        else:

            userdata = {
                "email": username,
                "name": username,
                # Password will not be used because the authmedos is `irods`
                "password": "not-used",
                "surname": "iCAT",
                "authmethod": "irods",
                "session": session,
            }
            user = self.auth.create_user(userdata, [self.auth.default_role])
            try:
                self.auth.db.session.commit()
                log.info("Cached iRODS user: {}", username)
            except BaseException as e:
                self.auth.db.session.rollback()
                log.error("Errors saving iRODS user: {}", username)
                log.error(str(e))
                log.error(type(e))

                user = self.auth.get_user(username)
                # Unable to do something...
                if user is None:
                    raise e
                user.session = session

        # token
        payload, full_payload = self.auth.fill_payload(user)
        token = self.auth.create_token(payload)
        now = datetime.now(pytz.utc)
        if user.first_login is None:
            user.first_login = now
        user.last_login = now
        try:
            self.auth.db.session.add(user)
            self.auth.db.session.commit()
        except BaseException as e:
            log.error("DB error ({}), rolling back", e)
            self.auth.db.session.rollback()

        self.auth.save_token(user, token, full_payload)

        return token, username

    def response(
        self,
        content: ResponseContent = None,
        errors: Optional[List[str]] = None,
        code: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        head_method: bool = False,
        allow_html: bool = False,
    ) -> Response:

        SEADATA_PROJECT = os.getenv("SEADATA_PROJECT", "0")
        # Locally apply the response wrapper, no longer available in the core

        if SEADATA_PROJECT == "0":

            return super().response(
                content=content,
                code=code,
                headers=headers,
                head_method=head_method,
                allow_html=allow_html,
            )

        if content is None:
            elements = 0
        elif isinstance(content, str):
            elements = 1
        else:
            elements = len(content)

        if errors is None:
            total_errors = 0
        else:
            total_errors = len(errors)

        if code is None:
            code = 200

        resp = {
            "Response": {"data": content, "errors": errors},
            "Meta": {
                "data_type": str(type(content)),
                "elements": elements,
                "errors": total_errors,
                "status": int(code),
            },
        }

        return super().response(
            content=resp,
            code=code,
            headers=headers,
            head_method=head_method,
            allow_html=allow_html,
        )

    def associate_object_to_attr(self, obj, key, value):
        try:
            setattr(obj, key, value)
            self.auth.db.session.commit()
        except BaseException as e:
            log.error("DB error ({}), rolling back", e)
            self.auth.db.session.rollback()
        return

    def get_main_irods_connection(self):
        # NOTE: Main API user is the key to let this happen
        return irods.get_instance()
