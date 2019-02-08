# -*- coding: utf-8 -*-

from restapi import decorators as decorate
from b2stage.apis.commons.b2access import B2accessUtilities
from restapi.exceptions import RestApiException
from restapi.flask_ext.flask_irods.client import get_and_verify_irods_session
from utilities import htmlcodes as hcodes
from utilities.logs import get_logger

log = get_logger(__name__)


class B2safeProxy(B2accessUtilities):
    """ Login to B2SAFE: directly. """

    _anonymous_user = 'anonymous'

    @decorate.catch_error()
    def get(self):

        user = self.get_current_user()
        log.debug("Token user: %s" % user)

        if user.session is not None and len(user.session) > 0:
            log.info("Valid B2SAFE user: %s" % user.uuid)
        else:
            msg = "This user is not registered inside B2SAFE"
            raise RestApiException(
                msg, status_code=hcodes.HTTP_BAD_UNAUTHORIZED)

        icom = self.get_service_instance(
            service_name='irods',
            user_session=user,
        )
        icom.list()
        return 'validated'

    @decorate.catch_error()
    def post(self):

        #############
        from flask import request
        auth = request.authorization

        jargs = self.get_input()
        if auth is not None:
            username = auth.username
            password = auth.password
        else:
            username = jargs.pop('username', None)
            password = jargs.pop('password', None)
        authscheme = jargs.pop('authscheme', 'credentials')

        # token is an alias for password parmeter
        if password is None:
            password = jargs.pop('token', None)

        if len(jargs) > 0:
            for j in jargs:
                log.warning("Unknown input parameter: %s", j)

        if authscheme.upper() == 'PAM':
            authscheme = 'PAM'

        if username == self._anonymous_user:
            password = 'WHATEVERYOUWANT:)'

        if username is None or password is None \
           or username.strip() == '' or password.strip() == '':
            msg = "Missing username or password"
            raise RestApiException(
                msg, status_code=hcodes.HTTP_BAD_UNAUTHORIZED)

        if authscheme.upper() == 'OPENID':
            authscheme = 'PAM'
            imain = self.get_main_irods_connection()

            username = self.get_irods_user_from_b2access(imain, username)

        #############
        func = self.get_service_instance
        params = {
            'service_name': "irods",
            'user': username,
            'password': password,
            'authscheme': authscheme,
        }

        # we verify that irods connects with this credentials
        irods = get_and_verify_irods_session(func, params)
        if irods is None:
            msg = "Failed to authenticate on B2SAFE"
            raise RestApiException(
                msg, status_code=hcodes.HTTP_BAD_UNAUTHORIZED)
        else:
            encoded_session = irods.prc.serialize()

        token, irods_user = self.auth.irods_user(username, encoded_session)

        #############
        response = {
            'token': token,
        }
        imain = self.get_service_instance(service_name='irods')

        user_home = imain.get_user_home(irods_user)
        if imain.is_collection(user_home):
            response['b2safe_home'] = user_home
        else:
            response['b2safe_home'] = imain.get_user_home(append_user=False)
        response['b2safe_user'] = irods_user

        return self.force_response(defined_content=response)
