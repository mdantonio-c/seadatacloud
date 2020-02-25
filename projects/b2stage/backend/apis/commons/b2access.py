# -*- coding: utf-8 -*-

"""
B2ACCESS utilities
"""

import json
import requests
from flask import session
from base64 import b64encode
from datetime import datetime as dt
from flask_oauthlib.client import OAuthResponse
from urllib3.exceptions import HTTPError

from restapi.rest.definition import EndpointResource
from b2stage.apis.commons.oauth2clients import ExternalLogins, decorate_http_request

from restapi.utilities.htmlcodes import hcodes
from restapi.utilities.logs import log

# 12 h
IRODS_CONNECTION_TTL = 43200


class B2accessUtilities(EndpointResource):

    ext_auth = None

    def __init__(self):

        super(B2accessUtilities, self).__init__()
        if B2accessUtilities.ext_auth is None:
            from flask import current_app
            B2accessUtilities.ext_auth = ExternalLogins(current_app)
            log.info("OAuth2 initialized")

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
        return self.get_service_instance(
            service_name='irods', cache_expiration=IRODS_CONNECTION_TTL
        )

    def create_b2access_client(self, auth, decorate=False):
        """ Create the b2access Flask oauth2 object """

        b2access = B2accessUtilities.ext_auth._available_services.get('b2access')
        # B2ACCESS requires some fixes to make authorization work...
        if decorate:
            decorate_http_request(b2access)
        return b2access

    def request_b2access_token(self, b2access):
        """ Use b2access client to get a token for all necessary operations """
        resp = None
        b2a_token = None
        b2a_refresh_token = None

        try:
            resp = b2access.authorized_response()
        except json.decoder.JSONDecodeError as e:
            log.critical("B2ACCESS empty:\n{}\nCheck your app credentials", e)
            return (
                b2a_token,
                b2a_refresh_token,
                'Server misconfiguration: oauth2 failed',
            )
        except Exception as e:
            # raise e  # DEBUG
            log.critical("Failed to get authorized in B2ACCESS: {}", e)
            return (b2a_token, b2a_refresh_token, 'B2ACCESS OAUTH2 denied: {}'.format(e))
        if resp is None:
            return (b2a_token, b2a_refresh_token, 'B2ACCESS denied: unknown error')

        b2a_token = resp.get('access_token')
        if b2a_token is None:
            log.critical("No token received")
            return (b2a_token, 'B2ACCESS: empty token')
        log.info("Received token: '{}'", b2a_token)

        b2a_refresh_token = resp.get('refresh_token')
        log.info("Received refresh token: '{}'", b2a_refresh_token)
        return (b2a_token, b2a_refresh_token, tuple())

    def get_b2access_user_info(
        self, auth, b2access, b2access_token, b2access_refresh_token
    ):
        """ Get user info from current b2access token """

        # To use the b2access token with oauth2 client
        # We have to save it into session
        session['b2access_token'] = (b2access_token, '')

        # Calling with the oauth2 client
        b2access_user = b2access.get('userinfo')

        error = True
        if b2access_user is None:
            errstring = "Empty response from B2ACCESS"
        elif not isinstance(b2access_user, OAuthResponse):
            errstring = "Invalid response from B2ACCESS"
        elif b2access_user.status > hcodes.HTTP_TRESHOLD:
            log.error("Bad status: {}", str(b2access_user._resp))
            if b2access_user.status == hcodes.HTTP_BAD_UNAUTHORIZED:
                errstring = "B2ACCESS token obtained is unauthorized..."
            else:
                errstring = (
                    "B2ACCESS token obtained failed with {}".format(b2access_user.status)
                )
        elif isinstance(b2access_user._resp, HTTPError):
            errstring = "Error from B2ACCESS: {}".format(b2access_user._resp)
        elif not hasattr(b2access_user, 'data'):
            errstring = "Authorized response is invalid (missing data)"
        elif b2access_user.data.get('email') is None:
            errstring = "Authorized response is invalid (missing email)"
        else:
            error = False

        if error:
            return None, None, errstring

        # Attributes you find: http://j.mp/b2access_profile_attributes

        # Store b2access information inside the db
        intuser, extuser = self.store_oauth2_user(
            "b2access", b2access_user, b2access_token, b2access_refresh_token
        )
        # In case of error this account already existed...
        if intuser is None:
            error = "Failed to store access info"
            if extuser is not None:
                error = extuser
            return None, None, error

        log.info("Stored access info")

        # Get token expiration time
        response = b2access.get('tokeninfo')
        timestamp = response.data.get('exp')

        timestamp_resolution = 1
        # timestamp_resolution = 1e3

        # Convert into datetime object and save it inside db
        tok_exp = dt.fromtimestamp(int(timestamp) / timestamp_resolution)
        self.associate_object_to_attr(extuser, 'token_expiration', tok_exp)

        return b2access_user, intuser, extuser

    def store_oauth2_user(self, account_type, current_user, token, refresh_token):
        """
        Allow external accounts (oauth2 credentials)
        to be connected to internal local user
        """

        cn = None
        dn = None
        if isinstance(current_user, str):
            email = current_user
        else:
            try:
                values = current_user.data
            except BaseException:
                return None, "Authorized response is invalid"

            # print("TEST", values, type(values))
            if not isinstance(values, dict) or len(values) < 1:
                return None, "Authorized response is empty"

            email = values.get('email')
            cn = values.get('cn')
            ui = values.get('unity:persistent')

            # distinguishedName is only defined in prod, not in dev and staging
            # dn = values.get('distinguishedName')
            # DN very strange: the current key is something like 'urn:oid:2.5.4.49'
            # is it going to change?
            for key, _ in values.items():
                if 'urn:oid' in key:
                    dn = values.get(key)
            if dn is None:
                return None, "Missing DN from authorized response..."

        # Check if a user already exists with this email
        internal_users = self.auth.db.User.query.filter(
            self.auth.db.User.email == email
        ).all()

        # Should never happen, please
        if len(internal_users) > 1:
            log.critical("Multiple users?")
            return None, "Server misconfiguration"

        internal_user = None
        # If something found
        if len(internal_users) > 0:

            internal_user = internal_users.pop()
            log.debug("Existing internal user: {}", internal_user)
            # A user already locally exists with another authmethod. Not good.
            if internal_user.authmethod != account_type:
                return None, "User already exists, cannot store oauth2 data"
        # If missing, add it locally
        else:
            userdata = {
                # "uuid": getUUID(),
                "email": email,
                "authmethod": account_type
            }
            try:
                internal_user = self.create_user(userdata, [self.default_role])
                self.auth.db.session.commit()
                log.info("Created internal user {}", internal_user)
            except BaseException as e:
                log.error("Could not create internal user ({}), rolling back", e)
                self.auth.db.session.rollback()
                return None, "Server error"

        # Get ExternalAccount for the oauth2 data if exists
        external_user = self.auth.db.ExternalAccounts.query.filter_by(
            username=email
        ).first()
        # or create it otherwise
        if external_user is None:
            external_user = self.auth.db.ExternalAccounts(username=email, unity=ui)

            # Connect the external account to the current user
            external_user.main_user = internal_user
            # Note: for pre-production release
            # we allow only one external account per local user
            log.info("Created external user {}", external_user)

        # Update external user data to latest info received
        external_user.email = email
        external_user.account_type = account_type
        external_user.token = token
        external_user.refresh_token = refresh_token
        if cn is not None:
            external_user.certificate_cn = cn
        if dn is not None:
            external_user.certificate_dn = dn

        try:
            self.auth.db.session.add(external_user)
            self.auth.db.session.commit()
            log.debug("Updated external user {}", external_user)
        except BaseException as e:
            log.error("Could not update external user ({}), rolling back", e)
            self.auth.db.session.rollback()
            return None, "Server error"

        return internal_user, external_user

    # def oauth_from_token(self, token):
    #     extus = self.auth.db.ExternalAccounts.query.filter_by(token=token).first()
    #     intus = extus.main_user
    #     return intus, extus

    def oauth_from_local(self, internal_user):
        accounts = self.auth.db.ExternalAccounts
        return accounts.query.filter(
            accounts.main_user.has(id=internal_user.id)
        ).first()

    def refresh_b2access_token(self, auth, b2access_user, b2access, refresh_token):
        """
            curl -X POST
                 -u 'myClientID':'myClientSecret'
                 -d '
                     grant_type=refresh_token&
                     refresh_token=myRefreshToken&
                     scope=USER_PROFILE'
                  'https://unity.eudat-aai.fz-juelich.de/oauth2/token'

            OR -H "Authorization Basic base64(client_id:client_secret)" instead of -u
        """

        client_id = b2access._consumer_key
        client_secret = b2access._consumer_secret

        refresh_data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": 'USER_PROFILE',
        }
        auth_hash = b64encode(str.encode("{}:{}".format(client_id, client_secret))).decode(
            "ascii"
        )
        headers = {'Authorization': 'Basic {}'.format(auth_hash)}

        resp = requests.post(
            b2access.access_token_url, data=refresh_data, headers=headers
        )
        resp = resp.json()

        access_token = resp['access_token']

        # Store b2access information inside the db
        intuser, extuser = self.store_oauth2_user(
            "b2access", b2access_user, access_token, refresh_token
        )
        # In case of error this account already existed...
        if intuser is None:
            log.error("Failed to store new access token")
            return None

        log.info("New access token = {}", access_token)

        return access_token

    def get_irods_user_from_b2access(self, icom, email):
        """ EUDAT RULE for b2access-to-b2safe user mapping """

        inputs = {}
        body = """
            EUDATGetPAMusers(*json_map);
            writeLine("stdout", *json_map);
        """

        rule_output = icom.rule('get_pid', body, inputs, output=True)
        try:
            rule_output = json.loads(rule_output)
        except BaseException:
            log.error("Unable to convert rule output as json: {}", rule_output)
            return None

        for user in rule_output:
            if email in rule_output[user]:
                return user
        return None

    def send_errors(
        self, message=None, errors=None, code=None, headers=None,
        head_method=False, print_error=True
    ):
        """ Setup an error message """

        if errors is None:
            errors = []
        if isinstance(errors, str):
            errors = [errors]

        # See if we have the main message
        if message is not None:
            errors.append(message)

        if code is None or code < hcodes.HTTP_BAD_REQUEST:
            # default error
            code = hcodes.HTTP_SERVER_ERROR

        if print_error and errors:
            log.error(errors)

        if head_method:
            errors = None

        return self.force_response(
            errors=errors, code=code, headers=headers, head_method=head_method
        )
