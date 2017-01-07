
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError

from common.internal import InternalError
from common.social import APIError, AuthResponse
from common.handler import AuthenticatedHandler
from common.access import scoped, AccessToken

from model.request import RequestError
from model.connection import ConnectionError
from model.social import SocialNotFound, NoFriendsFound, SocialAuthenticationRequired


class ConnectionsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def delete(self):
        target_accounts = filter(
            bool, 
            self.get_argument("target_accounts").split(","))

        try:
            yield self.application.connections.delete(
                self.token.account,
                target_accounts)
            
        except ConnectionError as e:
            raise HTTPError(500, e.message)

    @scoped()
    @coroutine
    def get(self):
        profile_fields = filter(
            bool, 
            self.get_argument("profile_fields", "").split(","))

        try:
            connections = yield self.application.connections.get_connections_profiles(
                self.token.get(AccessToken.GAMESPACE),
                self.token.account,
                profile_fields)
            
        except ConnectionError as e:
            raise HTTPError(500, e.message)

        self.dumps(connections)

    def options(self):
        self.set_header("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")

    @scoped()
    @coroutine
    def post(self):
        target_accounts = filter(
            bool,
            self.get_argument("target_accounts").split(","))

        approval = self.get_argument("approval", "true") == "true"

        if approval:
            try:
                yield self.application.requests.create(self.token.account, target_accounts)
            except RequestError as e:
                raise HTTPError(500, e.message)
        else:
            @scoped(scopes=["required_approval"])
            def create_connection():
                return self.application.connections.create(self.token.account, target_accounts)

            try:
                yield create_connection()
            except ConnectionError as e:
                raise HTTPError(500, e.message)


class ExternalHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):
        try:
            friends = yield self.application.social.list_friends(
                self.token.get(AccessToken.GAMESPACE),
                self.token.account)

        except SocialAuthenticationRequired as e:
            raise HTTPError(401, ujson.dumps({
                "credential": e.credential,
                "username": e.username
            }))

        except NoFriendsFound:
            raise HTTPError(404, "No connections found")

        except APIError as e:
            raise HTTPError(e.code, e.body)

        self.dumps(friends)


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @coroutine
    def attach_account(self, gamespace, credential, username, account):
        yield self.application.tokens.attach(
            gamespace,
            credential,
            username,
            account)

        try:
            api = self.application.social.api(credential)
        except SocialNotFound:
            raise InternalError(404, "No such credential: '{0}'.".format(credential))

        try:
            result = yield api.get_social_profile(gamespace, username, account)
        except APIError as e:
            raise InternalError(e.code, e.message)
        else:
            raise Return(result)

    @coroutine
    def import_social(self, gamespace, username, credential, auth):

        if not isinstance(auth, dict):
            raise InternalError(400, "Auth should be a dict")

        auth = AuthResponse(**auth)

        social = self.application.social
        try:
            api = social.api(credential)
        except SocialNotFound:
            raise InternalError(404, "No such credential: '{0}'.".format(credential))

        try:
            result = yield api.import_social(
                gamespace,
                username,
                auth)

        except APIError as e:
            raise HTTPError(e.code, e.message)
        else:
            raise Return(result)

    @coroutine
    def get_connections(self, gamespace, account_id, profile_fields):

        connections_data = self.application.connections

        try:
            connections = yield connections_data.get_connections_profiles(
                gamespace,
                account_id,
                profile_fields)

        except ConnectionError as e:
            raise HTTPError(500, e.message)

        raise Return(connections)
            

class RequestsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):

        profile_fields = filter(
            bool,
            self.get_argument("profile_fields", "").split(","))

        try:
            outbox_profiles = yield self.application.requests.list_inbox_profiles(
                self.token.account,
                profile_fields,
                self.token.get(AccessToken.GAMESPACE))

        except RequestError as e:
            raise HTTPError(500, e.message)

        self.dumps(outbox_profiles)

    @scoped()
    @coroutine
    def post(self):

        target_accounts = filter(
            bool,
            self.get_argument("target_accounts").split(","))

        action = self.get_argument("action")

        @coroutine
        def _accept():
            try:
                yield self.application.requests.accept(
                    self.token.account,
                    target_accounts,
                    self.application.connections)

            except RequestError as e:
                raise HTTPError(500, "Failed to accept a request: " + e.message)
            except ConnectionError as e:
                raise HTTPError(500, "Failed to accept a request: " + e.message)

        @coroutine
        def _reject():
            try:
                yield self.application.requests.reject_requests(
                    self.token.account,
                    target_accounts,
                    self.application.connections)
            except RequestError as e:
                raise HTTPError(500, "Failed to reject a request: " + e.message)

        actions = {
            "accept": _accept,
            "reject": _reject,
        }

        if action in actions:
            yield actions[action]()
        else:
            raise HTTPError(400, "No such action: " + action)


class RequestsSentHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def delete(self):

        target_accounts = filter(
            bool,
            self.get_argument("target_accounts").split(","))

        try:
            yield self.application.requests.delete(
                self.token.account,
                target_accounts)

        except RequestError as e:
            raise HTTPError(500, "Failed to cancel a request: " + e.message)

    @scoped()
    @coroutine
    def get(self):
        profile_fields = filter(
            bool,
            self.get_argument("profile_fields", "").split(","))

        try:
            outbox_profiles = yield self.application.requests.list_outbox_profiles(
                self.token.account,
                profile_fields,
                self.token.get(AccessToken.GAMESPACE))

        except RequestError as e:
            raise HTTPError(500, e.message)

        self.dumps(outbox_profiles)
