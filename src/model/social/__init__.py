
from tornado.gen import coroutine, Return

from common.internal import Internal, InternalError

from common import cached
from .. token import NoSuchToken
from abc import ABCMeta, abstractmethod

import time
import datetime
import logging


class SocialAuthenticationRequired(Exception):
    def __init__(self, credential, username):
        self.credential = credential
        self.username = username


class NoFriendsFound(Exception):
    pass


class SocialAPI(object):

    __metaclass__ = ABCMeta

    def __init__(self, application, tokens, credential_type, cache):
        self.application = application
        self.tokens = tokens
        self.credential_type = credential_type
        self.cache = cache
        self.internal = Internal()

    @coroutine
    def list_friends(self, gamespace, account_id):
        raise NotImplementedError()

    @coroutine
    def get_social_profile(self, gamespace, username, account_id):
        raise NotImplementedError()

    @coroutine
    def import_social(self, gamespace, username, auth):
        raise NotImplementedError()

    @coroutine
    def import_data(self, gamespace, username, access_token, expires_in, data):
        expires_at = datetime.datetime.fromtimestamp(int(time.time()) + expires_in)

        account = yield self.tokens.update_token(
            gamespace,
            self.credential_type,
            username,
            access_token,
            expires_at,
            data)

        result = {
            "account": account
        }
        raise Return(result)

    @abstractmethod
    def new_private_key(self, data):
        pass

    @coroutine
    def get_private_key(self, gamespace, data=None):
        """
        Looks for a key from login service.
        """

        if not data:
            key_name = self.type()

            @cached(kv=self.cache,
                    h=lambda: "auth_key:" + str(gamespace) + ":" + key_name,
                    ttl=300,
                    json=True)
            @coroutine
            def get():
                logging.info("Looking for key '{0}' in gamespace @{1}".format(key_name, gamespace))

                key_data = yield self.internal.request(
                    "login", "get_key", gamespace=gamespace, key_name=key_name)

                raise Return(key_data)

            data = yield get()

        raise Return(self.new_private_key(data))

    def type(self):
        return self.credential_type


class SocialAPIModel(object):
    def __init__(self, application, tokens, cache):
        self.tokens = tokens
        self.apis = {}
        self.init(application, tokens, cache)

    def api(self, api):

        if api not in self.apis:
            raise SocialNotFound()

        return self.apis[api]

    @coroutine
    def list_friends(self, gamespace, account_id):

        try:
            account_tokens = yield self.tokens.list_tokens(
                gamespace,
                account_id)

        except NoSuchToken:
            raise NoFriendsFound()

        friends = []

        for account_token in account_tokens:
            credential = account_token.credential

            api = self.api(credential)
            api_friends = yield api.list_friends(gamespace, account_id)

            for friend in api_friends:
                friend["credential"] = credential

            friends.extend(api_friends)

        raise Return(friends)

    def init(self, application, tokens, cache):

        import google
        import facebook
        import steam

        self.register(google.GoogleSocialAPI(application, tokens, cache))
        self.register(facebook.FacebookSocialAPI(application, tokens, cache))
        self.register(steam.SteamSocialAPI(application, tokens, cache))

    def register(self, api, credential=None):
        if credential is None:
            credential = api.type()

        self.apis[credential] = api


class SocialNotFound(Exception):
    pass
