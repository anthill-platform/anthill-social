
from tornado.gen import coroutine, Return
import datetime

from common.social import APIError
from common.social.steam import SteamAPI, SteamPrivateKey

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken


class SteamSocialAPI(SocialAPI, SteamAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "steam", cache)
        SteamAPI.__init__(self, cache)

    @coroutine
    def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes steam API call.
        """
        private_key = yield self.get_private_key(gamespace)
        kwargs["key"] = private_key.key
        result = yield method(*args, **kwargs)
        raise Return(result)

    @coroutine
    def get_social_profile(self, gamespace, username, account_id):
        user_info = yield self.call(
            gamespace,
            account_id,
            self.api_get_user_info,
            username=username)

        raise Return(user_info)

    @coroutine
    def import_social(self, gamespace, username, auth):
        raise NotImplementedError()
