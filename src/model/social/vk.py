
from tornado.gen import coroutine, Return
import datetime

from common.social import APIError
from common.social.vk import VKAPI

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken


class VKSocialAPI(SocialAPI, VKAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "vk", cache)
        VKAPI.__init__(self, cache)

    @coroutine
    def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes google API call.
        Validates everything, gathers tokens and then yields `method` with all information.
        """

        try:
            token_data = yield self.tokens.get_token(
                gamespace,
                account_id,
                self.credential_type)

        except NoSuchToken:
            raise SocialAuthenticationRequired(self.credential_type, None)

        expires_at = token_data.expires_at
        access_token = token_data.access_token

        if expires_at and datetime.datetime.now() > expires_at:
            raise APIError(403, "token expired")

        kwargs["access_token"] = access_token

        result = yield method(*args, **kwargs)

        raise Return(result)

    @coroutine
    def list_friends(self, gamespace, account_id):
        friends = yield self.call(
            gamespace,
            account_id,
            self.api_get_friends)

        raise Return(friends)

    @coroutine
    def get_social_profile(self, gamespace, username, account_id, env=None):
        user_info = yield self.call(
            gamespace,
            account_id,
            self.api_get_user_info)

        raise Return(user_info)

    @coroutine
    def import_social(self, gamespace, username, auth):

        access_token = auth.access_token
        expires_in = auth.expires_in

        data = {}

        result = yield self.import_data(
            gamespace,
            username,
            access_token,
            expires_in,
            data)

        raise Return(result)
