
import datetime

from tornado.gen import coroutine, Return

from common.social import APIError
from common.social.facebook import FacebookAPI, FacebookPrivateKey

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken

from common import to_int


class FacebookSocialAPI(SocialAPI, FacebookAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "facebook", cache)
        FacebookAPI.__init__(self, cache)

    @coroutine
    def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes facebook API call.
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
        data = token_data.payload

        try:
            if datetime.datetime.now() > expires_at:
                raise SocialAuthenticationRequired(self.credential_type, token_data.username)

            kwargs["access_token"] = access_token

            result = yield method(gamespace, *args, **kwargs)

        except APIError as e:
            if e.code == 401 or e.code == 400:
                raise SocialAuthenticationRequired(self.credential_type, token_data.username)
            raise e
        else:
            raise Return(result)

    @coroutine
    def list_friends(self, gamespace, account_id):
        friends = yield self.call(gamespace, account_id, self.api_get_friends)
        raise Return(friends)

    @coroutine
    def get_social_profile(self, gamespace, username, account_id):
        user_info = yield self.call(
            gamespace,
            account_id,
            self.api_get_user_info,
            fields="id,name,email,locale")

        raise Return(user_info)

    @coroutine
    def import_social(self, gamespace, username, auth):

        access_token = auth.access_token
        expires_in = to_int(auth.expires_in)
        data = {}

        result = yield self.import_data(
            gamespace,
            username,
            access_token,
            expires_in, data)

        raise Return(result)
