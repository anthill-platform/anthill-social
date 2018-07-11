
from tornado.gen import coroutine, Return
import datetime

from common.social import APIError
from common.social.apis import MailRuAPI

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken


class MailRuSocialAPI(SocialAPI, MailRuAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "mailru", cache)
        MailRuAPI.__init__(self, cache)

    def has_friend_list(self):
        return False

    @coroutine
    def get_social_profile(self, gamespace, username, account_id, env=None):

        private_key = yield self.get_private_key(gamespace)
        user_info = yield self.api_get_user_info(username, private_key)

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
