
from anthill.common.social.apis import MailRuAPI

from .. social import SocialAPI


class MailRuSocialAPI(SocialAPI, MailRuAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "mailru", cache)
        MailRuAPI.__init__(self, cache)

    def has_friend_list(self):
        return False

    async def get_social_profile(self, gamespace, username, account_id, env=None):

        private_key = await self.get_private_key(gamespace)
        user_info = await self.api_get_user_info(username, private_key)

        return user_info

    async def import_social(self, gamespace, username, auth):

        access_token = auth.access_token
        expires_in = auth.expires_in

        data = {}

        result = await self.import_data(
            gamespace,
            username,
            access_token,
            expires_in,
            data)

        return result
