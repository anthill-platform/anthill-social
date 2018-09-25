
import datetime

from anthill.common.social import APIError
from anthill.common.social.apis import VKAPI

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken


class VKSocialAPI(SocialAPI, VKAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "vk", cache)
        VKAPI.__init__(self, cache)

    async def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes google API call.
        Validates everything, gathers tokens and then awaits `method` with all information.
        """

        try:
            token_data = await self.tokens.get_token(
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

        result = await method(*args, **kwargs)

        return result

    async def list_friends(self, gamespace, account_id):
        friends = await self.call(
            gamespace,
            account_id,
            self.api_get_friends)

        return friends

    def has_friend_list(self):
        return True

    async def get_social_profile(self, gamespace, username, account_id, env=None):
        user_info = await self.call(
            gamespace,
            account_id,
            self.api_get_user_info)

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
