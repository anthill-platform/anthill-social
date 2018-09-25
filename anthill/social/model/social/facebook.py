
import datetime

from anthill.common import to_int
from anthill.common.social import APIError
from anthill.common.social.apis import FacebookAPI

from .. social import SocialAPI, SocialAuthenticationRequired
from .. token import NoSuchToken


class FacebookSocialAPI(SocialAPI, FacebookAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "facebook", cache)
        FacebookAPI.__init__(self, cache)

    async def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes facebook API call.
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
        data = token_data.payload

        try:
            if datetime.datetime.now() > expires_at:
                raise SocialAuthenticationRequired(self.credential_type, token_data.username)

            kwargs["access_token"] = access_token

            result = await method(gamespace, *args, **kwargs)

        except APIError as e:
            if e.code == 401 or e.code == 400:
                raise SocialAuthenticationRequired(self.credential_type, token_data.username)
            raise e
        else:
            return result

    async def list_friends(self, gamespace, account_id):
        friends = await self.call(gamespace, account_id, self.api_get_friends)
        return friends

    def has_friend_list(self):
        return True

    async def get_social_profile(self, gamespace, username, account_id, env=None):
        user_info = await self.call(
            gamespace,
            account_id,
            self.api_get_user_info,
            fields="id,name,email,locale")

        return user_info

    async def import_social(self, gamespace, username, auth):

        access_token = auth.access_token
        expires_in = to_int(auth.expires_in)
        data = {}

        result = await self.import_data(
            gamespace,
            username,
            access_token,
            expires_in, data)

        return result
