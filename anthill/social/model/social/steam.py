
from anthill.common.social.apis import SteamAPI
from .. social import SocialAPI


class SteamSocialAPI(SocialAPI, SteamAPI):
    def __init__(self, application, tokens, cache):
        SocialAPI.__init__(self, application, tokens, "steam", cache)
        SteamAPI.__init__(self, cache)

    async def call(self, gamespace, account_id, method, *args, **kwargs):
        """
        Makes steam API call.
        """

        private_key = await self.get_private_key(gamespace)
        kwargs["key"] = private_key.key
        result = await method(*args, **kwargs)
        return result

    async def get_social_profile(self, gamespace, username, account_id, env=None):
        user_info = await self.call(
            gamespace,
            account_id,
            self.api_get_user_info,
            username=username,
            env=env)

        return user_info

    def has_friend_list(self):
        return False

    async def import_social(self, gamespace, username, auth):
        raise NotImplementedError()
