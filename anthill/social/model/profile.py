
from anthill.common import cached
from anthill.common.internal import Internal, InternalError
from anthill.common.model import Model


class ProfileRequestError(Exception):
    def __init__(self, message):
        self.message = message


class ProfilesModel(Model):
    @staticmethod
    def __cache_hash__(account_id, data):
        return "profiles_" + str(account_id) + "_" + ("%x" % hash(data))

    def __init__(self, db, cache):
        self.db = db
        self.cache = cache
        self.internal = Internal()

    async def get_profiles(self, account_id, profile_ids, profile_fields, gamespace):

        if not profile_fields:

            result = [
                {
                    "account": account_id
                }
                for account_id in profile_ids
            ]

            return result

        @cached(kv=self.cache,
                h=lambda: ProfilesModel.__cache_hash__(account_id, ",".join(profile_ids + profile_fields)),
                ttl=300,
                json=True)
        async def get_profiles():
            try:
                profiles = await self.internal.request(
                    "profile",
                    "mass_profiles",
                    accounts=profile_ids,
                    profile_fields=profile_fields,
                    gamespace=gamespace,
                    action="get_public")

            except InternalError as e:
                raise ProfileRequestError(
                    "Failed to request profiles: " + e.body)

            return profiles

        account_profiles = await get_profiles()

        result = [
            {
                "account": account_id,
                "profile": profile
            }
            for account_id, profile in account_profiles.items()
        ]

        return result
