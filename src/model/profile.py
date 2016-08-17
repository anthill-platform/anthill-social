
from tornado.gen import coroutine, Return

from common import cached
from common.internal import Internal, InternalError
from common.model import Model


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

    @coroutine
    def get_profiles(self, account_id, profile_ids, profile_fields, gamespace):

        if not profile_fields:

            result = [
                {
                    "account": account_id
                }
                for account_id in profile_ids
            ]

            raise Return(result)

        @cached(kv=self.cache,
                h=lambda: ProfilesModel.__cache_hash__(account_id, ",".join(profile_ids + profile_fields)),
                ttl=300,
                json=True)
        @coroutine
        def get_profiles():
            try:
                profiles = yield self.internal.request(
                    "profile",
                    "mass_profiles",
                    accounts=profile_ids,
                    profile_fields=profile_fields,
                    gamespace=gamespace,
                    action="get_public")

            except InternalError as e:
                raise ProfileRequestError(
                    "Failed to request profiles: " + e.body)

            raise Return(profiles)

        account_profiles = yield get_profiles()

        result = [
            {
                "account": account_id,
                "profile": profile
            }
            for account_id, profile in account_profiles.iteritems()
        ]

        raise Return(result)
