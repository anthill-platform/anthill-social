
from tornado.gen import multi

from anthill.common.internal import Internal
from anthill.common.social import APIError
from anthill.common import cached

from .. token import SocialTokensError

import time
import datetime
import hashlib


class SocialAuthenticationRequired(Exception):
    def __init__(self, credential, username):
        self.credential = credential
        self.username = username


class NoFriendsFound(Exception):
    pass


class SocialAPI(object):

    def __init__(self, application, tokens, credential_type, cache):
        self.application = application
        self.tokens = tokens
        self.credential_type = credential_type
        self.cache = cache
        self.internal = Internal()

    async def list_friends(self, gamespace, account_id):
        raise NotImplementedError()

    def has_friend_list(self):
        return False

    async def get_social_profile(self, gamespace, username, account_id, env=None):
        raise NotImplementedError()

    async def import_social(self, gamespace, username, auth):
        raise NotImplementedError()

    async def import_data(self, gamespace, username, access_token, expires_in, data):
        expires_at = datetime.datetime.fromtimestamp(int(time.time()) + expires_in) if expires_in else None

        account = await self.tokens.update_token(
            gamespace,
            self.credential_type,
            username,
            access_token,
            expires_at,
            data)

        result = {
            "account": account
        }
        return result

    def type(self):
        return self.credential_type


class SocialAPIModel(object):
    def __init__(self, application, tokens, connections, cache):
        self.tokens = tokens
        self.connections = connections
        self.apis = {}
        self.cache = cache
        self.internal = Internal()
        self.init(application, tokens, cache)

    def api(self, api):

        if api not in self.apis:
            raise SocialNotFound()

        return self.apis[api]

    async def list_friends(self, gamespace, account_id, profile_fields=None):
        try:
            account_tokens = await self.tokens.list_tokens(
                gamespace,
                account_id)

        except SocialTokensError as e:
            raise APIError(500, e.message)

        calls = {}

        for account_token in account_tokens:
            credential_type = account_token.credential
            api = self.api(credential_type)

            if not api.has_friend_list():
                continue

            calls[credential_type] = api.list_friends(gamespace, account_id)

        @cached(kv=self.cache,
                h=lambda: "friends:" + str(gamespace) + ":" + str(account_id) +
                          ((":" + hashlib.sha256(",".join(profile_fields)).hexdigest()) if profile_fields else ""),
                ttl=300,
                json=True)
        async def do_request():

            if calls:
                friends_result = {}

                api_friends = await multi(calls, quiet_exceptions=(APIError,))

                for credential_type_, friends_ in api_friends.items():
                    for username, friend in friends_.items():
                        friends_result[credential_type_ + ":" + str(username)] = friend

                try:
                    credentials_to_accounts = await self.tokens.lookup_accounts(
                        gamespace, list(friends_result.keys()))
                except SocialTokensError as e2:
                    raise APIError(500, e2.message)

                account_ids = credentials_to_accounts.values()
            else:
                credentials_to_accounts = {}
                account_ids = []

            internal_connections = await self.connections.list_connections(account_id)

            account_ids.extend(internal_connections)

            account_profiles = await self.internal.request(
                "profile", "mass_profiles",
                accounts=list(set(account_ids)),
                gamespace=gamespace,
                action="get_public",
                profile_fields=profile_fields)

            def process_id(credential_account_id, credentials):
                result = {
                    "credentials": {
                        credential: {
                            "social": friends_result.get(credential, {})
                        }
                        for credential in credentials
                    }
                }

                if account_profiles:
                    result["profile"] = account_profiles.get(credential_account_id, {})

                return result

            ids_credentials = {}

            for internal_connection in internal_connections:
                existing = ids_credentials.get(internal_connection, None)
                if not existing:
                    ids_credentials[internal_connection] = []

            for credential_, account_id_ in credentials_to_accounts.items():
                existing = ids_credentials.get(account_id_, None)
                if existing:
                    existing.append(credential_)
                else:
                    ids_credentials[account_id_] = [credential_]

            return {
                str(account_id_): process_id(account_id_, credentials_)
                for account_id_, credentials_ in ids_credentials.items()
            }

        friends = await do_request()
        return friends

    def init(self, application, tokens, cache):

        from . import google
        from . import facebook
        from . import steam
        from . import vk
        from . import mailru

        self.register(google.GoogleSocialAPI(application, tokens, cache))
        self.register(facebook.FacebookSocialAPI(application, tokens, cache))
        self.register(vk.VKSocialAPI(application, tokens, cache))
        self.register(steam.SteamSocialAPI(application, tokens, cache))
        self.register(mailru.MailRuSocialAPI(application, tokens, cache))

    def register(self, api, credential=None):
        if credential is None:
            credential = api.type()

        self.apis[credential] = api


class SocialNotFound(Exception):
    pass
