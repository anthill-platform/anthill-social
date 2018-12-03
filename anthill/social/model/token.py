
from anthill.common.database import DatabaseError
from anthill.common.model import Model

import ujson


class SocialTokensError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class NoSuchToken(Exception):
    pass


class SocialTokenAdapter(object):
    def __init__(self, data):

        import logging
        logging.info(ujson.dumps(data))

        self.account = data.get("account_id")
        self.credential = data.get("credential")
        self.username = data.get("username")
        self.access_token = data.get("access_token")
        self.expires_at = data.get("expires_at")
        self.payload = data.get("payload")


class SocialTokensModel(Model):
    def __init__(self, db):
        self.db = db

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["credential_tokens"]

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            if gamespace_only:
                await self.db.execute(
                    """
                        DELETE FROM `credential_tokens`
                        WHERE `gamespace_id`=%s AND `account_id` IN %s;
                    """, gamespace, accounts)
            else:
                await self.db.execute(
                    """
                        DELETE FROM `credential_tokens`
                        WHERE `account_id` IN %s;
                    """, accounts)
        except DatabaseError as e:
            raise SocialTokensError("Failed to delete saved tokens: " + e.args[1])

    async def attach(self, gamespace_id, credential, username, account):
        try:
            merged = str(credential) + ":" + str(username)
            await self.db.execute(
                """
                    UPDATE `credential_tokens`
                    SET `account_id`=%s
                    WHERE `merged_credential`=%s AND `gamespace_id`=%s;
                """, account, merged, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to attach account: " + e.args[1])

    async def get_token(self, gamespace_id, account_id, credential):
        try:
            token = await self.db.get(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `account_id`=%s AND `credential`=%s AND `gamespace_id`=%s;
                """, account_id, credential, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to get social token: " + e.args[1])

        if not token:
            raise NoSuchToken()

        return SocialTokenAdapter(token)

    async def get_credential(self, gamespace_id, credential, username):

        try:
            merged = str(credential) + ":" + str(username)
            token = await self.db.get(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `merged_credential`=%s AND `gamespace_id`=%s
                """, merged, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to get token credential: " + e.args[1])

        if not token:
            raise NoSuchToken()

        return SocialTokenAdapter(token)

    async def lookup_accounts(self, gamespace_id, credentials):

        if not len(credentials):
            return {}

        try:
            tokens = await self.db.query(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `merged_credential` IN %s AND `gamespace_id`=%s
                """, credentials, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to get token credential: " + e.args[1])

        return {
            str(token["merged_credential"]): str(token["account_id"])
            for token in tokens
        }

    async def list_tokens(self, gamespace_id, account_id):

        try:
            tokens = await self.db.query(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `account_id`=%s AND `gamespace_id`=%s
              """, account_id, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed get tokens: " + e.args[1])

        return list(map(SocialTokenAdapter, tokens))

    async def update_token(self, gamespace_id, credential, username, access_token, expires_at, data):

        merged = str(credential) + ":" + str(username)

        try:
            old_token = await self.get_credential(gamespace_id, credential, username)
            account = old_token.account
        except NoSuchToken:
            data_text = ujson.dumps(data or {})

            try:
                await self.db.insert(
                    """
                        INSERT INTO `credential_tokens`
                        (`credential`, `username`, `access_token`, `expires_at`, `payload`, `gamespace_id`,
                        `merged_credential`)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, credential, username, access_token, expires_at, data_text, gamespace_id, merged)

            except DatabaseError as e:
                raise SocialTokensError("Failed to save token: " + e.args[1])

            return None
        else:
            old_data = old_token.payload or {}
            old_data.update(data or {})
            data_text = ujson.dumps(old_data)

            try:
                await self.db.execute(
                    """
                        UPDATE `credential_tokens`
                        SET `access_token`=%s, `expires_at`=%s, `payload`=%s
                        WHERE `merged_credential`=%s AND `gamespace_id`=%s;
                    """, access_token, expires_at, data_text, merged, gamespace_id)

            except DatabaseError as e:
                raise SocialTokensError("Failed to save token: " + e.args[1])

            return account
