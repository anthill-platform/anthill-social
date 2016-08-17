
import ujson

from tornado.gen import coroutine, Return
from common.database import DatabaseError
from common.model import Model


class SocialTokensError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


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

    @coroutine
    def attach(self, gamespace_id, credential, username, account):
        try:
            yield self.db.execute(
                """
                    UPDATE `credential_tokens`
                    SET `account_id`=%s
                    WHERE `credential`=%s AND `username`=%s AND `gamespace_id`=%s;
                """, account, credential, username, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to attach account: " + e.args[1])

    @coroutine
    def get_token(self, gamespace_id, account_id, credential):
        try:
            token = yield self.db.get(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `account_id`=%s AND `credential`=%s AND `gamespace_id`=%s;
                """, account_id, credential, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to get social token: " + e.args[1])

        if not token:
            raise NoSuchToken()

        raise Return(SocialTokenAdapter(token))

    @coroutine
    def get_credential(self, gamespace_id, credential, username):

        try:
            token = yield self.db.get(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `credential`=%s AND `username`=%s AND `gamespace_id`=%s
                """, credential, username, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed to get token credential: " + e.args[1])

        if not token:
            raise NoSuchToken()

        raise Return(SocialTokenAdapter(token))

    @coroutine
    def list_tokens(self, gamespace_id, account_id):

        try:
            tokens = yield self.db.query(
                """
                    SELECT *
                    FROM `credential_tokens`
                    WHERE `account_id`=%s AND `gamespace_id`=%s
              """, account_id, gamespace_id)

        except DatabaseError as e:
            raise SocialTokensError("Failed get tokens: " + e.args[1])

        if not tokens:
            raise NoSuchToken()

        raise Return(map(SocialTokenAdapter, tokens))

    @coroutine
    def update_token(self, gamespace_id, credential, username, access_token, expires_at, data):

        try:
            old_token = yield self.get_credential(gamespace_id, credential, username)
            account = old_token.account
        except NoSuchToken:
            data_text = ujson.dumps(data or {})

            try:
                yield self.db.insert(
                    """
                        INSERT INTO `credential_tokens`
                        (`credential`, `username`, `access_token`, `expires_at`, `payload`, `gamespace_id`)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, credential, username, access_token, expires_at, data_text, gamespace_id)

            except DatabaseError as e:
                raise SocialTokensError("Failed to save token: " + e.args[1])

            raise Return(None)
        else:
            old_data = old_token.payload or {}
            old_data.update(data or {})
            data_text = ujson.dumps(old_data)

            try:
                yield self.db.execute(
                    """
                        UPDATE `credential_tokens`
                        SET `access_token`=%s, `expires_at`=%s,  `payload`=%s
                        WHERE `credential`=%s AND `username`=%s AND `gamespace_id`=%s;
                    """, access_token, expires_at, data_text, credential, username, gamespace_id)

            except DatabaseError as e:
                raise SocialTokensError("Failed to save token: " + e.args[1])

            raise Return(account)


class NoSuchToken(Exception):
    pass
