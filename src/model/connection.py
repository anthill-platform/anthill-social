
from tornado.gen import coroutine, Return

import profile
from common.database import DatabaseError


class ConnectionError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class ConnectionsModel(profile.ProfilesModel):
    def __init__(self, db, cache):
        super(ConnectionsModel, self).__init__(db, cache)

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["account_connections"]

    @coroutine
    def create(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise ConnectionError("requested_accounts is not a list")

        connections = yield self.get(
            account_id,
            requested_accounts)

        for account in connections:
            requested_accounts.remove(account)

        for requested_account in requested_accounts:
            try:
                yield self.db.insert(
                    """
                        INSERT INTO `account_connections`
                        (`account_id`, `account_connection`)
                        VALUES (%s, %s);
                    """, account_id, requested_account)

                yield self.db.insert(
                    """
                        INSERT INTO `account_connections`
                        (`account_connection`, `account_id`)
                        VALUES (%s, %s);
                    """, account_id, requested_account)
            except DatabaseError as e:
                raise ConnectionError("Failed to add a connection: " + e.message)

    @coroutine
    def cleanup(self, account_id):
        try:
            yield self.db.execute(
                """
                    DELETE FROM `account_connections`
                    WHERE `account_id`=%s;
                """, account_id)
        except DatabaseError as e:
            raise ConnectionError("Failed to delete a connection: " + e.args[1])

    @coroutine
    def delete(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise ConnectionError("requested_accounts is not a list")

        for requested_account in requested_accounts:
            try:
                yield self.db.execute(
                    """
                        DELETE FROM `account_connections`
                        WHERE (`account_id`=%s AND `account_connection`=%s) OR
                              (`account_connection`=%s AND `account_id`=%s);
                    """, account_id, requested_account, account_id, requested_account)
            except DatabaseError as e:
                raise ConnectionError("Failed to delete a connection: " + e.args[1])

    @coroutine
    def get(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise ConnectionError("requested_accounts is not a list")

        try:
            query = """
                SELECT * FROM `account_connections`
                WHERE `account_id`=%s AND `account_connection` IN ({0});
            """.format(", ".join('%s' for acc in requested_accounts))

            connections = yield self.db.query(
                query,
                account_id,
                *requested_accounts)

        except DatabaseError as e:
            raise ConnectionError("Failed to get a connection: " + e.args[1])
        else:
            result = [str(c["account_connection"]) for c in connections]
            raise Return(result)

    @coroutine
    def get_connections_profiles(self, gamespace_id, account_id, profile_fields):
        connections = yield self.list(account_id)

        try:
            connection_profiles = yield self.get_profiles(
                account_id,
                connections,
                profile_fields,
                gamespace_id)

        except profile.ProfileRequestError as e:
            raise ConnectionError(e.message)

        raise Return(connection_profiles)

    @coroutine
    def list(self, account_id):
        try:
            connections = yield self.db.query(
                """
                    SELECT * FROM `account_connections` WHERE `account_id`=%s;
                """, account_id)
        except DatabaseError as e:
            raise ConnectionError("Failed to get connections: " + e.args[1])

        connections = [str(c["account_connection"]) for c in connections]

        raise Return(connections)
