
from tornado.gen import coroutine, Return
import profile
from common.database import DatabaseError


class RequestError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class RequestsModel(profile.ProfilesModel):
    def __init__(self, db, cache):
        super(RequestsModel, self).__init__(db, cache)

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["account_requests"]

    @coroutine
    def accept(self, account_id, target_accounts, connections):
        yield self.delete(
            account_id,
            target_accounts)

        yield connections.create(
            account_id,
            target_accounts)

    @coroutine
    def create(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise RequestError("requested_accounts is not a list")

        requests = yield self.list(
            account_id,
            requested_accounts)

        for request_id, r in requests.iteritems():
            requested_accounts.remove(request_id)

        for request_id in requested_accounts:
            try:
                yield self.db.insert("""
                    INSERT INTO `account_requests`
                    (`account_id`, `requested_account`)
                    VALUES (%s, %s);
                """, account_id, request_id)

            except DatabaseError as e:
                raise RequestError(
                    "Failed to add a request: " + e.args[1])

    @coroutine
    def cleanup(self, account_id):
        try:
            yield self.db.execute("""
                DELETE FROM `account_requests`
                WHERE `account_id`=%s;
            """, account_id)

        except DatabaseError as e:
            raise RequestError(
                "Failed to delete a request: " + e.args[1])

    @coroutine
    def delete(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise RequestError("requested_accounts is not a list")

        if not requested_accounts:
            raise RequestError("Cannot delete empty set")

        try:
            yield self.db.insert(
                """
                    DELETE FROM `account_requests`
                    WHERE `account_id`=%s AND `requested_account` IN (%s);
                """, account_id, requested_accounts)

        except DatabaseError as e:
            raise RequestError("Failed to delete a request: " + e.args[1])

    @coroutine
    def list(self, account_id, requested_accounts):

        if not isinstance(requested_accounts, list):
            raise RequestError("requested_accounts is not a list")

        if not requested_accounts:
            raise RequestError("Cannot delete empty set")

        try:
            requests = yield self.db.query(
                """
                    SELECT *
                    FROM `account_requests`
                    WHERE `account_id`=%s AND `requested_account` IN (%s);
                """, account_id, requested_accounts)
        except DatabaseError as e:
            raise RequestError("Failed to get a request: " + e.args[1])
        else:
            result = {
                str(r["requested_account"]): r
                for r in requests
            }

            raise Return(result)

    @coroutine
    def list_inbox(self, account_id):
        try:
            requests = yield self.db.query(
                """
                    SELECT `account_id`
                    FROM `account_requests`
                    WHERE `requested_account`=%s
                """, account_id)
        except DatabaseError as e:
            raise RequestError("Failed to list inbox: " + e.args[1])

        accounts = [str(request["account_id"]) for request in requests]

        raise Return(accounts)

    @coroutine
    def list_inbox_profiles(self, account_id, profile_fields, gamespace):
        accounts = yield self.list_inbox(account_id)

        try:
            inbox_profiles = yield self.get_profiles(
                account_id,
                accounts,
                profile_fields,
                gamespace)

        except profile.ProfileRequestError as e:
            raise RequestError(e.message)

        raise Return(inbox_profiles)

    @coroutine
    def list_outbox(self, account_id):
        try:
            requests = yield self.db.query(
                """
                    SELECT `requested_account`
                    FROM `account_requests`
                    WHERE `account_id`=%s
                """, account_id)
        except DatabaseError as e:
            raise RequestError("Failed to list outbox: " + e.args[1])

        accounts = [
            str(request["requested_account"])
            for request in requests
        ]

        raise Return(accounts)

    @coroutine
    def list_outbox_profiles(self, account_id, profile_fields, gamespace):
        accounts = yield self.list_outbox(account_id)

        try:
            requests_profiles = yield self.get_profiles(
                account_id,
                accounts,
                profile_fields,
                gamespace)

        except profile.ProfileRequestError as e:
            raise RequestError(e.message)

        raise Return(requests_profiles)

    @coroutine
    def reject_requests(self, account_id, target_accounts, connections):
        yield self.delete(
            account_id,
            target_accounts)
