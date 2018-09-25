from . import profile
from .request import RequestType, RequestError, NoSuchRequest

from anthill.common.internal import InternalError
from anthill.common.validate import validate
from anthill.common.database import DatabaseError


class ConnectionError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class ConnectionsModel(profile.ProfilesModel):
    APPROVAL_SCOPE = 'connection_approval'

    MESSAGE_CONNECTION_REQUEST = 'connection_request'
    MESSAGE_CONNECTION_CREATED = 'connection_created'
    MESSAGE_CONNECTION_DELETED = 'connection_deleted'
    MESSAGE_CONNECTION_APPROVED = 'connection_approved'
    MESSAGE_CONNECTION_REJECTED = 'connection_rejected'

    def __init__(self, db, cache, requests):
        super(ConnectionsModel, self).__init__(db, cache)

        self.requests = requests

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["account_connections"]

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        if gamespace_only:
            return

        try:
            await self.db.execute(
                """
                    DELETE FROM `account_connections`
                    WHERE `account_id` IN %s OR `account_connection` IN %s;
                """, accounts, accounts)
        except DatabaseError as e:
            raise ConnectionError(500, "Failed to delete user connections: " + e.args[1])

    @validate(account_id="int", target_account="int")
    async def create(self, account_id, target_account):

        try:
            await self.db.insert(
                """
                    INSERT INTO `account_connections`
                    (`account_id`, `account_connection`)
                    VALUES (%s, %s), (%s, %s);
                """, account_id, target_account, target_account, account_id)
        except DatabaseError as e:
            raise ConnectionError(500, "Failed to add a connection: " + e.args[1])

    @validate(gamespace_id="int", account_id="int", approve_account_id="int", key="str", notify="json_dict")
    async def approve_connection(self, gamespace_id, account_id, approve_account_id, key, notify=None):

        try:
            request = await self.requests.acquire(
                gamespace_id, approve_account_id, key)
        except NoSuchRequest:
            raise ConnectionError(404, "No such request")
        except RequestError as e:
            raise ConnectionError(500, e.message)

        if request.type != RequestType.ACCOUNT:
            raise ConnectionError(400, "Bad request type")

        try:
            await self.create(
                account_id, request.account)
        except ConnectionError as e:
            raise ConnectionError(500, e.message)

        if notify is not None:
            await self.__send_message__(
                gamespace_id, "user", str(request.account), account_id,
                ConnectionsModel.MESSAGE_CONNECTION_APPROVED, notify, ["remove_delivered"],
                authoritative=True)

    @validate(gamespace_id="int", account_id="int", reject_account_id="int", key="str", notify="json_dict")
    async def reject_connection(self, gamespace_id, account_id, reject_account_id, key, notify=None):

        try:
            request = await self.requests.acquire(
                gamespace_id, reject_account_id, key)
        except NoSuchRequest:
            raise ConnectionError(404, "No such request")
        except RequestError as e:
            raise ConnectionError(500, e.message)

        if request.type != RequestType.ACCOUNT:
            raise ConnectionError(400, "Bad request type")

        if notify is not None:
            await self.__send_message__(
                gamespace_id, "user", str(request.account), account_id,
                ConnectionsModel.MESSAGE_CONNECTION_REJECTED, notify, ["remove_delivered"],
                authoritative=True)

    @validate(gamespace_id="int", account_id="int", target_account="int", approval="bool", notify="json_dict",
              payload="json_dict")
    async def request_connection(self, gamespace_id, account_id, target_account, approval=True, notify=None,
                                 payload=None):

        if approval:
            try:
                key = await self.requests.create_request(
                    gamespace_id, account_id, RequestType.ACCOUNT, target_account, payload)
            except RequestError as e:
                raise ConnectionError(500, e.message)

            if notify is not None:
                notify.update({
                    "key": key
                })

                await self.__send_message__(
                    gamespace_id, "user", str(target_account), account_id,
                    ConnectionsModel.MESSAGE_CONNECTION_REQUEST, notify, ["remove_delivered"],
                    authoritative=True)

            return {
                "key": key
            }
        else:
            try:
                await self.create(account_id, target_account)
            except ConnectionError as e:
                raise ConnectionError(500, e.message)

            if notify is not None:
                await self.__send_message__(
                    gamespace_id, "user", str(target_account), account_id,
                    ConnectionsModel.MESSAGE_CONNECTION_CREATED, notify, ["remove_delivered"],
                    authoritative=True)

    async def __send_message__(self, gamespace_id, recipient_class, recipient_key,
                               account_id, message_type, payload, flags=None, authoritative=False):
        try:
            await self.internal.rpc(
                "message", "send_message",
                gamespace=gamespace_id, sender=account_id,
                recipient_class=recipient_class, recipient_key=recipient_key,
                message_type=message_type, payload=payload, flags=flags or [],
                authoritative=authoritative)
        except InternalError:
            pass  # well

    async def cleanup(self, account_id):
        try:
            await self.db.execute(
                """
                    DELETE FROM `account_connections`
                    WHERE `account_id`=%s OR `account_connection`=%s
                    LIMIT 2;
                """, account_id, account_id)
        except DatabaseError as e:
            raise ConnectionError(500, "Failed to delete a connection: " + e.args[1])

    @validate(gamespace_id="int", account_id="int", target_account="int", notify="json_dict")
    async def delete(self, gamespace_id, account_id, target_account, notify=None):

        try:
            await self.db.execute(
                """
                    DELETE FROM `account_connections`
                    WHERE (`account_id`=%s AND `account_connection`=%s) OR
                          (`account_connection`=%s AND `account_id`=%s)
                    LIMIT 2;
                """, account_id, target_account, account_id, target_account)
        except DatabaseError as e:
            raise ConnectionError(500, "Failed to delete a connection: " + e.args[1])

        if notify is not None:
            await self.__send_message__(
                gamespace_id, "user", str(target_account), account_id,
                ConnectionsModel.MESSAGE_CONNECTION_DELETED, notify, ["remove_delivered"],
                authoritative=True)

    async def get_connections_profiles(self, gamespace_id, account_id, profile_fields):
        connections = await self.list_connections(account_id)

        try:
            connection_profiles = await self.get_profiles(
                account_id,
                connections,
                profile_fields,
                gamespace_id)

        except profile.ProfileRequestError as e:
            raise ConnectionError(500, e.message)

        return connection_profiles

    async def list_connections(self, account_id):
        try:
            connections = await self.db.query(
                """
                    SELECT * 
                    FROM `account_connections` 
                    WHERE `account_id`=%s;
                """, account_id)
        except DatabaseError as e:
            raise ConnectionError(500, "Failed to get connections: " + e.args[1])

        return [str(c["account_connection"]) for c in connections]
