
from anthill.common import Enum
from anthill.common.validate import validate
from anthill.common.database import DatabaseError, DuplicateError
from anthill.common.internal import Internal, InternalError

from . import profile

import datetime
import uuid
import ujson


class RequestError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class NoSuchRequest(Exception):
    pass


class RequestAdapter(object):
    def __init__(self, data, current_account_id=None):
        self.type = RequestType(data.get("request_type", RequestType.ACCOUNT))
        self.account = str(data.get("account_id"))
        self.time = data.get("request_time")
        self.object = str(data.get("request_object"))
        self.profile = None
        self.key = data.get("request_key")
        self.payload = data.get("request_payload")

        # apparently, mysql returns LONGTEXT field type instead of JSON in case of union calls
        if isinstance(self.payload, str):
            self.payload = ujson.loads(self.payload)

        self.kind = RequestKind(RequestKind.OUTGOING
                                if current_account_id == self.account
                                else RequestKind.INCOMING)

        if self.kind == RequestKind.OUTGOING:
            self.remote_object = self.object
        elif self.kind == RequestKind.INCOMING:
            self.remote_object = self.account
        else:
            self.remote_object = None

    def dump(self):
        result = {
            "type": str(self.type),
            "kind": str(self.kind),
            "time": str(self.time),
            "sender": self.account,
            "object": self.object,
            "key": self.key,
            "payload": self.payload
        }

        if self.profile:
            result["profile"] = self.profile

        return result


class RequestAdapterMapper(object):
    def __init__(self, account_id):
        self.account_id = str(account_id)

    def __call__(self, data):
        return RequestAdapter(data, current_account_id=self.account_id)


class RequestType(Enum):
    ACCOUNT = 'account'
    GROUP = 'group'

    ALL = {
        ACCOUNT, GROUP
    }


class RequestKind(Enum):
    INCOMING = 'incoming'
    OUTGOING = 'outgoing'

    ALL = {
        INCOMING, OUTGOING
    }


class RequestsModel(profile.ProfilesModel):

    # a week
    REQUEST_EXPIRE_IN = 604800

    def __init__(self, db, cache):
        super(RequestsModel, self).__init__(db, cache)
        self.internal = Internal()

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["requests"]

    def get_setup_events(self):
        return ["requests_expiration"]

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            async with self.db.acquire() as db:
                if gamespace_only:
                    await db.execute(
                        """
                            DELETE FROM `requests`
                            WHERE `gamespace_id`=%s AND `account_id` IN %s;
                        """, gamespace, accounts)
                    await db.execute(
                        """
                            DELETE FROM `requests`
                            WHERE `gamespace_id`=%s AND `request_type`=%s AND `request_object` IN %s;
                        """, gamespace, RequestType.ACCOUNT, accounts)
                else:
                    await db.execute(
                        """
                            DELETE FROM `requests`
                            WHERE `account_id` IN %s;
                        """, accounts)
                    await db.execute(
                        """
                            DELETE FROM `requests`
                            WHERE `request_type`=%s AND `request_object` IN %s;
                        """, RequestType.ACCOUNT, accounts)
        except DatabaseError as e:
            raise RequestError(500, "Failed to delete requests: " + e.args[1])

    @validate(gamespace_id="int", account_id="int", request_type='str_name', request_object="int",
              request_payload="json")
    async def create_request(self, gamespace_id, account_id, request_type, request_object, request_payload=None):
        async with self.db.acquire() as db:
            existing_request = await db.get(
                """
                SELECT `request_key` FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_type`=%s AND `request_object`=%s
                LIMIT 1;
                """, gamespace_id, account_id, str(request_type), request_object)

            if existing_request:
                return existing_request["request_key"]

            request_time = datetime.datetime.now()
            expire = datetime.datetime.now() + datetime.timedelta(seconds=RequestsModel.REQUEST_EXPIRE_IN)
            key = str(uuid.uuid4())

            try:
                await db.execute(
                    """
                    INSERT INTO `requests`
                    (`account_id`, `gamespace_id`, `request_type`, `request_object`, `request_time`, `request_expire`, 
                        `request_key`, `request_payload`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    """, account_id, gamespace_id, str(request_type), request_object, request_time, expire, key,
                    ujson.dumps(request_payload))
            except DuplicateError:
                raise RequestError(409, "Request already exists")
            except DatabaseError as e:
                raise RequestError(500, "Failed to create new request: " + str(e.args[1]))

            return key

    @validate(gamespace_id="int", account_id="int")
    async def cleanup(self, gamespace_id, account_id):
        try:
            await self.db.execute("""
                DELETE FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s;
            """, gamespace_id, account_id)

        except DatabaseError as e:
            raise RequestError(500, "Failed to delete requests: " + str(e.args[1]))

    def __fetch_profile__(self, gamespace_id, account_ids, profile_fields):
        return self.internal.request(
            "profile", "mass_profiles",
            accounts=list(set(account_ids)),
            gamespace=gamespace_id,
            action="get_public",
            profile_fields=profile_fields)

    @validate(gamespace_id="int", account_id="int", profile_fields="json_list_of_strings")
    async def list_outgoing_account_requests(self, gamespace_id, account_id, profile_fields=None):
        try:
            data = await self.db.query("""
                SELECT `account_id`, `request_type`, `request_object`, `request_time`, `request_key`, `request_payload`
                FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s;
            """, gamespace_id, account_id)

        except DatabaseError as e:
            raise RequestError(500, "Failed to list requests: " + str(e.args[1]))

        requests = map(RequestAdapterMapper(account_id), data)

        if profile_fields is not None:
            account_ids = [r.object for r in requests]

            try:
                profiles = await self.__fetch_profile__(gamespace_id, account_ids, profile_fields)
            except InternalError as e:
                raise RequestError(e.code, str(e))

            for r in requests:
                r.profile = profiles.get(str(r.object), None)

        return requests

    @validate(gamespace_id="int", account_id="int", profile_fields="json_list_of_strings")
    async def list_incoming_account_requests(self, gamespace_id, account_id, profile_fields=None):
        try:
            data = await self.db.query("""
                SELECT `account_id`, `request_type`, `request_object`, `request_time`, `request_key`, `request_payload`
                FROM `requests`
                WHERE `gamespace_id`=%s AND `request_type`=%s AND `request_object`=%s;
            """, gamespace_id, RequestType.ACCOUNT, account_id)

        except DatabaseError as e:
            raise RequestError(500, "Failed to list requests: " + str(e.args[1]))

        requests = map(RequestAdapterMapper(account_id), data)

        if profile_fields is not None:
            account_ids = [r.account for r in requests]

            try:
                profiles = await self.__fetch_profile__(gamespace_id, account_ids, profile_fields)
            except InternalError as e:
                raise RequestError(e.code, str(e))

            for r in requests:
                r.profile = profiles.get(str(r.account), None)

        return requests

    @validate(gamespace_id="int", account_id="int", profile_fields="json_list_of_strings")
    async def list_total_account_requests(self, gamespace_id, account_id, profile_fields=None):
        try:
            data = await self.db.query("""
                SELECT `account_id`, `request_type`, `request_object`, `request_time`, `request_key`, `request_payload`
                FROM `requests`
                WHERE `gamespace_id`=%s AND `request_type`=%s AND `request_object`=%s
                
                UNION
                
                SELECT `account_id`, `request_type`, `request_object`, `request_time`, `request_key`, `request_payload`
                FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s;
            """, gamespace_id, RequestType.ACCOUNT, account_id, gamespace_id, account_id)

        except DatabaseError as e:
            raise RequestError(500, "Failed to list requests: " + str(e.args[1]))

        requests = map(RequestAdapterMapper(account_id), data)

        if profile_fields is not None:
            account_ids = [r.remote_object for r in requests if r.remote_object]

            try:
                profiles = await self.__fetch_profile__(gamespace_id, account_ids, profile_fields)
            except InternalError as e:
                raise RequestError(e.code, str(e))

            for r in requests:
                r.profile = profiles.get(str(r.remote_object), None)

        return requests

    @validate(gamespace_id="int", account_id="int", request_type=RequestType, request_object="int")
    async def delete(self, gamespace_id, account_id, request_type, request_object):

        try:
            deleted = await self.db.execute(
                """
                    DELETE FROM `requests`
                    WHERE `account_id`=%s AND `gamespace_id`=%s AND `request_type`=%s AND `request_object`=%s
                    LIMIT 1;
                """, account_id, gamespace_id, str(request_type), request_object)

        except DatabaseError as e:
            raise RequestError(500, "Failed to delete a request: " + str(e.args[1]))

        return bool(deleted)

    @validate(gamespace_id="int", account_id="int", key="str")
    async def acquire(self, gamespace_id, account_id, key):

        async with self.db.acquire(auto_commit=False) as db:
            try:
                request = await db.get(
                    """
                        SELECT * FROM `requests`
                        WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_key`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """, gamespace_id, account_id, key)

                if not request:
                    raise NoSuchRequest()

                request = RequestAdapter(request)

                await db.execute(
                    """
                    DELETE FROM `requests`
                    WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_type`=%s AND `request_object`=%s
                    LIMIT 1;
                    """, gamespace_id, request.account, str(request.type), request.object)

                return request

            except DatabaseError as e:
                raise RequestError(500, "Failed to acquire a request: " + str(e.args[1]))
            finally:
                await db.commit()
