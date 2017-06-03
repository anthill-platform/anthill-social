
from tornado.gen import coroutine, Return

import profile
import datetime
import uuid
import ujson

from common import Enum
from common.validate import validate
from common.database import DatabaseError, DuplicateError


class RequestError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + self.message


class NoSuchRequest(Exception):
    pass


class RequestAdapter(object):
    def __init__(self, data):
        self.type = RequestType(data.get("request_type", RequestType.ACCOUNT))
        self.account = data.get("account_id")
        self.object = data.get("request_object")
        self.key = data.get("request_key")
        self.payload = data.get("request_payload")


class RequestType(Enum):
    ACCOUNT = 'account'
    GROUP = 'group'

    ALL = {
        ACCOUNT, GROUP
    }


class RequestsModel(profile.ProfilesModel):

    # a week
    REQUEST_EXPIRE_IN = 604800

    def __init__(self, db, cache):
        super(RequestsModel, self).__init__(db, cache)

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["requests"]

    def get_setup_events(self):
        return ["requests_expiration"]

    @coroutine
    @validate(gamespace_id="int", account_id="int", request_type='str_name', request_object="int",
              request_payload="json")
    def create_request(self, gamespace_id, account_id, request_type, request_object, request_payload=None):

        with (yield self.db.acquire()) as db:
            existing_request = yield db.get(
                """
                SELECT `request_key` FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_type`=%s AND `request_object`=%s
                LIMIT 1;
                """, gamespace_id, account_id, str(request_type), request_object)

            if existing_request:
                raise Return(existing_request["request_key"])

            expire = datetime.datetime.now() + datetime.timedelta(seconds=RequestsModel.REQUEST_EXPIRE_IN)
            key = str(uuid.uuid4())

            try:
                yield db.execute(
                    """
                    INSERT INTO `requests`
                    (`account_id`, `gamespace_id`, `request_type`, `request_object`, `request_expire`, 
                        `request_key`, `request_payload`)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, account_id, gamespace_id, str(request_type), request_object, expire, key,
                    ujson.dumps(request_payload))
            except DuplicateError:
                raise RequestError(409, "Request already exists")
            except DatabaseError as e:
                raise RequestError(500, "Failed to create new request: " + str(e.args[1]))

            raise Return(key)

    @coroutine
    @validate(gamespace_id="int", account_id="int")
    def cleanup(self, gamespace_id, account_id):
        try:
            yield self.db.execute("""
                DELETE FROM `requests`
                WHERE `gamespace_id`=%s AND `account_id`=%s;
            """, gamespace_id, account_id)

        except DatabaseError as e:
            raise RequestError(500, "Failed to delete requests: " + str(e.args[1]))

    @coroutine
    @validate(gamespace_id="int", account_id="int", request_type=RequestType, request_object="int")
    def delete(self, gamespace_id, account_id, request_type, request_object):

        try:
            deleted = yield self.db.execute(
                """
                    DELETE FROM `requests`
                    WHERE `account_id`=%s AND `gamespace_id`=%s AND `request_type`=%s AND `request_object`=%s
                    LIMIT 1;
                """, account_id, gamespace_id, str(request_type), request_object)

        except DatabaseError as e:
            raise RequestError(500, "Failed to delete a request: " + str(e.args[1]))

        raise Return(bool(deleted))

    @coroutine
    @validate(gamespace_id="int", account_id="int", key="str")
    def acquire(self, gamespace_id, account_id, key):

        with (yield self.db.acquire(auto_commit=False)) as db:
            try:
                request = yield db.get(
                    """
                        SELECT * FROM `requests`
                        WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_key`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """, gamespace_id, account_id, key)

                if not request:
                    raise NoSuchRequest()

                request = RequestAdapter(request)

                yield db.execute(
                    """
                    DELETE FROM `requests`
                    WHERE `gamespace_id`=%s AND `account_id`=%s AND `request_type`=%s AND `request_object`=%s
                    LIMIT 1;
                    """, gamespace_id, request.account, str(request.type), request.object)

                raise Return(request)

            except DatabaseError as e:
                raise RequestError(500, "Failed to acquire a request: " + str(e.args[1]))
            finally:
                yield db.commit()
