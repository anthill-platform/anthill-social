
from tornado.gen import coroutine, Return

from common.validate import validate
from common.model import Model
from common.database import DatabaseError, DuplicateError
from common.internal import Internal, InternalError
from common import cached

import re
import hashlib


class NamesModelError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + str(self.message)


class NameIsBusyError(Exception):
    pass


class NameAdapter(object):
    def __init__(self, data):
        self.account_id = data.get("account_id")
        self.name = data.get("name")
        self.profile = None


class NamesModel(Model):
    def __init__(self, db, cache):
        self.db = db
        self.cache = cache
        self.internal = Internal()

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["unique_names"]

    def has_delete_account_event(self):
        return True

    @coroutine
    def accounts_deleted(self, gamespace, accounts, gamespace_only):
        try:
            if gamespace_only:
                yield self.db.execute(
                    """
                        DELETE FROM `unique_names`
                        WHERE `gamespace_id`=%s AND `account_id` IN %s;
                    """, gamespace, accounts)
            else:
                yield self.db.execute(
                    """
                        DELETE FROM `unique_names`
                        WHERE `account_id` IN %s;
                    """, accounts)
        except DatabaseError as e:
            raise NamesModelError(500, "Failed to delete unique names: " + e.args[1])

    @coroutine
    @validate(gamespace_id="int", kind="str_name", query="str", profile_fields="json_list_of_strings")
    def search_names(self, gamespace_id, kind, query, profile_fields=None, db=None):

        words = re.findall(r'[^\s]+', query)

        if not words:
            raise Return([])

        if len(words) > 32:
            # too many words
            words = words[:32]

        compiled = u" ".join(u"+" + word + u"*" for word in words if len(word) > 2)

        try:
            names = yield (db or self.db).query(
                u"""
                    SELECT `account_id`, `name`
                    FROM `unique_names`
                    WHERE `gamespace_id`=%s AND `kind`=%s AND MATCH(`name`) AGAINST (%s IN BOOLEAN MODE)
                    LIMIT 100;
                """, gamespace_id, kind, compiled)
        except DatabaseError as e:
            raise NamesModelError(500, e.args[1])

        names = map(NameAdapter, names)

        if profile_fields is not None:
            account_ids = [str(name.account_id) for name in names]

            def _hash():
                h = hashlib.sha256()
                for s in profile_fields:
                    h.update(s + ",")
                for s in account_ids:
                    h.update(s + ",")
                return h.hexdigest()

            @cached(kv=self.cache,
                    h=lambda: "names:" + str(gamespace_id) + ":" + str(kind) + ":" + _hash(),
                    ttl=20,
                    json=True)
            def do_request():
                return self.internal.request(
                    "profile", "mass_profiles",
                    accounts=list(set(account_ids)),
                    gamespace=gamespace_id,
                    action="get_public",
                    profile_fields=profile_fields)

            try:
                profiles = yield do_request()
            except InternalError as e:
                raise NamesModelError(e.code, e.message)

            for name in names:
                name.profile = profiles.get(str(name.account_id))

        raise Return(names)

    @coroutine
    @validate(gamespace_id="int", kind="str_name", name="str")
    def check_name(self, gamespace_id, kind, name):
        try:
            busy = yield self.db.get(
                """
                SELECT `account_id` FROM `unique_names`
                WHERE `gamespace_id`=%s AND `kind`=%s AND `name`=%s
                LIMIT 1;
                """, gamespace_id, kind, name)
        except DatabaseError as e:
            raise NamesModelError(500, e.args[1])

        if not busy:
            raise Return(None)

        raise Return(busy["account_id"])

    @coroutine
    @validate(gamespace_id="int", account_id="int", kind="str_name", name="str")
    def acquire_name(self, gamespace_id, account_id, kind, name):
        try:
            updated = yield self.db.execute(
                """
                INSERT INTO `unique_names`
                (`gamespace_id`, `account_id`, `kind`, `name`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY 
                UPDATE `name`=VALUES(`name`);
                """, gamespace_id, account_id, kind, name)
        except DatabaseError as e:
            raise NamesModelError(500, e.args[1])

        if not updated:
            raise NameIsBusyError()

    @coroutine
    @validate(gamespace_id="int", account_id="int", kind="str_name")
    def release_name(self, gamespace_id, account_id, kind):
        try:
            released = yield self.db.execute(
                """
                DELETE FROM `unique_names`
                WHERE `gamespace_id`=%s AND `account_id`=%s AND `kind`=%s
                LIMIT 1;
                """, gamespace_id, account_id, kind)
        except DatabaseError as e:
            raise NamesModelError(500, e.args[1])
        else:
            raise Return(bool(released))
