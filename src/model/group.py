
from tornado.gen import coroutine, Return

from common import Flags, Enum

from common.internal import Internal, InternalError
from common.model import Model
from common.validate import validate
from common.database import DatabaseError, DuplicateError
from common.profile import DatabaseProfile, NoDataError, ProfileError

from request import RequestType, NoSuchRequest, RequestError

import ujson
import logging


class NoSuchGroup(Exception):
    pass


class NoSuchParticipation(Exception):
    pass


class GroupError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.code) + ": " + str(self.message)


class GroupParticipationProfile(DatabaseProfile):
    @staticmethod
    def __encode_profile__(profile):
        return ujson.dumps(profile)

    def __init__(self, db, gamespace_id, group_id, account_id):
        super(GroupParticipationProfile, self).__init__(db)
        self.gamespace_id = gamespace_id
        self.group_id = group_id
        self.account_id = account_id

    @staticmethod
    def __parse_profile__(profile):
        return profile

    @coroutine
    def get(self):
        group = yield self.conn.get(
            """
                SELECT `participation_profile`
                FROM `group_participants`
                WHERE `account_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                LIMIT 1
                FOR UPDATE;
            """, self.account_id, self.group_id, self.gamespace_id)

        if group:
            raise Return(GroupProfile.__parse_profile__(group["participation_profile"]))

        raise NoDataError()

    @coroutine
    def insert(self, data):
        raise ProfileError("Insertion is not supported")

    @coroutine
    def update(self, data):
        encoded = GroupProfile.__encode_profile__(data)
        yield self.conn.execute(
            """
                UPDATE `group_participants`
                SET `participation_profile`=%s
                WHERE `account_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """, encoded, self.account_id, self.group_id, self.gamespace_id)


class GroupProfile(DatabaseProfile):
    @staticmethod
    def __encode_profile__(profile):
        return ujson.dumps(profile)

    def __init__(self, db, gamespace_id, group_id):
        super(GroupProfile, self).__init__(db)
        self.gamespace_id = gamespace_id
        self.group_id = group_id

    @staticmethod
    def __parse_profile__(profile):
        return profile

    @coroutine
    def get(self):
        group = yield self.conn.get(
            """
                SELECT `group_profile`
                FROM `groups`
                WHERE `group_id`=%s AND `gamespace_id`=%s
                LIMIT 1
                FOR UPDATE;
            """, self.group_id, self.gamespace_id)

        if group:
            raise Return(GroupProfile.__parse_profile__(group["group_profile"]))

        raise NoDataError()

    @coroutine
    def insert(self, data):
        raise ProfileError("Insertion is not supported")

    @coroutine
    def update(self, data):
        encoded = GroupProfile.__encode_profile__(data)
        yield self.conn.execute(
            """
                UPDATE `groups`
                SET `group_profile`=%s
                WHERE `group_id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """, encoded, self.group_id, self.gamespace_id)


class GroupAdapter(object):
    def __init__(self, data):
        self.profile = data.get("group_profile") or {}
        self.flags = GroupFlags(data.get("group_flags", "").split(","))
        self.join_method = GroupJoinMethod(data.get("group_join_method", GroupJoinMethod.FREE))
        self.free_members = data.get("group_free_members", GroupsModel.DEFAULT_MAX_MEMBERS)
        self.owner = data.get("group_owner", 0)

    def is_owner(self, owner):
        return str(self.owner) == str(owner)


class GroupParticipationAdapter(object):
    def __init__(self, data):
        self.account = int(data.get("account_id", 0))
        self.role = data.get("participation_role", 0)
        self.permissions = set(data.get("participation_permissions", "").split(","))
        self.profile = data.get("participation_profile", {})

    def has_permission(self, permission):
        return permission in self.permissions


class GroupFlags(Flags):
    MESSAGE_SUPPORT = 'messages'


class GroupJoinMethod(Enum):
    FREE = 'free'
    INVITE = 'invite'
    APPROVE = 'approve'

    ALL = {
        FREE, INVITE, APPROVE
    }


class GroupsModel(Model):

    MAXIMUM_ROLE = 1000
    MINIMUM_ROLE = 0

    GROUP_CLASS = "social-group"
    DEFAULT_MAX_MEMBERS = 50
    MAX_MEMBERS_LIMIT = 1000
    MIN_MEMBERS_LIMIT = 2

    PERMISSION_REQUEST_APPROVAL = "request_approval"
    PERMISSION_SEND_INVITE = "send_invite"
    PERMISSION_KICK = "kick"

    MESSAGE_PERMISSIONS_UPDATED = "permissions_updated"
    MESSAGE_OWNERSHIP_TRANSFERRED = "ownership_transferred"
    MESSAGE_GROUP_PROFILE_UPDATED = "group_profile_updated"
    MESSAGE_PARTICIPATION_PROFILE_UPDATED = "participation_profile_updated"
    MESSAGE_GROUP_INVITE = "group_invite"
    MESSAGE_GROUP_REQUEST = "group_request"
    MESSAGE_GROUP_REQUEST_APPROVED = "group_request_approved"

    def __init__(self, db, requests):
        self.db = db
        self.internal = Internal()
        self.requests = requests

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["groups", "group_participants"]

    @coroutine
    @validate(gamespace_id="int", group_profile="json_dict", group_flags=GroupFlags,
              group_join_method=GroupJoinMethod, max_members="int", account_id="int",
              participation_profile="json_dict")
    def create_group(self, gamespace_id, group_profile, group_flags, group_join_method, max_members,
                     owner_account_id, participation_profile):

        if max_members < 2:
            raise GroupError(400, "Max members cannot be lass than {0}".format(GroupsModel.MIN_MEMBERS_LIMIT))

        if max_members > GroupsModel.MAX_MEMBERS_LIMIT:
            raise GroupError(400, "Max members cannot be more than {0}".format(GroupsModel.MAX_MEMBERS_LIMIT))

        # remove one since we are joining into it
        max_members -= 1
        group_id = None

        with (yield self.db.acquire()) as db:

            # create the group first

            try:
                group_id = yield db.insert(
                    """
                        INSERT INTO `groups`
                        (`gamespace_id`, `group_profile`, `group_flags`, `group_join_method`, 
                            `group_free_members`, `group_owner`)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, gamespace_id, ujson.dumps(group_profile), str(group_flags),
                    str(group_join_method), max_members, owner_account_id)
            except DatabaseError as e:
                raise GroupError(500, "Failed to create a group: " + str(e.args[1]))

            # then join to the group automatically as there

            try:
                yield db.execute(
                    """
                        INSERT INTO `group_participants`
                        (`gamespace_id`, `group_id`, `account_id`, `participation_role`, `participation_profile`,
                            `participation_permissions`)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, gamespace_id, group_id, owner_account_id, GroupsModel.MAXIMUM_ROLE,
                    ujson.dumps(participation_profile), "")
            except DatabaseError as e:
                try:
                    yield self.delete_group(gamespace_id, group_id, db=db)
                except GroupError:
                    pass  # we should try at least

                raise GroupError(500, "Failed to automatically join to a group: " + str(e.args[1]))

            if GroupFlags.MESSAGE_SUPPORT in group_flags:
                try:
                    yield self.internal.request(
                        "message", "create_group",
                        gamespace=gamespace_id, group_class=GroupsModel.GROUP_CLASS, group_key=str(group_id),
                        join_account_id=owner_account_id, join_role="member")
                except InternalError as e:
                    try:
                        yield self.delete_group(gamespace_id, group_id, db=db)
                    except GroupError:
                        pass

                    raise GroupError(500, "Failed to create in-message group: " + str(e.message))

        raise Return(group_id)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int", group_profile="json_dict",
              merge="bool", notify="json_dict_or_none")
    def update_group(self, gamespace_id, group_id, account_id, group_profile, merge=True, notify=None):

        has_participation = yield self.get_group_participation(gamespace_id, group_id, account_id)
        if not has_participation:
            raise GroupError(404, "Player has not participated this group")

        profile = GroupProfile(self.db, gamespace_id, group_id)

        try:
            result = yield profile.set_data(group_profile, None, merge=merge)
        except NoDataError:
            raise GroupError(404, "No such group")
        except ProfileError as e:
            raise GroupError(500, "Failed to update group profile: " + e.message)

        if notify:
            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS, str(group_id), account_id,
                GroupsModel.MESSAGE_GROUP_PROFILE_UPDATED, notify)

        raise Return(result)

    @coroutine
    @validate(gamespace_id="int", group_id="int", updater_account_id="int",
              participation_account_id="int", participation_profile="json_dict",
              merge="bool", notify="json_dict_or_none")
    def update_group_participation(self, gamespace_id, group_id, updater_account_id, participation_account_id,
                                   participation_profile, merge=True, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if not group.is_owner(updater_account_id):
            if str(participation_account_id) != str(updater_account_id):
                higher = yield self.check_group_participation_role_higher(
                    gamespace_id, group_id, updater_account_id, participation_account_id)

                if not higher:
                    raise GroupError(406, "Your role should be higher to edit other player's participation profiles")

        profile = GroupParticipationProfile(self.db, gamespace_id, group_id, participation_account_id)

        try:
            result = yield profile.set_data(participation_profile, None, merge=merge)
        except NoDataError:
            raise NoSuchParticipation()
        except ProfileError as e:
            raise GroupError(500, "Failed to update participation profile: " + e.message)

        if notify:
            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS, str(group_id), updater_account_id,
                GroupsModel.MESSAGE_PARTICIPATION_PROFILE_UPDATED, notify)

        raise Return(result)

    @coroutine
    @validate(gamespace_id="int", group_id="int", updater_account_id="int", participation_account_id="int",
              participation_role="int", participation_permissions="json_list_of_str_name",
              notify="json_dict_or_none")
    def update_group_participation_permissions(
            self, gamespace_id, group_id, updater_account_id, participation_account_id,
            participation_role, participation_permissions, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if group.is_owner(updater_account_id):
            yield self.__internal_update_group_participation_permissions__(
                gamespace_id, group_id, updater_account_id, participation_account_id,
                participation_role, participation_permissions, notify=notify)
        else:

            if str(updater_account_id) == str(participation_account_id):

                # that makes sure you can only downgrade your own role but not upgrade
                def check_increase(old):
                    return old >= participation_role

                yield self.__internal_update_group_participation_permissions__(
                    gamespace_id, group_id, updater_account_id, participation_account_id, participation_role,
                    participation_permissions, role_callback=check_increase, notify=notify)
            else:

                my_participation = yield self.get_group_participation(
                    gamespace_id, group_id, updater_account_id)

                my_role = my_participation.role

                participation_permissions = list(set(participation_permissions) & my_participation.permissions)

                if participation_role >= my_role:
                    raise GroupError(406, "You cannot set a role >= than yours")

                # that makes sure you cannot edit roles of another player with role higher than yours
                def check_roles(old):
                    return my_role > old

                yield self.__internal_update_group_participation_permissions__(
                    gamespace_id, group_id, updater_account_id, participation_account_id,
                    participation_role, participation_permissions, role_callback=check_roles, notify=notify)

    @coroutine
    def __internal_update_group_participation_permissions__(
            self, gamespace_id, group_id, updater_account_id, account_id, participation_role,
            participation_permissions, role_callback=None, notify=None):

        with (yield self.db.acquire(auto_commit=False)) as db:
            try:
                role = yield db.get(
                    """
                        SELECT `participation_role`, `participation_permissions`
                        FROM `group_participants`
                        WHERE `account_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """, account_id, group_id, gamespace_id)

                if not role:
                    raise NoSuchParticipation()

                if role_callback:
                    old_role = role["participation_role"]

                    if not role_callback(old_role):
                        raise GroupError(409, "Cannot update role")

                yield db.execute(
                    """
                        UPDATE `group_participants`
                        SET `participation_role`=%s, `participation_permissions`=%s
                        WHERE `account_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                        LIMIT 1;
                    """, participation_role, ",".join(participation_permissions), account_id, group_id, gamespace_id
                )
            except DatabaseError as e:
                raise GroupError(500, "Failed to update role: " + str(e.args[1]))
            finally:
                yield db.commit()

        if notify:
            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS, str(group_id), updater_account_id,
                GroupsModel.MESSAGE_PERMISSIONS_UPDATED, notify)

    @coroutine
    def __send_message__(self, gamespace_id, recipient_class, recipient_key,
                         account_id, message_type, payload, flags=None):
        try:
            yield self.internal.rpc(
                "message", "send_message",
                gamespace=gamespace_id, sender=account_id,
                recipient_class=recipient_class, recipient_key=recipient_key,
                message_type=message_type, payload=payload, flags=flags or [])
        except InternalError:
            pass  # well

    @coroutine
    @validate(gamespace_id="int", group_id="int")
    def get_group(self, gamespace_id, group_id, db=None):
        try:
            group = yield (db or self.db).get(
                """
                    SELECT *
                    FROM `groups`
                    WHERE `gamespace_id`=%s AND `group_id`=%s
                    LIMIT 1;
                """, gamespace_id, group_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group: " + str(e.args[1]))
        else:
            if not group:
                raise NoSuchGroup()

            raise Return(GroupAdapter(group))

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int")
    def get_group_with_participants(self, gamespace_id, group_id, account_id):
        with (yield self.db.acquire()) as db:
            group = yield self.get_group(gamespace_id, group_id, db=db)
            participants = yield self.list_group_participants(gamespace_id, group_id, db=db)
            my_participant = next((participant for participant in participants
                                   if participant.account == account_id), None)

            result = (group, participants, my_participant)
            raise Return(result)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int")
    def get_group_with_participation(self, gamespace_id, group_id, account_id):
        with (yield self.db.acquire()) as db:
            group = yield self.get_group(gamespace_id, group_id, db=db)
            participation = yield self.get_group_participation(gamespace_id, group_id, account_id, db=db)
            result = (group, participation)
            raise Return(result)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int")
    def get_group_participation(self, gamespace_id, group_id, account_id, db=None):
        try:
            participation = yield (db or self.db).get(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id`=%s
                    LIMIT 1;
                """, gamespace_id, group_id, account_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group participation: " + str(e.args[1]))
        else:
            if not participation:
                raise NoSuchParticipation()

            raise Return(GroupParticipationAdapter(participation))

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="json_list_of_ints")
    def get_group_participants(self, gamespace_id, group_id, account_ids, db=None):

        if not account_ids:
            raise GroupError(400, "Empty account_ids")

        try:
            participants = yield (db or self.db).query(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id` IN %s;
                """, gamespace_id, group_id, account_ids)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group participation: " + str(e.args[1]))
        else:
            raise Return({
                participant["account_id"]: GroupParticipationAdapter(participant)
                for participant in participants
            })

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int")
    def has_group_participation(self, gamespace_id, group_id, account_id, db=None):
        try:
            count = yield (db or self.db).get(
                """
                    SELECT COUNT(*) AS count
                    FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id`=%s
                    LIMIT 1;
                """, gamespace_id, group_id, account_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group participation: " + str(e.args[1]))
        else:
            if not count or not count["count"]:
                raise Return(False)

            raise Return(True)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int")
    def check_group_participation_role_higher(self, gamespace_id, group_id, account_a, account_b, db=None):
        try:
            result = yield (db or self.db).get(
                """
                    SELECT IF(
                        (SELECT `participation_role` FROM `group_participants`
                          WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id`=%s
                          LIMIT 1)
                        >
                        (SELECT `participation_role` FROM `group_participants`
                          WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id`=%s
                          LIMIT 1)
                    , 1, 0) AS result;
                """, gamespace_id, group_id, account_a,  gamespace_id, group_id, account_b)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group participation: " + str(e.args[1]))
        else:
            if not result or not result["result"]:
                raise Return(False)

            raise Return(True)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_ids="json_list_of_ints")
    def get_group_multiple_participants(self, gamespace_id, group_id, account_ids, db=None):

        if not account_ids:
            raise GroupError(400, "Empty account_ids")

        try:
            participants = yield (db or self.db).query(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id` IN %s
                    LIMIT 1;
                """, gamespace_id, group_id, account_ids)
        except DatabaseError as e:
            raise GroupError(500, "Failed to get a group participation: " + str(e.args[1]))
        else:
            if len(participants) < len(account_ids):
                raise NoSuchParticipation()

            raise Return({
                int(participation["account_id"]): GroupParticipationAdapter(participation)
                for participation in participants
            })

    @coroutine
    @validate(gamespace_id="int", group_id="int")
    def delete_group(self, gamespace_id, group_id, db=None):
        if not db:
            with (yield self.db.acquire()) as db:
                yield self.delete_group(gamespace_id, group_id, db=db)
            return

        try:
            yield db.execute(
                """
                    DELETE FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s;
                """, gamespace_id, group_id)
            yield db.execute(
                """
                    DELETE FROM `groups`
                    WHERE `gamespace_id`=%s AND `group_id`=%s
                    LIMIT 1;
                """, gamespace_id, group_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to delete a group: " + str(e.args[1]))

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int", participation_profile="json_dict",
              notify="json_dict_or_none")
    def join_group_request(self, gamespace_id, group_id, account_id, participation_profile, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if group.free_members == 0:
            raise GroupError(410, "Group is full")

        if group.join_method != GroupJoinMethod.APPROVE:
            raise GroupError(409, "This group join cannot be requested, it is: {0}".format(str(group.join_method)))

        has_participation = yield self.has_group_participation(
            gamespace_id, group_id, account_id)

        if has_participation:
            raise GroupError(406, "Player is already in this group")

        key = yield self.requests.create_request(
            gamespace_id, account_id, RequestType.GROUP, group_id, {
                "participation_profile": participation_profile
            })

        if notify and GroupFlags.MESSAGE_SUPPORT in group.flags:

            notify.update({
                "key": key
            })

            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS,
                str(group_id), account_id,
                GroupsModel.MESSAGE_GROUP_REQUEST, notify)

        raise Return(key)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int",
              invite_account_id="int", role="int", permissions="json_list_of_str_name",
              notify="json_dict_or_none")
    def invite_to_group(self, gamespace_id, group_id, account_id,
                        invite_account_id, role, permissions, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if group.free_members == 0:
            raise GroupError(410, "Group is full")

        if group.join_method != GroupJoinMethod.INVITE:
            raise GroupError(409, "This group is not for invites, it is: {0}".format(str(group.join_method)))

        participation = yield self.get_group_participation(gamespace_id, group_id, account_id)

        if not group.is_owner(account_id):
            if not participation.has_permission(GroupsModel.PERMISSION_SEND_INVITE):
                raise GroupError(406, "You have no permission to send invites")

            permissions = list(set(permissions) & participation.permissions)

            if role > participation.role:
                raise GroupError(409, "Invited role cannot be higher than your role")

        key = yield self.requests.create_request(
            gamespace_id, invite_account_id, RequestType.GROUP, group_id, {
                "role": role,
                "permissions": permissions
            })

        if notify and GroupFlags.MESSAGE_SUPPORT in group.flags:
            notify.update({
                "invite_group_id": str(group_id),
                "key": key
            })
            yield self.__send_message__(
                gamespace_id, "user", str(invite_account_id), account_id,
                GroupsModel.MESSAGE_GROUP_INVITE, notify, flags=["remove_delivered"])

        raise Return(key)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int", approve_account_id="int",
              role="int", key="str", permissions="json_list_of_str_name", notify="json_dict_or_none")
    def approve_join_group(self, gamespace_id, group_id, account_id, approve_account_id,
                           role, key, permissions, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if group.free_members == 0:
            raise GroupError(410, "Group is full")

        if group.join_method != GroupJoinMethod.APPROVE:
            raise GroupError(409, "This group is not approve-like, it is: {0}".format(str(group.join_method)))

        if not group.is_owner(account_id):
            participation = yield self.get_group_participation(gamespace_id, group_id, account_id)

            if not participation.has_permission(GroupsModel.PERMISSION_REQUEST_APPROVAL):
                raise GroupError(406, "You have no permission to approve items")

            # limit permissions only to those the player has
            permissions = list(set(permissions) & participation.permissions)

            if role > participation.role:
                raise GroupError(409, "Approved role cannot be higher than your role")

        request = yield self.requests.acquire(gamespace_id, approve_account_id, key)

        if request.type != RequestType.GROUP:
            raise GroupError(400, "Bad request object")

        if str(request.object) != str(group_id):
            raise GroupError(406, "This invite key is not for that object")

        message_support = GroupFlags.MESSAGE_SUPPORT in group.flags

        participation_profile = (request.payload or {}).get("participation_profile", {})

        yield self.__internal_join_group__(
            gamespace_id, group_id, approve_account_id, role,
            participation_profile, permissions, message_support=message_support, notify=notify)

        if notify and GroupFlags.MESSAGE_SUPPORT in group.flags:
            notify.update({
                "approved_by": str(account_id)
            })
            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS, str(group_id), approve_account_id,
                GroupsModel.MESSAGE_GROUP_REQUEST_APPROVED, notify)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int",
              participation_profile="json_dict", key="str_or_none", notify="json_dict_or_none")
    def join_group(self, gamespace_id, group_id, account_id, participation_profile,
                   key=None, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if group.free_members == 0:
            raise GroupError(410, "Group is full")

        if group.join_method == GroupJoinMethod.FREE:
            role = GroupsModel.MINIMUM_ROLE
            permissions = []
        elif group.join_method == GroupJoinMethod.INVITE:
            if not key:
                raise GroupError(406, "Group is invite-based and invite key is not passed")

            try:
                request = yield self.requests.acquire(gamespace_id, account_id, key)
            except NoSuchRequest:
                raise GroupError(410, "No such invite request")
            except RequestError as e:
                raise GroupError(500, e.message)

            if request.type != RequestType.GROUP:
                raise GroupError(400, "Bad request object")

            if str(request.object) != str(group_id):
                raise GroupError(406, "This invite key is not for that object")

            payload = request.payload or {}

            role = payload.get("role", GroupsModel.MINIMUM_ROLE)
            permissions = payload.get("permissions", [])

        else:
            raise GroupError(409, "Group join method is not free, it is: {0}".format(str(group.join_method)))

        message_support = GroupFlags.MESSAGE_SUPPORT in group.flags

        yield self.__internal_join_group__(
            gamespace_id, group_id, account_id, role,
            participation_profile, permissions, message_support=message_support, notify=notify)

    @coroutine
    def __internal_join_group__(
            self, gamespace_id, group_id, account_id, participation_role,
            participation_profile, permissions, message_support=True, notify=None):

        with (yield self.db.acquire(auto_commit=False)) as db:
            try:
                try:
                    group = yield db.get(
                        """
                            SELECT `group_free_members` FROM `groups`
                            WHERE `gamespace_id`=%s AND `group_id`=%s
                            LIMIT 1
                            FOR UPDATE;
                        """, gamespace_id, group_id)
                except DatabaseError as e:
                    raise GroupError(500, "Failed to join to a group: " + str(e.args[1]))

                group_free_members = group["group_free_members"]

                if group_free_members <= 0:
                    raise GroupError(410, "The group is full")

                if message_support:
                    try:
                        yield self.internal.request(
                            "message", "join_group",
                            gamespace=gamespace_id, group_class=GroupsModel.GROUP_CLASS,
                            group_key=str(group_id), account_id=account_id,
                            role="member", notify=notify)
                    except InternalError as e:
                        logging.exception("Failed to join to message group.")
                        raise GroupError(e.code, e.message)

                # first, add the joined record

                try:
                    yield db.execute(
                        """
                            INSERT INTO `group_participants`
                            (`gamespace_id`, `group_id`, `account_id`, `participation_role`, 
                                `participation_profile`, `participation_permissions`)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """, gamespace_id, group_id, account_id, participation_role,
                        ujson.dumps(participation_profile), ",".join(permissions))
                except DuplicateError:
                    raise GroupError(409, "Account '{0}' has already jointed the group.".format(account_id))
                except DatabaseError as e:
                    raise GroupError(500, "Failed to join to a group: " + str(e.args[1]))

                # second, update the group

                group_free_members -= 1

                try:
                    yield db.execute(
                        """
                            UPDATE `groups`
                            SET `group_free_members`=%s
                            WHERE `gamespace_id`=%s AND `group_id`=%s
                            LIMIT 1
                        """, group_free_members, gamespace_id, group_id)
                except DatabaseError as e:
                    raise GroupError(500, "Failed to join to a group: " + str(e.args[1]))

                group_free_members -= 1

            finally:
                yield db.commit()

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int", notify="json_dict_or_none")
    def leave_group(self, gamespace_id, group_id, account_id, db=None, group=None, notify=None):

        if not group:
            group = yield self.get_group(gamespace_id, group_id, db=db)

        if group.is_owner(account_id):
            raise GroupError(409, "Group owner cannot leave a group, transfer ownership first")

        if GroupFlags.MESSAGE_SUPPORT in group.flags:
            try:
                yield self.internal.request(
                    "message", "leave_group",
                    gamespace=gamespace_id, group_class=GroupsModel.GROUP_CLASS,
                    group_key=str(group_id), account_id=account_id, notify=notify)
            except InternalError as e:
                raise GroupError(e.code, e.message)

        try:
            yield (db or self.db).execute(
                """
                    DELETE FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s AND `account_id`=%s
                    LIMIT 1;;
                """, gamespace_id, group_id, account_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to leave a group: " + str(e.args[1]))

    @coroutine
    @validate(gamespace_id="int", group_id="int", kicker_account_id="int", account_id="int")
    def kick_from_group(self, gamespace_id, group_id, kicker_account_id, account_id):

        with (yield self.db.acquire()) as db:
            group = yield self.get_group(gamespace_id, group_id, db=db)

            if group.is_owner(account_id):
                raise GroupError(406, "You cannot kick an owner")

            if not group.is_owner(kicker_account_id):
                participants = yield self.get_group_participants(
                    gamespace_id, group_id, [account_id, kicker_account_id], db=db)

                kicker_permissions = participants[kicker_account_id]
                account_permissions = participants[account_id]

                if not kicker_permissions.has_permission(GroupsModel.PERMISSION_KICK):
                    raise GroupError(406, "You have no permission to kick")

                if account_permissions.role >= kicker_permissions.role:
                    raise GroupError(406, "You cannot kick a player with a higher role")

            yield self.leave_group(gamespace_id, group_id, account_id, db=db, group=group)

    @coroutine
    @validate(gamespace_id="int", group_id="int", account_id="int", account_transfer_to="int",
              notify="json_dict_or_none")
    def transfer_ownership(self, gamespace_id, group_id, account_id, account_transfer_to, notify=None):

        group = yield self.get_group(gamespace_id, group_id)

        if not group.is_owner(account_id):
            raise GroupError(409, "You are not an owner of that group")

        has_participation = yield self.has_group_participation(gamespace_id, group_id, account_transfer_to)
        if not has_participation:
            raise GroupError(406, "Account transfer to is no participating in that group")

        try:
            yield self.db.execute(
                """
                    UPDATE `groups`
                    SET `group_owner`=%s
                    WHERE `gamespace_id`=%s AND `group_id`=%s
                    LIMIT 1;;
                """, account_transfer_to, gamespace_id, group_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to transfer ownership: " + str(e.args[1]))

        if notify and GroupFlags.MESSAGE_SUPPORT in group.flags:
            yield self.__send_message__(
                gamespace_id, GroupsModel.GROUP_CLASS, str(group_id), account_id,
                GroupsModel.MESSAGE_OWNERSHIP_TRANSFERRED, notify)

    @coroutine
    @validate(gamespace_id="int", group_id="int")
    def list_group_participants(self, gamespace_id, group_id, db=None):

        try:
            participants = yield (db or self.db).query(
                """
                    SELECT *
                    FROM `group_participants`
                    WHERE `gamespace_id`=%s AND `group_id`=%s;
                """, gamespace_id, group_id)
        except DatabaseError as e:
            raise GroupError(500, "Failed to list group participants: " + str(e.args[1]))

        raise Return(map(GroupParticipationAdapter, participants))
