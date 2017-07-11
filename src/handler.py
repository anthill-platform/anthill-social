
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError

from common import to_int
from common.internal import InternalError
from common.social import APIError, AuthResponse
from common.handler import AuthenticatedHandler
from common.access import scoped, AccessToken, parse_scopes
from common.validate import validate, validate_value, ValidationError

from model.request import RequestError, RequestType, NoSuchRequest
from model.connection import ConnectionError, ConnectionsModel
from model.social import SocialNotFound, NoFriendsFound, SocialAuthenticationRequired
from model.group import GroupError, GroupsModel, GroupFlags, NoSuchGroup, NoSuchParticipation, GroupJoinMethod


class ConnectionsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):
        profile_fields = filter(
            bool, 
            self.get_argument("profile_fields", "").split(","))

        try:
            connections = yield self.application.connections.get_connections_profiles(
                self.token.get(AccessToken.GAMESPACE),
                self.token.account,
                profile_fields)
            
        except ConnectionError as e:
            raise HTTPError(e.code, e.message)

        self.dumps(connections)


class AccountConnectionHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def delete(self, target_account):

        gamespace = self.token.get(AccessToken.GAMESPACE)

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.connections.delete(
                gamespace, self.token.account, target_account, notify=notify, authoritative=authoritative)

        except ConnectionError as e:
            raise HTTPError(e.code, e.message)

    def options(self):
        self.set_header("Access-Control-Allow-Methods", "POST,DELETE,OPTIONS")

    @scoped()
    @coroutine
    def post(self, target_account):

        approval = self.get_argument("approval", "true") == "true"
        gamespace = self.token.get(AccessToken.GAMESPACE)

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        if not approval and not self.token.has_scope(ConnectionsModel.APPROVAL_SCOPE):
            raise HTTPError(403, "Scope '{0}' is required if approval is disabled".format(
                ConnectionsModel.APPROVAL_SCOPE))

        try:
            result = yield self.application.connections.request_connection(
                gamespace, self.token.account, target_account, approval=approval, notify=notify,
                authoritative=authoritative)
        except ConnectionError as e:
            raise HTTPError(e.code, e.message)

        if result:
            self.dumps(result)


class ApproveConnectionHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, approve_account_id):
        key = self.get_argument("key")
        gamespace = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.connections.approve_connection(
                gamespace, account_id, approve_account_id, key, notify=notify)
        except ConnectionError as e:
            raise HTTPError(e.code, e.message)


class RejectConnectionHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, reject_account_id):

        key = self.get_argument("key")
        gamespace = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.connections.reject_connection(
                gamespace, account_id, reject_account_id, key, notify=notify, authoritative=authoritative)
        except ConnectionError as e:
            raise HTTPError(500, e.message)


class ExternalConnectionsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):
        try:
            friends = yield self.application.social.list_friends(
                self.token.get(AccessToken.GAMESPACE),
                self.token.account)

        except SocialAuthenticationRequired as e:
            raise HTTPError(401, ujson.dumps({
                "credential": e.credential,
                "username": e.username
            }))

        except NoFriendsFound:
            raise HTTPError(404, "No connections found")

        except APIError as e:
            raise HTTPError(e.code, e.body)

        self.dumps(friends)


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @coroutine
    def attach_account(self, gamespace, credential, username, account, env=None):
        yield self.application.tokens.attach(
            gamespace,
            credential,
            username,
            account)

        try:
            api = self.application.social.api(credential)
        except SocialNotFound:
            raise InternalError(404, "No such credential: '{0}'.".format(credential))

        try:
            result = yield api.get_social_profile(gamespace, username, account, env=env)
        except APIError as e:
            raise InternalError(e.code, e.message)
        else:
            raise Return(result)

    @coroutine
    def import_social(self, gamespace, username, credential, auth):

        if not isinstance(auth, dict):
            raise InternalError(400, "Auth should be a dict")

        auth = AuthResponse(**auth)

        social = self.application.social
        try:
            api = social.api(credential)
        except SocialNotFound:
            raise InternalError(404, "No such credential: '{0}'.".format(credential))

        try:
            result = yield api.import_social(
                gamespace,
                username,
                auth)

        except APIError as e:
            raise HTTPError(e.code, e.message)
        else:
            raise Return(result)

    @coroutine
    def get_connections(self, gamespace, account_id, profile_fields):

        connections_data = self.application.connections

        try:
            connections = yield connections_data.get_connections_profiles(
                gamespace,
                account_id,
                profile_fields)

        except ConnectionError as e:
            raise HTTPError(e.code, e.message)

        raise Return(connections)

    @coroutine
    def get_group(self, gamespace, group_id):

        try:
            group, participants = yield self.application.groups.get_group_with_participants(
                gamespace, group_id)
        except NoSuchGroup:
            raise InternalError(404, "No such group")
        except GroupError as e:
            raise InternalError(e.code, e.message)

        group_out = {
            "group_id": group.group_id,
            "profile": group.profile,
            "join_method": str(group.join_method),
            "free_members": group.free_members,
            "owner": str(group.owner)
        }

        if group.name:
            group_out["name"] = group.name

        result = {
            "group": group_out,
            "participants": {
                int(participant.account): {
                    "role": participant.role,
                    "permissions": participant.permissions,
                    "profile": participant.profile
                }
                for participant in participants
            }
        }

        if GroupFlags.MESSAGE_SUPPORT in group.flags:
            result["message"] = {
                "recipient_class": GroupsModel.GROUP_CLASS,
                "recipient": str(group_id),
            }

        raise Return(result)

    @coroutine
    @validate(gamespace="int", group_id="int", profile="json_dict", path="json_list_of_strings", merge="bool")
    def update_group_profile(self, gamespace, group_id, profile, path=None, merge=True):

        try:
            result = yield self.application.groups.update_group_no_check(
                gamespace, group_id, profile, path=path, merge=merge)
        except GroupError as e:
            raise InternalError(e.code, e.message)

        raise Return(result)


class CreateGroupHandler(AuthenticatedHandler):
    @scoped(scopes=["group_create"])
    @coroutine
    def post(self):

        join_method_str = self.get_argument("join_method", GroupJoinMethod.FREE)
        max_members = self.get_argument("max_members", GroupsModel.DEFAULT_MAX_MEMBERS)
        group_name = self.get_argument("name", None)

        if join_method_str not in GroupJoinMethod:
            raise HTTPError(400, "Invalid join method")

        join_method = GroupJoinMethod(join_method_str)

        try:
            group_profile = ujson.loads(self.get_argument("group_profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        try:
            participation_profile = ujson.loads(self.get_argument("participation_profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        group_messages = self.get_argument("group_messages", "true") == "true"

        flags = GroupFlags()

        if group_messages:
            flags.set(GroupFlags.MESSAGE_SUPPORT)

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            group_id = yield self.application.groups.create_group(
                gamespace, group_profile, flags, join_method, max_members,
                account, participation_profile, group_name=group_name)
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "id": group_id
        })


class SearchGroupsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):

        query = self.get_argument("query")

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            groups = yield self.application.groups.search_groups(gamespace, query)
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "groups": [
                {
                    "group": {
                        "group_id": str(group.group_id),
                        "profile": group.profile,
                        "join_method": str(group.join_method),
                        "free_members": int(group.free_members),
                        "owner": str(group.owner),
                        "name": group.name
                    }
                } for group in groups
            ]
        })


class GroupHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def get(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        try:
            group, participants, my_participation = yield self.application.groups.get_group_with_participants(
                gamespace, group_id, account_id)
        except NoSuchGroup as e:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        group_out = {
            "group_id": group.group_id,
            "profile": group.profile,
            "join_method": str(group.join_method),
            "free_members": group.free_members,
            "owner": str(group.owner)
        }

        if group.name:
            group_out["name"] = group.name

        result = {
            "group": group_out,
            "participants": {
                participant.account: {
                    "role": participant.role,
                    "permissions": participant.permissions,
                    "profile": participant.profile
                }
                for participant in participants
            }
        }

        if my_participation:
            result["me"] = {
                "role": my_participation.role,
                "permissions": my_participation.permissions,
                "profile": my_participation.profile
            }

            if GroupFlags.MESSAGE_SUPPORT in group.flags:
                result["message"] = {
                    "recipient_class": GroupsModel.GROUP_CLASS,
                    "recipient": str(group_id),
                }

        self.dumps(result)

    @scoped(scopes=["group", "group_write"])
    @coroutine
    def post(self, group_id):

        new_name = self.get_argument("name", None)
        new_join_method_str = self.get_argument("join_method", None)

        if new_join_method_str:
            if new_join_method_str not in GroupJoinMethod.ALL:
                raise HTTPError(400, "Bad 'join_method'")

            new_join_method = GroupJoinMethod(new_join_method_str)
        else:
            new_join_method = None

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")
        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            yield self.application.groups.update_group_summary(
                gamespace, group_id, account, name=new_name, join_method=new_join_method,
                notify=notify, authoritative=authoritative)
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupProfileHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def get(self, group_id):

        account_id = self.token.account
        gamespace = self.token.get(AccessToken.GAMESPACE)

        try:
            group = yield self.application.groups.get_group(gamespace, group_id)
        except NoSuchGroup as e:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        try:
            participant = yield self.application.groups.has_group_participation(gamespace, group_id, account_id)
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        group_out = {
            "group_id": group.group_id,
            "profile": group.profile,
            "join_method": str(group.join_method),
            "free_members": group.free_members,
            "owner": str(group.owner),
        }

        if group.name:
            group_out["name"] = group.name

        result = {
            "group": group_out,
            "participant": participant
        }

        self.dumps(result)

    @scoped(scopes=["group", "group_write"])
    @coroutine
    def post(self, group_id):

        try:
            group_profile = ujson.loads(self.get_argument("profile"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")
        merge = self.get_argument("merge", "true") == "true"
        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            result = yield self.application.groups.update_group(
                gamespace, group_id, account, group_profile, merge=merge,
                notify=notify, authoritative=authoritative)
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "group": {
                "profile": result
            }
        })


class GroupJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            participation_profile = ujson.loads(self.get_argument("participation_profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.join_group(
                gamespace, group_id, account,
                participation_profile, notify=notify,
                authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupAcceptInvitationHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account
        key = self.get_argument("key")

        try:
            participation_profile = ujson.loads(self.get_argument("participation_profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.accept_group_invitation(
                gamespace, group_id, account,
                participation_profile, key=key,
                notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupRejectInvitationHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account
        key = self.get_argument("key")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.reject_group_invitation(
                gamespace, group_id, account, key=key,
                notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupLeaveHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.leave_group(
                gamespace, group_id, account, notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupOwnershipHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        account_transfer_to = self.get_argument("account_transfer_to")

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.transfer_ownership(
                gamespace, group_id, account, account_transfer_to,
                notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupRequestJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        account = self.token.account

        try:
            participation_profile = ujson.loads(self.get_argument("participation_profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            key = yield self.application.groups.join_group_request(
                gamespace, group_id, account, participation_profile,
                notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "key": key
        })


class GroupInviteAccountJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id, invite_account):

        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account
        role = self.get_argument("role")

        try:
            permissions = ujson.loads(self.get_argument("permissions"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Permissions json is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            key = yield self.application.groups.invite_to_group(
                gamespace_id, group_id, account_id, invite_account,
                role, permissions, notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except NoSuchParticipation:
            raise HTTPError(406, "You are not a member of this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "key": key
        })


class GroupApproveAccountJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id, approve_account):

        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        role = to_int(self.get_argument("role"))
        key = self.get_argument("key")

        try:
            permissions = ujson.loads(self.get_argument("permissions"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Permissions json is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.approve_join_group(
                gamespace_id, group_id, account_id, approve_account,
                role, key, permissions, notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except NoSuchRequest:
            raise HTTPError(404, "No such request")
        except NoSuchParticipation:
            raise HTTPError(406, "You are not a member of this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupRejectAccountJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id, reject_account):

        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        account_id = self.token.account

        key = self.get_argument("key")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.reject_join_group(
                gamespace_id, group_id, account_id, reject_account,
                key, notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except NoSuchRequest:
            raise HTTPError(404, "No such request")
        except NoSuchParticipation:
            raise HTTPError(406, "You are not a member of this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupParticipationHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def get(self, group_id, account_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)

        if account_id == "me":
            account_id = self.token.account

        try:
            owner = yield self.application.groups.is_group_owner(
                gamespace, group_id, account_id)
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        try:
            participation = yield self.application.groups.get_group_participation(
                gamespace, group_id, account_id)
        except NoSuchParticipation as e:
            raise HTTPError(404, "Player is not participating this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)

        self.dumps({
            "participation": {
                "profile": participation.profile,
                "role": participation.role,
                "permissions": participation.permissions
            },
            "owner": owner
        })

    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id, account_id):

        if account_id == "me":
            account_id = self.token.account
        else:
            account_id = to_int(account_id)

        try:
            participation_profile = ujson.loads(self.get_argument("profile"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Profile is corrupted")

        gamespace = self.token.get(AccessToken.GAMESPACE)
        my_account = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")
        merge = self.get_argument("merge", "true") == "true"

        try:
            result = yield self.application.groups.update_group_participation(
                gamespace, group_id, my_account, account_id, participation_profile, merge=merge,
                notify=notify, authoritative=authoritative)
        except NoSuchParticipation:
            raise HTTPError(404, "Player is not participating this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)
        else:
            self.dumps({
                "profile": result
            })

    @scoped(scopes=["group"])
    @coroutine
    def delete(self, group_id, account_id):

        gamespace = self.token.get(AccessToken.GAMESPACE)
        my_account = self.token.account

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        try:
            yield self.application.groups.kick_from_group(
                gamespace, group_id, my_account, account_id,
                notify=notify, authoritative=authoritative)
        except NoSuchParticipation:
            raise HTTPError(404, "Player is not participating this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)


class GroupParticipationPermissionsHandler(AuthenticatedHandler):
    @scoped(scopes=["group"])
    @coroutine
    def post(self, group_id, account_id):

        if account_id == "me":
            account_id = self.token.account
        else:
            account_id = to_int(account_id)

        try:
            permissions = validate_value(ujson.loads(self.get_argument("permissions")), "json_list_of_str_name")
        except (KeyError, ValueError, ValidationError):
            raise HTTPError(400, "Permissions json is corrupted")

        notify_str = self.get_argument("notify", None)
        if notify_str:
            try:
                notify = ujson.loads(notify_str)
            except (KeyError, ValueError):
                raise HTTPError(400, "Notify is corrupted")
        else:
            notify = None

        authoritative = self.token.has_scope("message_authoritative")

        target_role = to_int(self.get_argument("role"))

        gamespace = self.token.get(AccessToken.GAMESPACE)
        my_account = self.token.account

        try:
            yield self.application.groups.update_group_participation_permissions(
                gamespace, group_id, my_account, account_id, target_role, permissions,
                notify=notify, authoritative=authoritative)
        except NoSuchGroup:
            raise HTTPError(404, "No such group")
        except NoSuchParticipation:
            raise HTTPError(406, "Player is not participating this group")
        except GroupError as e:
            raise HTTPError(e.code, e.message)
