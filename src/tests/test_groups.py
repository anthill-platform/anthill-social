from tornado.gen import coroutine, Return, sleep
from tornado.testing import gen_test

from server import SocialServer
from model.group import GroupFlags, GroupJoinMethod, GroupError, GroupsModel
from model.request import NoSuchRequest

import common.testing
import options as _opts


class GroupsTestCase(common.testing.ServerTestCase):
    GAMESPACE_ID = 1
    ACCOUNT_A = 1
    ACCOUNT_B = 2
    ACCOUNT_C = 3
    ACCOUNT_D = 4

    @classmethod
    @coroutine
    def co_setup_class(cls):
        cls.db = yield cls.get_test_db()

        cls.app = SocialServer(cls.db)
        cls.groups = cls.app.groups

        yield cls.app.started()

    @gen_test
    def test_group_create(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {})

        self.assertGreater(group_id, 0, "New group ID must be positive")

    @gen_test
    def test_free_join(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        members = yield self.groups.list_group_participants(GroupsTestCase.GAMESPACE_ID, group_id)

        self.assertEquals(len(members), 1, "Group should have one member from scratch")
        self.assertEquals(members[0].account, GroupsTestCase.ACCOUNT_A, "Member should be ACCOUNT_A")
        self.assertEquals(members[0].role, GroupsModel.MAXIMUM_ROLE, "Member role should be max")

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "b"})

        members = yield self.groups.list_group_participants(GroupsTestCase.GAMESPACE_ID, group_id)

        members = {
            member.account: member
            for member in members
        }

        self.assertEquals(len(members), 2, "After group join there should be two members")
        self.assertEquals(members[GroupsTestCase.ACCOUNT_A].profile, {"test": "a"})
        self.assertEquals(members[GroupsTestCase.ACCOUNT_B].profile, {"test": "b"})
        self.assertEquals(members[GroupsTestCase.ACCOUNT_B].role, GroupsModel.MINIMUM_ROLE,
                          "Free member role should be min")

    @gen_test
    def test_same_join(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {})

        members = yield self.groups.list_group_participants(GroupsTestCase.GAMESPACE_ID, group_id)
        self.assertEquals(len(members), 1, "Group should have one member from scratch")
        self.assertEquals(members[0].account, GroupsTestCase.ACCOUNT_A, "Member should be ACCOUNT_A")

        with self.assertRaises(GroupError) as e:
            yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                         GroupsTestCase.ACCOUNT_A, {"test": "b"})

        self.assertEqual(e.exception.code, 409)

        members = yield self.groups.list_group_participants(GroupsTestCase.GAMESPACE_ID, group_id)
        self.assertEquals(len(members), 1, "Group should have one member from scratch")
        self.assertEquals(members[0].account, GroupsTestCase.ACCOUNT_A, "Member should be ACCOUNT_A")

    @gen_test
    def test_join_limit(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 2, GroupsTestCase.ACCOUNT_A, {})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "a"})

        with self.assertRaises(GroupError) as e:
            yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                         GroupsTestCase.ACCOUNT_C, {"test": "b"})

    @gen_test
    def test_concurrent_group_profile(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {"value": 1}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield [self.groups.update_group(
            GroupsTestCase.GAMESPACE_ID, group_id,
            GroupsTestCase.ACCOUNT_A, {"value": {"@func": "++", "@value": 1}}
        ) for x in xrange(0, 10)]

        updated_group = yield self.groups.get_group(GroupsTestCase.GAMESPACE_ID, group_id)

        self.assertEquals(updated_group.profile, {"value": 11})

    @gen_test
    def test_concurrent_group_participation_profile(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"value": 100})

        yield [self.groups.update_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_A,
            {"value": {"@func": "--", "@value": 1}}
        ) for x in xrange(0, 10)]

        updated_group_participation = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id,
            GroupsTestCase.ACCOUNT_A)

        self.assertEquals(updated_group_participation.profile, {"value": 90})

    @gen_test
    def test_roles(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "b"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_C, {"test": "c"})

        # as an owner I should be able to do that
        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_C,
            1000, [])

        # downgrade own roles
        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_C,
            500, [])

        # now try to push them back up
        with self.assertRaises(GroupError):
            yield self.groups.update_group_participation_permissions(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_C,
                1000, [])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B,
            200, [])

        with self.assertRaises(GroupError):
            yield self.groups.update_group_participation_permissions(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B, GroupsTestCase.ACCOUNT_A,
                100, [])

    @gen_test
    def test_owner(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_A,
            999999999, [])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_A,
            0, [])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_A,
            5000, ["root"])

        updated_group_participation = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A)

        self.assertEqual(updated_group_participation.permissions, {"root"},
                         "Permissions of account C should be root")
        self.assertEqual(updated_group_participation.role, 5000, "Role should be 5000")

    @gen_test
    def test_ownership(self):

        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "b"})

        with (self.assertRaises(GroupError)) as e:
            yield self.groups.leave_group(GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A)
        self.assertEqual(e.exception.code, 409)

        with (self.assertRaises(GroupError)) as e:
            yield self.groups.transfer_ownership(GroupsTestCase.GAMESPACE_ID, group_id,
                                                 GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_C)
        self.assertEqual(e.exception.code, 406)

        yield self.groups.transfer_ownership(GroupsTestCase.GAMESPACE_ID, group_id,
                                             GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B)
        yield self.groups.leave_group(GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A)

    @gen_test
    def test_roles_permissions(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "b"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_C, {"test": "c"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_D, {"test": "d"})

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B,
            200, ["cat", "dog", "cow"])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B, GroupsTestCase.ACCOUNT_C,
            199, ["cow", "cat", "fox"])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_D,
            198, ["cat", "chicken", "pig"])

        updated_group_participation = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C)

        self.assertEqual(updated_group_participation.permissions, {"cat", "cow"},
                         "Permissions of account C should be cat,cow")

        updated_group_participation = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D)

        self.assertEqual(updated_group_participation.permissions, {"cat"},
                         "Permissions of account D should be cat")

    @gen_test
    def test_kick(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.FREE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_B, {"test": "b"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_C, {"test": "c"})

        yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                     GroupsTestCase.ACCOUNT_D, {"test": "d"})

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B,
            500, [])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_C,
            400, [GroupsModel.PERMISSION_KICK])

        yield self.groups.update_group_participation_permissions(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_D,
            300, [])

        # kick an owner
        with self.assertRaises(GroupError) as e:
            yield self.groups.kick_from_group(
                GroupsTestCase.GAMESPACE_ID, group_id,
                GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_A)

        self.assertEqual(e.exception.code, 406, "Should be 'You cannot kick an owner'")

        # kick higher role
        with self.assertRaises(GroupError) as e:
            yield self.groups.kick_from_group(
                GroupsTestCase.GAMESPACE_ID, group_id,
                GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_B)

        self.assertEqual(e.exception.code, 406, "Should be'You cannot kick a player with a higher role'")

        # kick with no permissions to
        with self.assertRaises(GroupError) as e:
            yield self.groups.kick_from_group(
                GroupsTestCase.GAMESPACE_ID, group_id,
                GroupsTestCase.ACCOUNT_B, GroupsTestCase.ACCOUNT_C)

        self.assertEqual(e.exception.code, 406, "Should be 'You have no permission to kick'")

        # should kick just fine
        yield self.groups.kick_from_group(
            GroupsTestCase.GAMESPACE_ID, group_id,
            GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_D)

        # kick being owner
        yield self.groups.kick_from_group(
            GroupsTestCase.GAMESPACE_ID, group_id,
            GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_C)

    @gen_test
    def test_approve(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.APPROVE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        # free join to approve-based group is prohibited
        with (self.assertRaises(GroupError)) as e:
            yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                         GroupsTestCase.ACCOUNT_B, {"test": "b"})

        self.assertEqual(e.exception.code, 409)

        key_b = yield self.groups.join_group_request(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B, {"bbb": 555})
        key_c = yield self.groups.join_group_request(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, {"ccc": 666})
        key_d = yield self.groups.join_group_request(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D, {"ddd": 777})

        yield self.groups.approve_join_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B,
            900, key_b, ["test"])

        # give account C a permission to approve other requests
        yield self.groups.approve_join_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_C,
            950, key_c, [GroupsModel.PERMISSION_REQUEST_APPROVAL])

        # approve by B who has no such permission
        with self.assertRaises(GroupError) as e:
            yield self.groups.approve_join_group(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B, GroupsTestCase.ACCOUNT_D,
                800, key_d, [])

        self.assertEqual(e.exception.code, 406, "Should be 'You have no permission to approve items'")

        # approve by C but raise the role more than us
        with self.assertRaises(GroupError) as e:
            yield self.groups.approve_join_group(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_D,
                960, key_d, [])

        self.assertEqual(e.exception.code, 409, "Should be 'Approved role cannot be higher than your role'")

        # do the actual approval
        yield self.groups.approve_join_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, GroupsTestCase.ACCOUNT_D,
            940, key_d, [])

        updated_group_participation_b = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B)
        updated_group_participation_c = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C)
        updated_group_participation_d = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D)

        self.assertEquals(updated_group_participation_b.role, 900)
        self.assertEquals(updated_group_participation_b.profile, {"bbb": 555})
        self.assertEquals(updated_group_participation_c.role, 950)
        self.assertEquals(updated_group_participation_c.profile, {"ccc": 666})
        self.assertEquals(updated_group_participation_d.role, 940)
        self.assertEquals(updated_group_participation_d.profile, {"ddd": 777})

        # use same key twice
        with (self.assertRaises(NoSuchRequest)):
            yield self.groups.approve_join_group(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A, GroupsTestCase.ACCOUNT_B,
                950, key_b, ["test"])

    @gen_test
    def test_invite(self):
        group_id = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.INVITE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"})

        # free join to invite-based group is prohibited
        with (self.assertRaises(GroupError)) as e:
            yield self.groups.join_group(GroupsTestCase.GAMESPACE_ID, group_id,
                                         GroupsTestCase.ACCOUNT_B, {"test": "b"})

        self.assertEqual(e.exception.code, 409)

        key_b = yield self.groups.invite_to_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A,
            GroupsTestCase.ACCOUNT_B, 500, [])

        key_c = yield self.groups.invite_to_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_A,
            GroupsTestCase.ACCOUNT_C, 600, [GroupsModel.PERMISSION_SEND_INVITE])

        yield self.groups.accept_group_invitation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B, {"b": True}, key_b)

        yield self.groups.accept_group_invitation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C, {"c": True}, key_c)

        updated_group_participation_b = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B)

        self.assertEquals(updated_group_participation_b.role, 500)
        self.assertEquals(updated_group_participation_b.profile, {"b": True})

        updated_group_participation_c = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C)

        self.assertEquals(updated_group_participation_c.role, 600)
        self.assertEquals(updated_group_participation_c.profile, {"c": True})

        with (self.assertRaises(GroupError)) as e:
            yield self.groups.invite_to_group(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_B,
                GroupsTestCase.ACCOUNT_D, 400, [])

        self.assertEqual(e.exception.code, 406, "Should be 'You have no permission to send invites'")

        key_d = yield self.groups.invite_to_group(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_C,
            GroupsTestCase.ACCOUNT_D, 400, [])

        # use wrong key
        with (self.assertRaises(GroupError)) as e:
            yield self.groups.accept_group_invitation(
                GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D, {"d": False}, key_c)

        self.assertEqual(e.exception.code, 410, "Should be 'No such invite request'")

        yield self.groups.accept_group_invitation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D, {"d": False}, key_d)

        updated_group_participation_d = yield self.groups.get_group_participation(
            GroupsTestCase.GAMESPACE_ID, group_id, GroupsTestCase.ACCOUNT_D)

        self.assertEquals(updated_group_participation_d.role, 400)
        self.assertEquals(updated_group_participation_d.profile, {"d": False})

    @gen_test
    def test_search(self):
        group_a = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.INVITE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"},
            group_name="Lorem ipsum dolor sit amet, consectetur adipiscing elit, including same text at the end!")

        group_b = yield self.groups.create_group(
            GroupsTestCase.GAMESPACE_ID, {}, GroupFlags([]),
            GroupJoinMethod(GroupJoinMethod.INVITE), 50, GroupsTestCase.ACCOUNT_A, {"test": "a"},
            group_name="The quick brown fox jumps over the lazy dog, including same text at the end!")

        result_1 = yield self.groups.search_groups(GroupsTestCase.GAMESPACE_ID, "quick brown fox")
        self.assertEquals(len(result_1), 1)
        self.assertEquals(result_1[0].group_id, group_b)

        result_2 = yield self.groups.search_groups(GroupsTestCase.GAMESPACE_ID, "Lor")
        self.assertEquals(len(result_2), 1)
        self.assertEquals(result_2[0].group_id, group_a)

        result_3 = yield self.groups.search_groups(GroupsTestCase.GAMESPACE_ID, "including same text")
        self.assertEquals(len(result_3), 2)
