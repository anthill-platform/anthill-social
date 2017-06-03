
from common.options import options

import handler as h

import common.server
import common.database
import common.access
import common.sign
import common.keyvalue

from model.connection import ConnectionsModel
from model.request import RequestsModel
from model.social import SocialAPIModel
from model.token import SocialTokensModel
from model.group import GroupsModel

import options as _opts
import admin


class SocialServer(common.server.Server):
    # noinspection PyShadowingNames
    def __init__(self, db=None):
        super(SocialServer, self).__init__()

        self.db = db or common.database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.cache = common.keyvalue.KeyValueStorage(
            host=options.cache_host,
            port=options.cache_port,
            db=options.cache_db,
            max_connections=options.cache_max_connections)

        self.tokens = SocialTokensModel(self.db)
        self.requests = RequestsModel(self.db, self.cache)
        self.connections = ConnectionsModel(self.db, self.cache, self.requests)
        self.social = SocialAPIModel(self, self.tokens, self.cache)
        self.groups = GroupsModel(self.db, self.requests)

    def get_models(self):
        return [self.tokens, self.requests, self.connections, self.groups]

    def get_admin(self):
        return {
            "index": admin.RootAdminController
        }

    def get_handlers(self):
        return [
            (r"/connections", h.ConnectionsHandler),
            (r"/connection/([0-9]+)/approve", h.ApproveConnectionHandler),
            (r"/connection/([0-9]+)/reject", h.RejectConnectionHandler),
            (r"/connection/([0-9]+)", h.AccountConnectionHandler),

            (r"/external", h.ExternalConnectionsHandler),

            (r"/group/create", h.CreateGroupHandler),
            (r"/group/([0-9]+)/participation/(.+)/permissions", h.GroupParticipationPermissionsHandler),
            (r"/group/([0-9]+)/participation/(.+)", h.GroupParticipationHandler),
            (r"/group/([0-9]+)/join", h.GroupJoinHandler),
            (r"/group/([0-9]+)/leave", h.GroupLeaveHandler),
            (r"/group/([0-9]+)/ownership", h.GroupOwnershipHandler),
            (r"/group/([0-9]+)/request", h.GroupRequestJoinHandler),
            (r"/group/([0-9]+)/approve/([0-9]+)", h.GroupApproveJoinHandler),
            (r"/group/([0-9]+)/invite/([0-9]+)", h.GroupInviteJoinHandler),
            (r"/group/([0-9]+)", h.GroupHandler)
        ]

    def get_metadata(self):
        return {
            "title": "Social",
            "description": "Manage social networks, groups and friend connections",
            "icon": "share-alt-square"
        }

    def get_internal_handler(self):
        return h.InternalHandler(self)


if __name__ == "__main__":
    stt = common.server.init()
    common.access.AccessToken.init([common.access.public()])
    common.server.start(SocialServer)
