
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

import options as _opts
import admin


class SocialServer(common.server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(SocialServer, self).__init__()

        self.db = common.database.Database(
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
        self.connections = ConnectionsModel(self.db, self.cache)
        self.requests = RequestsModel(self.db, self.cache)
        self.social = SocialAPIModel(self, self.tokens, self.cache)

    def get_models(self):
        return [self.tokens, self.connections, self.requests]

    def get_admin(self):
        return {
            "index": admin.RootAdminController
        }

    def get_handlers(self):
        return [
            (r"/connections", h.ConnectionsHandler),
            (r"/requests/sent", h.RequestsSentHandler),
            (r"/requests", h.RequestsHandler),
            (r"/external", h.ExternalHandler)
        ]

    def get_metadata(self):
        return {
            "title": "Social",
            "description": "Manage social networks and friend connections",
            "icon": "share-alt-square"
        }

    def get_internal_handler(self):
        return h.InternalHandler(self)


if __name__ == "__main__":
    stt = common.server.init()
    common.access.AccessToken.init([common.access.public()])
    common.server.start(SocialServer)
