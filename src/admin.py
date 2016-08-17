import common.admin as a

from tornado.gen import coroutine


class RootAdminController(a.AdminController):
    def render(self, data):
        return [
            a.notice("Not implemented", "Not implemented yet.")
        ]

    def scopes_read(self):
        return ["social_admin"]

    def scopes_write(self):
        return ["social_admin"]
