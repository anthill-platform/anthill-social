
import anthill.common.admin as a


class RootAdminController(a.AdminController):
    def render(self, data):
        return [
            a.notice("Not implemented", "Not implemented yet.")
        ]

    def access_scopes(self):
        return ["social_admin"]

