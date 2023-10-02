from .base import CanvasEndpoint
from ..decorators import paginate, get, post, cache, course, user, account
from typing import Union


class CanvasAccount(CanvasEndpoint):

    endpoint = '/api/v1/accounts'
    id: Union[str, int]

    @cache("courses", replace=True)  # store results as class attribute
    @course  # convert pagination results to CanvasCourse objects
    @paginate  # paginate the endpoint
    def get_courses(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/courses',
            params=kwargs,
            )

    @cache("subaccounts", replace=True)
    @account
    @paginate
    def get_subaccounts(self, **kwargs) -> dict:
        return dict(
            endpoint=f"/{self.id}/sub_accounts",
            params=kwargs,
            )

    @post
    def set_admin(self, user_id: Union[int, str], **kwargs) -> dict:
        params = {"user_id": user_id}
        params.update(kwargs)
        self.params.update(params)
        return dict(
            endpoint=f'/{self.id}/admins',
            params=kwargs
            )

    @user
    @paginate
    def get_users(self, **kwargs):
        return dict(
            endpoint=f'/{self.id}/users',
            params=kwargs
        )

