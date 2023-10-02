from .base import Canvas
from .decorators import get, paginate, course, user, account, cache
from typing import Union


class CanvasAPI(Canvas):

    @course
    @get
    def get_course(self, id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f'/api/v1/courses/{id}',
            params=kwargs,
            )

    @cache("courses", replace=True)
    @course
    @paginate
    def get_courses(self, **kwargs) -> dict:
        return dict(
            endpoint='/api/v1/courses',
            params=kwargs,
            )

    @user
    @get
    def get_user(self, id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f'/api/v1/users/{id}',
            params=kwargs,
            )

    @account
    @get
    def get_account(self, id: Union[int, str]=1, **kwargs) -> dict:
        return dict(
            endpoint=f'/api/v1/accounts/{id}',
            params=kwargs,
            )
    
