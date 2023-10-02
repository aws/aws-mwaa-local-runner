from .base import CanvasEndpoint
from ..decorators import paginate, cache, course
from datetime import datetime
from typing import Union
    

class CanvasUser(CanvasEndpoint):

    endpoint = '/api/v1/users'
    id: Union[str, int]

    @paginate
    def get_page_views(self, start_time: datetime = '', end_time: datetime = '') -> str:
        strftime = '%Y-%m-%dT%H:%M:%SZ'
        endpoint = (f'/{self.id}/page_views'
                    f'?start_time={start_time.strftime(strftime) if start_time else ""}'
                    f'&end_time={end_time.strftime(strftime) if end_time else ""}') 
        return endpoint

    @paginate
    def get_course_assignments(self, course_id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/courses/{course_id}/assignments',
            params=kwargs,
            )
    
    @cache("courses")
    @course
    @paginate
    def get_courses(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/courses',
            params= kwargs
            )
