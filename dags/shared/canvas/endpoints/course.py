from .base import CanvasEndpoint
from ..decorators import paginate, get, cache, user, course
from typing import Union, List


class CanvasCourse(CanvasEndpoint):

    endpoint = '/api/v1/courses'
    id: Union[str, int]

    def __init__(self, **data):
        super().__init__(**data)

        if hasattr(self, 'course_code') and 'flex' in self.course_code.lower():
            self.flex = True
        else:
            self.flex = False

    @cache("students", replace=True)  # store results as class attribute
    @user  # convert pagination results to CanvasUsers objects
    @paginate  # paginate the endpoint
    def get_students(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/students',
            params=kwargs,
            )

    @cache("users")
    @user
    @paginate
    def get_users(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/users',
            params=kwargs,
            )

    @cache("quizzes")
    @paginate
    def get_quizzes(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/quizzes',
            params=kwargs,
            )

    @get  # get request the endpoint
    def get_quiz(self, quiz_id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/quizzes/{quiz_id}',
            params=kwargs,
            )
    
    @get
    def get_quiz_statistics(self, quiz_id: Union[int, str], **kwargs):
        return dict(
            endpoint=f'/{self.id}/quizzes/{quiz_id}/statistics',
            **kwargs,
            )
    
    # DEV
    # @paginate
    # def _get_quiz_submissions(self, quiz_id: Union[int, str], **kwargs):
    #     return dict(
    #         endpoint=f'/{self.id}/quizzes/{quiz_id}/submissions',
    #         params=kwargs,
    #         )
    
    # DEV
    # @quiz_submission
    # def get_quiz_submissions(self, quiz_id: Union[int, str], **kwargs):
    #     data = self._get_quiz_submissions(quiz_id=quiz_id, **kwargs)
    #     return_list = []
    #     for result in data:
    #         if isinstance(result, dict) and result.get("quiz_submissions"):
    #             return_list += result.get("quiz_submissions")
    #     print(data)
    #     print('_', return_list)
    #     if not return_list:
    #         print('71', data)
        
    #    return return_list
    
    @paginate
    def get_quiz_submission_events(self, quiz_id, submission_id, **kwargs):
        return dict(
            endpoint=f'/{self.id}/quizzes/{quiz_id}/submissions/{submission_id}/events',
            params=kwargs,
            )

    @paginate
    def get_user_course_todos(self, user_id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f"/{self.id}/todo?as_user_id={user_id}",
            params=kwargs,
            )

    @cache("assignments", replace=True)
    @paginate
    def get_assignments(self, **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/assignments',
            params=kwargs,
            )

    def get_assignment(self, assignment_id: Union[int, str], controller: str="assignments") -> dict:
        course_id = self.id
        endpoints = {
            "wiki_pages": f'/{course_id}/pages/{assignment_id}',
            "assignments": f'/{course_id}/assignments/{assignment_id}',
            "quizzes/quizzes": f'/{course_id}/quizzes/{assignment_id}',
            }
        endpoint = endpoints[controller]
        response = self.get(endpoint)
        return response.json()

    def get_assignment_submissions(self, assignment_id: Union[int, str]) -> List[dict]:
        submission_endpoint = f"/{self.id}/assignments/{assignment_id}/submissions"
        results = self.paginate(submission_endpoint)

        return [dict(course_id=self.id,
                     **result)
                for result in results]

    @cache("student_page_views", replace=True)
    def get_student_page_views(self, **kwargs) -> dict:
        if not (hasattr(self, 'students') and self.students):
            self.get_students()

        page_views = {}
        for student in self.students:
            endpoint = f'/{self.id}/analytics/users/{student.id}/activity'
            data = self.paginate(endpoint, **kwargs)
            page_views[student.id] = data

        return page_views

    @get
    def get_user_progress(self, user_id: Union[int, str], **kwargs) -> dict:
        return dict(
            endpoint=f'/{self.id}/users/{user_id}/progress',
            params=kwargs,
            )
    
    @course
    @paginate
    def get_blueprint_associations(self, **kwargs):
        return dict(
            endpoint=f'/{self.id}/blueprint_templates/default/associated_courses',
            params=kwargs,
            )
    

