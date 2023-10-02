from .base import CanvasEndpoint
from ..decorators import paginate
from typing import Union, List


class QuizSubmission(CanvasEndpoint):

    endpoint = '/api/v1/quiz_submissions'
    id = None

    @paginate
    def get_submission_questions(self, **kwargs):
        return dict(
            endpoint=f'/{self.id}/questions',
            params=kwargs,
            )
