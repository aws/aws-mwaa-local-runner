from typing import Union, List
from shared.formstack.base import Formstack
from shared.decorators import get, paginate


class Form(Formstack):

    endpoint = "/form"

    @get
    def form(
        self,
        form_id: Union[int, str]
        ) -> dict:
        
        return f"/{form_id}.json"

    @paginate
    def submissions(
        self,
        form_id: Union[int, str],
        min_time="", 
        max_time="",
        **kwargs
        ) -> List[dict]:

        return {
            "endpoint": f"/{form_id}/submission.json",
            "min_time": min_time,
            "max_time": max_time,
            "processor": lambda x: x["submissions"],
            "method": "add",
            **kwargs,
            }

class Submission(Formstack):

    endpoint = '/submission'

    @get
    def submission(
            self,
            submission_id: Union[int, str]
            ) -> dict:
        
        return f"/{submission_id}.json"