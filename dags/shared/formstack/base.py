from typing import Union
from types import LambdaType, FunctionType
from requests import Session
from shared.irondata import Irondata


class Formstack(Session):
    root: str = "https://www.formstack.com/api/v2"
    endpoint: str = ""

    oauth_token: str = Irondata.get_config('FORMSTACK_OAUTH_ACCESS_TOKEN')


    def __init__(self):
        Session.__init__(self)
        
        self.params.update(
            {
                "oauth_token": self.oauth_token,
                "data": "true",
                "expand_data": "true",
                }
            )
        
    
    def get(self, endpoint, construct=True, check_response=True, **kwargs):
        if construct:
            request = self.root + self.endpoint + endpoint
        else:
            request = endpoint
            
        response = super().get(request, **kwargs)
        
        if check_response:
            self.check_response(response)

        return response

    def check_response(self, response: dict):
        assert "error" not in response.json(), "Formstack responsed with an error: " + response.json()["error"]


    def paginate(self,
                 endpoint,
                 per_page=50,
                 test_limit=0,
                 method:str="append",
                 processor: Union[LambdaType, FunctionType]=lambda x: x,
                **kwargs
                ):
        current_page = 1
        self.params.update(dict(per_page=per_page, page=current_page, **kwargs))
        response = self.get(endpoint).json()
        processed = processor(response)
        total_pages = response.get("pages")
        data = [x for x in processed] if isinstance(processed, list) else [processed]

        while self.params["page"] < total_pages:
            self.params["page"] += 1
            response = self.get(endpoint).json()
            
            processed = processor(response)
            if method == "extend":
                data.extend(processed)
            if method == "add":
                data += processed
            else:
                data.append(processed)

            if test_limit and self.params["page"] >= test_limit:
                break

        self.params.pop("per_page")
        self.params.pop("page")
        return data
