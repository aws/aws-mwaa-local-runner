from time import sleep
from shared.irondata import Irondata
from requests import Session
from requests.models import Response
from typing import Union


class CanvasResponseError(Exception):
    pass

class Canvas(Session):

    root = 'https://learning.flatironschool.com'

    def __init__(self, token=None, **kwargs):
        Session.__init__(self)
        
        self.set_headers(token)

        self._keys = list(kwargs.keys())
        if kwargs:
            for key in kwargs:
                setattr(self, key, kwargs[key])

        self.validate()

    def set_headers(self, token: str) -> None:
        if not token:
            token = Irondata.get_config('CANVAS_API_TOKEN')
            assert token, ("A token must be provided " 
                           "in the init or a CANVAS_API_TOKEN " 
                           "environment variable must be set"
                           )
        
        self.headers['Authorization'] = 'Bearer ' + token
        self.token = token

    def get(self, endpoint: str, construct: bool=True, **kwargs) -> Response:
        if construct:
            request = self.construct_request(endpoint=endpoint)
        else:
            request = endpoint
            
        response = super().get(request, **kwargs) 
        self.check_response(response)
        return response

    def post(self, endpoint: str, construct: bool=True, **kwargs) -> Response:
        if construct:
            url = self.construct_request(endpoint)
        else:
            url = endpoint
        
        return super().post(url, **kwargs)

    def paginate(self, endpoint: str, construct: bool=True, **kwargs) -> list:
        response = self.get(endpoint, construct=construct, **kwargs)
        data = response.json()
        complete = False
        pages = []
        while not complete:
            links = response.links
            if 'next' in links and links['next']['url'] not in pages:
                next_page = links['next']['url']
                pages.append(next_page)
                response = self.get(next_page, construct=False)
                result = response.json()
                if not isinstance(data, list):
                    data = [data]
                data += result

                # Rate Limiting
                cost = float(response.headers['X-Request-Cost'])
                limit = float(response.headers['X-Rate-Limit-Remaining'])
                if round(limit) == 0 or cost > limit:
                    print("\n\n\n")
                    print('Rate limit reached!')
                    print("\n\n\n")
                    sleep(cost * 100 if cost < 1 else cost)
            else:
                complete = True

        if not isinstance(data, list):
            data = [data]

        return data

    @classmethod
    def construct_request(self, endpoint: str) -> str:
        return f"{self.root}{endpoint}"

    @classmethod
    def check_response(self, response: Response) -> None:
        if response.status_code != 200:
            raise CanvasResponseError(
                f'Error Response {response.status_code} received'
                f' {response.text}'
                )

    def to_dict(self) -> dict:
        data = {}
        for key in self._keys:
            value = self.__dict__[key]
            if isinstance(value, list) and value and isinstance(value[0], Canvas):
                value = [x.to_dict() for x in value]
            data[key] = value
        return data

    def validate(self) -> None:
        if hasattr(self, '__annotations__'):
            for attr in self.__annotations__:
                assert hasattr(self, attr), (f"{attr} must be included as a keyword argument")
