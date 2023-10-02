import requests
from shared.irondata import Irondata

class HuntrClient():
    BASE_URL="https://api.huntr.co/org/"

    def __init__(self, auth_token=None):
        if not auth_token:
            auth_token = Irondata.get_config('HUNTR_AUTH_TOKEN')
            assert auth_token, "A token must be provided in the init or a HUNTR_AUTH_TOKEN environment variable must be set"
        self.headers = {"Authorization": f"Bearer {auth_token}"}

    def _get_endpoint(self, url, params=None):
        response = requests.get(url, headers=self.headers, params=params).json()

        if "data" in response:
            return response.get("data"), response.get("next")
        elif "error" in response:
            raise BaseException(f"""
                Huntr responded with an error:
                {response["error"]}
            """)
        else:
            return response, None

    def _get_paginated_endpoint(self, url, params={}):
        all_items = []

        data, next = self._get_endpoint(url, params=params)
        all_items += data

        while next:
            data, next = self._get_endpoint(url, params={"next": next, **params})
            if data:
                all_items += data
            else:
                break

        return all_items
