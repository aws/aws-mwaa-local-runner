import requests


class HttpClient:

    def __init__(self, credentials):
        self.account_id = credentials["account_id"]
        self.headers = {"Authorization": "Token {auth_token}".format(auth_token=credentials["auth_token"])}

    def get_user_activities(self):
        BASE_URL = "https://learning.oreilly.com/api/v3/insights/accounts/{account_id}/user-activity/"
        url = BASE_URL.format(account_id=self.account_id)

        return self._get_paginated_endpoint(url)

    def get_usage_timelines(self, day):
        BASE_URL = "https://learning.oreilly.com/api/v3/insights/accounts/{account_id}/usage-timeline/{day}/"

        url = BASE_URL.format(account_id=self.account_id, day=day)

        return self._get_paginated_endpoint(url)

    def _get_paginated_endpoint(self, url):
        all_items = []

        results, next_url = self._get_endpoint(url)
        all_items.extend(results)

        while next_url:
            results, next_url = self._get_endpoint(next_url)
            if results:
                all_items.extend(results)
            else:
                break

        return all_items

    def _get_endpoint(self, url):
        response = requests.get(url, headers=self.headers).json()

        if "results" in response:
            return response["results"], response["next"]
        elif "error" in response:
            raise BaseException(f"""
                O'Reilly responded with an error:
                {response["error"]}
            """)
        else:
            print(f"Response: {response}")
            raise BaseException("""
                Something went wrong with call to O'Reilly API.
                Possibly rate-limit max hit or Auth token may have become invalid.
            """)
