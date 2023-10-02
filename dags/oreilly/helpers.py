from shared.irondata import Irondata
from oreilly.http_client import HttpClient

def http_client():
    return HttpClient(
        {
            "account_id": Irondata.get_config("OREILLY_ACCOUNT_ID"),
            "auth_token": Irondata.get_config("OREILLY_AUTH_TOKEN")
        }
    )
