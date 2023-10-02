import os
from shared.irondata import Irondata
from calendly.http_client import ScheduledEvents


def sql_templates_path():
    return os.path.join(os.path.dirname(__file__), "sql")


def http_client():
    return ScheduledEvents(Irondata.get_config("CALENDLY_OAUTH_ACCESS_TOKEN"))
