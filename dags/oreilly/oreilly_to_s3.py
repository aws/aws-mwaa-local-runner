from datetime import datetime, timedelta
from shared import s3
from oreilly.helpers import http_client
import hashlib


def oreilly_extract(ds, bucket, user_activity_table, usage_timeline_table, **kwargs):
    client = _get_oreilly_client()

    day = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y-%m-%d")

    user_activities = _fetch_activities(client)
    usage_timelines = _fetch_usage_timelines(client, day)

    if user_activities and usage_timelines:
        activity_csv = _process_results(user_activities, _build_row_from_activity)
        s3.upload_as_csv(
            bucket,
            f"{user_activity_table.schema_in_env}/{ds}/{user_activity_table.table_in_env}",
            activity_csv)

        usage_timeline_csv = _process_results(usage_timelines, _build_row_from_usage_timeline)
        s3.upload_as_csv(
            bucket,
            f"{usage_timeline_table.schema_in_env}/{ds}/{usage_timeline_table.table_in_env}",
            usage_timeline_csv)

        print("User activities and usage timelines found. Written to S3.")
        return True
    else:
        print("Found no user activities or usage timelines via O'Reilly API")
        return False


def _get_oreilly_client():
    return http_client()


def _fetch_activities(client):
    return client.get_user_activities()


def _fetch_usage_timelines(client, day):
    return client.get_usage_timelines(day)


def _process_results(results, f):
    rows = [f(result) for result in results]

    return rows


def _build_row_from_activity(activity):
    return [
        activity.get("identifier"),
        activity.get("email"),
        activity.get("first_name"),
        activity.get("last_name"),
        activity.get("content_last_accessed"),
        activity.get("activation_date"),
        activity.get("deactivation_date"),
        activity.get("active")
    ]


def _build_row_from_usage_timeline(usage_timeline):
    id = usage_timeline.get("max_client_timestamp") + usage_timeline.get("account_identifier") + \
        usage_timeline.get("user_identifier") + usage_timeline.get("title_identifier")

    return [
        hashlib.md5(id.encode("utf-8")).hexdigest(),
        usage_timeline.get("date"),
        usage_timeline.get("account_identifier"),
        usage_timeline.get("user_identifier"),
        usage_timeline.get("email"),
        usage_timeline.get("title_identifier"),
        usage_timeline.get("title"),
        usage_timeline.get("content_format"),
        usage_timeline.get("virtual_pages"),
        usage_timeline.get("duration_seconds"),
        usage_timeline.get("topic"),
        usage_timeline.get("device_type"),
        usage_timeline.get("platform"),
        usage_timeline.get("units_viewed"),
        usage_timeline.get("min_client_timestamp"),
        usage_timeline.get("max_client_timestamp")
    ]
