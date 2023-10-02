import json
from shared import s3
from shared.irontable import Irontable
from huntr.huntr_client import HuntrClient


def huntr_to_s3(ds, tables, start, end, bucket):
    client = _get_client()

    tables = get_tables(client,
                        start=start,
                        end=end)

    for tablename, data in tables.items():

        table = Irontable(schema="huntr_api", table=tablename)
        key = f"{table.schema_in_env}/{ds}/{table.table_in_env}"
        s3.upload_as_csv(
            bucket,
            key,
            data)
        print(f"Data uploaded to {bucket}/{key}.")

    return True

def _get_client():
    return HuntrClient()

# Member-related helpers
def _get_member_fields(client):
    member_fields, _ = client._get_endpoint("https://api.huntr.co/org/member-fields")
    return { field["id"]: field["name"] for field in member_fields }

def _get_members(client):
    members = client._get_paginated_endpoint("https://api.huntr.co/org/members")
    return members

def _build_row_from_member(member, member_field_dict):
    member_fields = { i["fieldId"]: i["value"] for i in member.get("memberFieldValues") }
    return [
        member.get("id")
        , member.get("givenName")
        , member.get("familyName")
        , member.get("fullName")
        , member.get("email")
        , member.get("createdAt")
        , member.get("isActive")
        , *[ member_fields.get(id) for id in member_field_dict.keys() ]
        , member.get("lastSeenAt")
        , json.dumps(member.get("advisor"))
    ]

def _members(client):
    member_fields = _get_member_fields(client)
    members = _get_members(client)
    return [ _build_row_from_member(member, member_fields) for member in members ]

# Advisor-related helpers
def _get_advisors(client):
    advisors = client._get_paginated_endpoint("https://api.huntr.co/org/advisors")
    return advisors

def _build_row_from_advisor(advisor):
    return [
        advisor.get("id")
        , advisor.get("givenName")
        , advisor.get("familyName")
        , advisor.get("fullName")
        , advisor.get("email")
        , advisor.get("createdAt")
        , advisor.get("lastSeenAt")
    ]

def _advisors(client):
    advisors = _get_advisors(client)
    return [ _build_row_from_advisor(advisor) for advisor in advisors ]

# Job post related helpers
def _get_job_posts(client):
    job_posts = client._get_paginated_endpoint("https://api.huntr.co/org/job-posts")
    return job_posts

def _build_row_from_job_post(job_post):
    return [
        job_post.get("id")
        , job_post.get("title")
        , job_post.get("isRemote")
        , job_post.get("jobType")
        , job_post.get("url")
        , job_post.get("postDate")
        , job_post.get("createdAt")
        , json.dumps(job_post.get("location"))
        , json.dumps(job_post.get("jobPostStatus"))
        , json.dumps(job_post.get("employer"))
        , json.dumps(job_post.get("ownerMember"))
        , job_post.get("htmlDescription")
    ]

def _job_posts(client):
    job_posts = _get_job_posts(client)
    return [ _build_row_from_job_post(job_post) for job_post in job_posts ]

# Activity-related helpers
def _get_activities(client, start, end):
    activities = client._get_paginated_endpoint("https://api.huntr.co/org/activities",
                                                params={
                                                    "created_after": start
                                                    , "created_before": end
                                                })
    return activities

def _build_row_from_activity(activity):
    return [
        activity.get("id")
        , activity.get("title")
        , activity.get("note")
        , activity.get("completed")
        , activity.get("completedAt")
        , activity.get("startAt")
        , activity.get("endAt")
        , activity.get("createdAt")
        , json.dumps(activity.get("activityCategory"))
        , json.dumps(activity.get("job"))
        , json.dumps(activity.get("jobPost"))
        , json.dumps(activity.get("employer"))
        , json.dumps(activity.get("ownerMember"))
        , json.dumps(activity.get("creatorMember"))
        , activity.get("createdByWorkflow")
    ]

def _activities(client, start, end):
    activities = _get_activities(client, start, end)
    return [ _build_row_from_activity(activity) for activity in activities ]

# Action-related helpers
def _get_actions(client, start, end, action_type=None):
    activities = client._get_paginated_endpoint("https://api.huntr.co/org/actions",
                                                params={
                                                    "created_after": start
                                                    , "created_before": end
                                                    , "action_type": action_type
                                                })
    return activities

def _build_row_from_action(action):
    return [
        action.get("id")
        , action.get("actionType")
        , action.get("date")
        , json.dumps(action.get("creatorMember"))
        , json.dumps(action.get("ownerMember"))
        , json.dumps(action.get("document"))
    ]

def _actions(client, start, end, action_type=None):
    actions = _get_actions(client, start, end, action_type)
    return [ _build_row_from_action(action) for action in actions ]

def get_tables(client, **kwargs):
    start = kwargs.get('start')
    end = kwargs.get('end')

    members = _members(client)
    advisors = _advisors(client)
    job_posts = _job_posts(client)
    activities = _activities(client, start, end)
    documents_created = _actions(client, start, end, 'DOCUMENT_CREATED')
    candidates_created = _actions(client, start, end, 'CANDIDATE_CREATED')

    return {
        'members': members,
        'advisors': advisors,
        'job_posts': job_posts,
        'activities': activities,
        'documents_created': documents_created,
        'candidates_created': candidates_created
    }

