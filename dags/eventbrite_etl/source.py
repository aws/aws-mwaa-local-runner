from shared.irondata import Irondata
from shared import s3

from datetime import datetime, timedelta
import pytz
import re

from eventbrite import Eventbrite


class EventbriteSource:
    def __init__(self, oauth_token):
        self.oauth_token = oauth_token

    def extract_completed_events_and_attendees(self, ds, **kwargs):
        self.eventbrite = Eventbrite(self.oauth_token)

        ds_obj = datetime.strptime(ds, "%Y-%m-%d")
        one_day = timedelta(days=1)
        one_week = timedelta(weeks=1)
        window_start = datetime.combine(ds_obj, datetime.min.time()).replace(tzinfo=pytz.UTC) - one_week
        window_end = datetime.combine(ds_obj, datetime.max.time()).replace(tzinfo=pytz.UTC) + one_day

        events = self.events_completed_between(window_start, window_end)
        attendees = []
        for event in events:
            attendees.extend(self.event_attendees(event["id"]))

        event_rows = self.event_rows(events)
        attendee_rows = self.attendee_rows(attendees)

        s3.upload_as_csv(
            Irondata.s3_warehouse_bucket(),
            f"eventbrite/{ds}/events",
            event_rows)

        s3.upload_as_csv(
            Irondata.s3_warehouse_bucket(),
            f"eventbrite/{ds}/attendees",
            attendee_rows)

    def get_organization_id(self):
        org_res = self.eventbrite.get('/users/me/organizations')

        fis_org = next((item for item in org_res['organizations'] if item["name"] == "Flatiron School Events"), None)

        if fis_org:
            self.organization_id = fis_org['id']
        else:
            raise BaseException(f"Unknown organizations: {org_res['organizations']}")

        return self.organization_id

    def events_completed_between(self, window_start, window_end):
        res = None
        events = []
        page = 1
        while res is None or res['pagination']['has_more_items']:
            res = self.eventbrite.get(
                '/organizations/{}/events'.format(self.get_organization_id()),
                data={
                    'page': page,
                    'page_size': '500',
                    'order_by': 'start_desc',
                    'start_date.range_start': window_start.strftime("%Y-%m-%d"),
                    'start_date.range_end': window_end.strftime("%Y-%m-%d"),
                    'status': 'completed',
                    'expand': 'venue'
                })

            events.extend(res['events'])

            page += 1

        return events

    def event_attendees(self, event_id):
        res = None
        attendees = []
        page = 1
        while res is None or res['pagination']['has_more_items']:
            res = self.eventbrite.get(
                '/events/{}/attendees'.format(event_id),
                data={'page': page, 'expand': ['profile', 'attendee-affiliate']})

            attendees_page = res['attendees']
            attendees.extend(attendees_page)

            page += 1

        return attendees

    def extract_updated_attendees(self, ds, **kwargs):
        # This endpoint only has a "changed_since" parameter with an upper bound of the current time.
        # Running this on a date far in the past would scoop a ton of data so we're just going
        # to run this on the scheduled jobs looking 48 hours back.
        if not kwargs['test_mode'] and not re.search('scheduled', kwargs["run_id"]):
            return

        self.eventbrite = Eventbrite(self.oauth_token)

        ds_obj = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        since_time = datetime.combine(ds_obj, datetime.min.time()).replace(tzinfo=pytz.UTC)
        attendees = self.attendees_changed_since(since_time)
        attendee_rows = self.attendee_rows(attendees)

        s3.upload_as_csv(
            Irondata.s3_warehouse_bucket(),
            f"eventbrite/{ds}/updated_attendees",
            attendee_rows)

    def attendees_changed_since(self, since_time):
        res = None
        attendees = []
        page = 1

        while res is None or res['pagination']['has_more_items']:
            res = self.eventbrite.get(
                '/organizations/{}/attendees'.format(self.get_organization_id()),
                data={
                    'page': page,
                    'expand': ['profile', 'attendee-affiliate'],
                    'changed_since': since_time.strftime("%Y-%m-%dT%H:%M:%SZ")})

            attendees_page = res['attendees']
            attendees.extend(attendees_page)

            page += 1

        return attendees

    def attendee_rows(self, attendees):
        return list(map(lambda a: self.attendee_row(a), attendees))

    def attendee_row(self, a):
        return [
            a['id'],
            a['created'],
            a['changed'],
            a['ticket_class_id'],
            a['variant_id'],
            a['ticket_class_name'],
            a['checked_in'],
            a['cancelled'],
            a['refunded'],
            a['status'],
            a['event_id'],
            a['order_id'],
            a['guestlist_id'],
            a['invited_by'],
            a['delivery_method'],
            a['profile']['name'] if 'name' in a['profile'] else None,
            a['profile']['email'] if 'email' in a['profile'] else None,
            a['profile']['first_name']
            if 'first_name' in a['profile'] else None,
            a['profile']['last_name']
            if 'last_name' in a['profile'] else None,
            a['profile']['prefix'] if 'prefix' in a['profile'] else None,
            a['profile']['suffix'] if 'suffix' in a['profile'] else None,
            a['profile']['age'] if 'age' in a['profile'] else None,
            a['profile']['job_title'] if 'job_title' in a['profile'] else None,
            a['profile']['company'] if 'company' in a['profile'] else None,
            a['profile']['website'] if 'website' in a['profile'] else None,
            a['profile']['blog'] if 'blog' in a['profile'] else None,
            a['profile']['gender'] if 'gender' in a['profile'] else None,
            a['profile']['birth_date']
            if 'birth_date' in a['profile'] else None,
            a['profile']['cell_phone']
            if 'cell_phone' in a['profile'] else None,
            a['affiliate']
        ]

    def event_rows(self, events):
        return list(map(lambda e: self.event_row(e), events))

    def event_row(self, e):
        if e.get('venue', {}) is None:
            e.pop('venue')

        return [
            e.get('id', None),
            e.get('name', {}).get('text', None),
            e.get('summary', None),
            e.get('description', {}).get('text', None),
            e.get('url', None),
            e.get('start', {}).get('utc', None),
            e.get('end', {}).get('utc', None),
            e.get('created', None),
            e.get('changed', None),
            e.get('published', None),
            e.get('status', None),
            e.get('currency', None),
            e.get('online_event', None),
            e.get('venue', {}).get('address', {}).get('address_1', None),
            e.get('venue', {}).get('address', {}).get('address_2', None),
            e.get('venue', {}).get('address', {}).get('city', None),
            e.get('venue', {}).get('address', {}).get('region', None),
            e.get('venue', {}).get('address', {}).get('postal_code', None),
            e.get('venue', {}).get('address', {}).get('country', None),
            e.get('venue', {}).get('id', None),
            e.get('venue', {}).get('capacity', None),
            e.get('venue', {}).get('name', None),
            e.get('venue', {}).get('latitude', None),
            e.get('venue', {}).get('longitude', None)
        ]
