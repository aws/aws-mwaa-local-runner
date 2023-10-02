import requests
import numpy as np
from time import sleep
import hashlib


class Calendly:

    organization = 'FAHHTKYSRTNRGU7P'

    def __init__(self, token):
        self.headers = {
            "Authorization": 'Bearer ' + token,
            "Content-Type": "application/json"
        }
        self.count = 0

    def _send_request(self, request, params={}):
        r = requests.get(request, headers=self.headers, params=params).json()
        return r

    def paginate(self, response_json, data_collection, function):
        self.count += 1
        print('Pagination count:', self.count)

        if response_json["pagination"]["next_page"]:
            next_page = function(request=response_json["pagination"]["next_page"])
            data_collection += next_page

        return data_collection


class ScheduledEvents(Calendly):

    def request_scheduled_events(self, request=None, **kwargs):
        if not request:
            request = ("https://api.calendly.com"
                       "/scheduled_events"
                       "?organization=https://api.calendly.com/organizations"
                       f"/{Calendly.organization}")

        response_json = self._send_request(request, params=kwargs)
        print(response_json)
        data_collection = response_json['collection']

        # Fields that require additional api calls
        self.add_invitee_data(data_collection)
        self.add_host_data(data_collection)

        # Paginate for all events
        data = self.paginate(response_json, data_collection, self.request_scheduled_events)
        return data

    def add_invitee_data(self, data):

        for idx, row in enumerate(data):
            uri = row['uri'] + '/invitees'
            invitee_data = self._send_request(uri)
            invitee_data.get('collection', [{}])[0].pop('uri', None)  # Drop uri variable
            data[idx]['invitees'] = [invitee_data.get('collection', {})]  # Add invitee data to overall dataset
            if idx % 100 == 0:  # Rate limiting
                sleep(1)

    def add_host_data(self, data):

        for idx in range(len(data)):
            uri = data[idx].get('event_memberships', [{}])[0].get('user')
            if uri:
                response_json = self._send_request(uri).get('resource', {})
                data[idx]['host_name'] = response_json.get('name')
                data[idx]['host_email'] = response_json.get('email')
                data[idx]['host_timezone'] = response_json.get('timezone')
                if idx % 100 == 0:  # Rate limiting
                    sleep(1)

    def _events_row(self, data):

        invitees = data.get('invitees', [[{}]])[0][0]
        email = invitees.get('email', '').strip().lower()
        invitee_id = hashlib.md5(email.encode('utf-8')).hexdigest()

        return np.array(
            [
                data.get('uri', '').split('/')[-1],  # event uuid
                data.get('event_type', '').split('/')[-1],  # event_type uuid
                data.get('created_at'),
                data.get('start_time'),
                data.get('end_time'),
                data.get('invitees_counter', {}).get('total'),
                data.get('invitees_counter', {}).get('active'),
                data.get('host_name'),
                data.get('host_email'),
                data.get('host_timezone'),
                data.get('location', {}).get('join_url'),
                data.get('name'),
                data.get('status'),
                invitee_id,
            ]
        )

    def events(self, data):
        return [self._events_row(row) for row in data]

    def _invitees_row(self, data):
        """
        This function constructs a row for the `invitees` table.

        At the moment, this function only collects data for the first
        invitee for a given event. If additional invitees exist their data
        will not be collected. As of 12/17/21 only one event has more
        than a single invitee.
        """
        invitees = data.get('invitees', [[{}]])[0][0]
        email = invitees.get('email', '').strip().lower()
        id_ = hashlib.md5(email.encode('utf-8')).hexdigest()

        return np.array(
            [
                id_,
                email,
                invitees.get('name'),
                invitees.get('first_name'),
                invitees.get('last_name'),
                invitees.get('timezone'),
                invitees.get('tracking', {}).get('salesforce_uuid')
            ]
        )

    def invitees(self, data):
        return [self._invitees_row(row) for row in data]

    def get_questions_and_answers(self, data):
        """
        There is not known limit for how many questions ann event can have.
        This function loops over all invitee dictionaries and returns a list
        of questions and answers for every event. The event id and invitee id
        are added to each row of data to capture an invitees answer to a question
        for a specific meeting.
        """
        questions_and_answers = []
        for event in data:
            invitees = event.get('invitees', [[{}]])[0][0]
            q_and_a = invitees.get('questions_and_answers', [])
            if q_and_a:  # Many events have no questions
                for entry in q_and_a:
                    row = [
                        entry.get('question'),
                        entry.get('answer'),
                        int(entry.get('position')),
                        self._events_row(event)[0],
                        self._invitees_row(event)[0],
                    ]
                    questions_and_answers.append(row)

        return questions_and_answers

    def get_tables(self, **kwargs):

        data = self.request_scheduled_events(**kwargs)
        events = self.events(data)
        invitees = self.invitees(data)
        q_and_a = self.get_questions_and_answers(data)

        return {
            'events': events,
            'invitees': invitees,
            'questions_and_answers': q_and_a
        }

    def __getitem__(self, key):
        return self.tables[key]
