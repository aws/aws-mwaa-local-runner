# NOTE: Helper for PythonVirtualenvOperator to handle dependencies properly
def run(ds):
    from canvas_rollcall_scheduler.report_scheduler import build_scheduler_instance
    scheduler = build_scheduler_instance(ds)
    scheduler.run(ds)


def build_scheduler_instance(ds):
    from datetime import datetime, timedelta
    import logging
    from shared.irondata import Irondata
    from bs4 import BeautifulSoup
    import requests

    class ReportScheduler:
        def __init__(self):
            self.account_id = 1

            if Irondata.is_production():
                self.external_tool_id = 1
            else:
                self.external_tool_id = 4

            self.report_interval_days = 6

            self.learn_portal_url = Irondata.get_config('LEARN_PORTAL_URL')
            self.learn_username = Irondata.get_config('LEARN_USERNAME')
            self.learn_password = Irondata.get_config('LEARN_PASSWORD')

            self.canvas_base_url = Irondata.get_config('CANVAS_BASE_URL')
            self.canvas_oauth_url = Irondata.get_config('CANVAS_OAUTH_URL')
            self.report_inbox = Irondata.get_config('CANVAS_ROLLCALL_REPORT_INBOX')

        def run(self, ds):
            session = self._oauth_authenticate()
            self._schedule_report(session, ds)

        def _oauth_authenticate(self):
            session = requests.Session()

            session.get(self.canvas_oauth_url, headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'  # noqa: E501
            })

            login_res = session.get(f'{self.learn_portal_url}/sign_in', headers={
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'  # noqa: E501
            })

            login_soup = BeautifulSoup(login_res.text, features="lxml")
            login_inputs = login_soup.find(action="/sign_in").find_all("input")

            form_data = {}
            for li in login_inputs:
                form_data[li.get('name')] = li.get('value')

            form_data['user[email]'] = self.learn_username
            form_data['user[password]'] = self.learn_password

            session.post(f'{self.learn_portal_url}/sign_in', data=form_data, headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",  # noqa: E501
                "content-type": "application/x-www-form-urlencoded"
            })

            return session

        def _schedule_report(self, session, ds):
            rollcall_launch_res = self._launch_rollcall_lti(session)
            report_form_data = self._build_report_params(rollcall_launch_res, ds)

            logging.info(f'_schedule_report form data: {report_form_data}')

            response = session.post('https://rollcall.instructure.com/reports', data=report_form_data)

            if response.status_code == requests.codes.ok:
                logging.info(response.text)
            else:
                logging.error(response.text)
                raise BaseException(f'_schedule_report: HTTP STATUS {response.status_code}')

        def _build_report_params(self, rollcall_launch_res, ds):
            rollcall_soup = BeautifulSoup(rollcall_launch_res.text, features="lxml")
            report_inputs = rollcall_soup.find(id="new_report").find_all("input")

            report_form_data = {}
            for ri in report_inputs:
                report_form_data[ri.get('name')] = ri.get('value')

            report_interval = timedelta(days=self.report_interval_days)
            start_date = (datetime.strptime(ds, "%Y-%m-%d") - report_interval).strftime("%m-%d-%Y")
            end_date = datetime.strptime(ds, "%Y-%m-%d").strftime("%m-%d-%Y")

            report_form_data['report[start_date]'] = start_date
            report_form_data['report[end_date]'] = end_date
            report_form_data['report[email]'] = self._plus_datestamp_email(self.report_inbox)

            return report_form_data

        def _plus_datestamp_email(self, email):
            run_date = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            return email.replace('@', f"+{run_date}@")

        def _launch_rollcall_lti(self, session):
            tool_res = session.get(
                f"{self.canvas_base_url}/accounts/{self.account_id}/external_tools/{self.external_tool_id}")

            tool_soup = BeautifulSoup(tool_res.text, features="lxml")
            tool_inputs = tool_soup.find(id="tool_form").find_all("input")

            tool_form_data = {}
            for ti in tool_inputs:
                tool_form_data[ti.get('name')] = ti.get('value')

            rollcall_res = session.post('https://rollcall.instructure.com/launch', data=tool_form_data)

            return rollcall_res

    return ReportScheduler()
