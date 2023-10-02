# NOTE: Helper for PythonVirtualenvOperator to handle dependencies properly
def run(ds, bucket_name, prefix):
    from canvas_rollcall_ingester.report_extractor import build_extractor_instance
    extractor = build_extractor_instance(ds, bucket_name, prefix)
    extractor.run()


def build_extractor_instance(ds, bucket_name, prefix):
    import csv
    from email import message_from_bytes
    import logging
    import quopri
    import re
    from urllib.parse import urlparse
    from bs4 import BeautifulSoup
    import requests
    from shared.irondata import Irondata
    from shared.s3 import copy_object, delete_object, get_object, list_objects, load_bytes, upload_as_csv
    from canvas_rollcall_ingester.dag import ATTENDANCE_COLS

    class ReportExtractor:
        def __init__(self, ds, bucket_name, prefix):
            self.ds = ds
            self.bucket_name = bucket_name
            self.prefix = prefix

        def run(self):
            for s3_obj in list_objects(self.bucket_name, prefix=self.prefix):
                email = self._get_email(s3_obj)
                self._process_email(s3_obj, email)

        def _get_email(self, s3_obj):
            raw_email = get_object(Irondata.s3_warehouse_bucket(), s3_obj['Key'])
            return message_from_bytes(raw_email['Body'].read())

        def _process_email(self, s3_obj, email):
            email_html = self._get_email_html(email)
            url_obj = self._get_report_download_url(s3_obj, email_html)

            if url_obj is None:
                return True

            ds = self._get_ds(email)
            response = requests.get(url_obj.geturl())

            if response.status_code == requests.codes.ok:
                csv_bytes = response.content

                self._upload_raw_report(ds, url_obj, csv_bytes)
                self._upload_clean_report(ds, csv_bytes)
            else:
                error_message = None
                if response.headers.get('content-type') == 'application/xml':
                    xml = BeautifulSoup(response.text, features='lxml')
                    error_tag = xml.find('error')
                    error_message_tag = error_tag and error_tag.find('message')
                    error_message = error_message_tag and error_message_tag.text

                if error_message and re.search('request has expired', error_message, re.IGNORECASE):
                    logging.warn(f'Download link expired in email: {s3_obj["Key"]}.')
                    logging.warn(response.text)
                else:
                    logging.error(f'HTTP RESPONSE {response.status_code}')
                    logging.error(response.text)
                    raise BaseException('unknown failure')

            self._archive_email(ds, s3_obj)

        def _upload_clean_report(self, ds, csv_bytes):
            reader = csv.reader(csv_bytes.decode('utf-8').strip().split('\n'))
            csv_list = [row for row in reader]
            num_cols = len(ATTENDANCE_COLS)
            truncated_csv = [row[0:num_cols] for row in csv_list]
            upload_as_csv(
                Irondata.s3_warehouse_bucket(),
                f'canvas_rollcall/reports/{ds}/attendance.csv',
                truncated_csv)

        def _archive_email(self, ds, s3_obj):
            email_digest = s3_obj['Key'].split('/')[-1]

            copy_object(
                self.bucket_name,
                s3_obj['Key'],
                f'canvas_rollcall/emails/processed/{ds}/{email_digest}')

            delete_object(
                self.bucket_name,
                s3_obj['Key'])

        def _upload_raw_report(self, ds, url_obj, csv_bytes):
            report_filename = url_obj.path.split('/')[-1]
            load_bytes(
                csv_bytes,
                f'canvas_rollcall/reports/{ds}/{report_filename}',
                Irondata.s3_warehouse_bucket())

        def _get_ds(self, email):
            recipient = email.get_all('to', [])[0]
            # return re.match('^.*\+(\d{4}\-\d{1,2}\-\d{1,2})[A-z0-9\-&]*@.*$', recipient)[1]  # noqa: W605
            return re.search('etl-rollcall\+(\d{4}\-\d{1,2}\-\d{1,2})[A-z0-9\-&\.]*@.*', recipient)[1]  # noqa: W605

        def _get_email_html(self, email):
            quoted_printable_payload = email.get_payload()
            msg = quopri.decodestring(quoted_printable_payload).decode('utf-8')
            return BeautifulSoup(msg, features='lxml')

        def _get_report_download_url(self, s3_obj, email_html):
            url_obj = None

            a_link = email_html.find('a')
            if a_link:
                url_obj = urlparse(a_link.get('href').strip())

                if not re.match('^.*\.csv$', url_obj.path):  # noqa: W605
                    raise BaseException(f"Unknown link found in email {s3_obj['Key']}: {url_obj.netloc}{url_obj.path}")
            elif re.search('AMAZON_SES_SETUP_NOTIFICATION', s3_obj['Key']):
                pass
            else:
                raise BaseException(f"No link found in email {s3_obj['Key']}")

            return url_obj

    return ReportExtractor(ds, bucket_name, prefix)
