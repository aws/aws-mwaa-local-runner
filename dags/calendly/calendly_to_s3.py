from shared import s3
from shared.irontable import Irontable
from calendly.helpers import http_client


def to_s3(ds, bucket, start, end, **kwargs):

    # Filter for collection window
    print(start)
    print(end)

    client = http_client()
    tables = client.get_tables(min_start_time=start,
                               max_start_time=end)

    for table_name, data in tables.items():

        entity = Irontable(schema=kwargs.get('schema'),
                           table=table_name)

        s3.upload_as_csv(
            bucket,
            f"{entity.schema_in_env}/{ds}/{entity.table_in_env}",
            data)
        print("submissions found. Written to S3.")

    return True
