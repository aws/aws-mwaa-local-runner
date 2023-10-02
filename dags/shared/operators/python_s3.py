from io import StringIO
from pandas import DataFrame, read_csv
from airflow.hooks.S3_hook import S3Hook
from shared.irondata import Irondata
from airflow.operators.python_operator import PythonOperator
from shared.s3 import S3CSVConnection
from typing import (Any,
                    Callable,
                    Mapping,
                    Optional,
                    Sequence,
                    Union,
                    )


class S3PythonOperator(S3CSVConnection, PythonOperator):

    template_fields: Sequence[str] = ('source_s3_key', 'dest_s3_key')
    ui_color: str = '#d2770b'
    ui_fgcolor: str = 'white'

    def __init__(
            self,
            task_id: str,
      
            # PythonOperator - Positional
            python_callable: Callable,
            
            # IronS3CSVConnection - Default
            aws_conn_id: str = 'aws_default',
            source_s3_key: Optional[str] = None,
            dest_s3_key: Optional[str] = None,
            bucket_name: str = Irondata.s3_warehouse_bucket(),
            replace: bool = True,
            header: bool = True,
            sep: str = ',',
            pull_header: Union[str, None, int] = 'infer',

            # Python Operator - Default
            op_kwargs: Optional[Mapping[str, Any]] = {},
            *args,
            **kwargs):

        assert any([source_s3_key, dest_s3_key]), 'A source and/or dest s3 key must be provided'
        if not isinstance(dest_s3_key, list):
            dest_s3_key = [dest_s3_key]

        PythonOperator.__init__(
            self,
            task_id=task_id,
            python_callable=python_callable,
            op_args=[],
            op_kwargs=op_kwargs,
            *args, **kwargs
            )
        
        self.source_s3_key = source_s3_key
        self.dest_s3_key = dest_s3_key

        S3CSVConnection.__init__(
            self,
            aws_conn_id=aws_conn_id,
            bucket_name=bucket_name,
            s3_key='',
            replace=replace,
            header=header,
            sep=sep,
            pull_header=pull_header,
            headers=header
        )

    # def push_to_s3(self, df: DataFrame, key:str):
    #     csv = df.to_csv(sep=self.sep, index=False, header=self.headers)
    #     print('\n\nCSV\n\n', csv[:500])
    #     self.hook.load_string(
    #         csv,
    #         key,
    #         bucket_name=self.bucket_name,
    #         replace=self.replace,
    #         encrypt=True,
    #         )

    # def pull_from_s3(self, key):
    #     encoded = self.hook.read_key(key=key, bucket_name=self.bucket_name)
    #     data = StringIO(encoded)
    #     print('\n\nPULL data', data, '\n\n')
    #     df = read_csv(data, sep=self.sep, header=self.pull_header)
    #     return df

    def collect_s3_source(self):
        print(f'\n\nDownloading from S3: {self.source_s3_key}\n\n')
        self.s3_key = self.source_s3_key
        source = self.pull_from_s3()
        print('\n\nSOURCE\n\n', source)
        self.op_args = self.op_args + (source,)
        print('\n\nOP ARGS', self.op_args, '\n\n')
        self.log.info('Calling python callable...')

    def check_callable_return(self, callable_return):
        if isinstance(callable_return, DataFrame):
            callable_return = [callable_return]
        if isinstance(self.dest_s3_key, str):
            self.dest_s3_key = [self.dest_s3_key]
        return_len = len(callable_return)
        key_len = len(self.dest_s3_key)
        if not return_len == key_len:
            raise ValueError('The return shape of the python callable '
                             ' and the shape of dest_s3_key must be the same. '
                             f'The return shape of python callable == {return_len} '
                             f'The return shape of dest_s3_key == {key_len}.')
        return callable_return

    def upload_s3_dest(self, callable_return):
        for dataset, key in zip(callable_return, self.dest_s3_key):
            self.log.info(f'Uploading to S3: {key}')
            self.s3_key = key
            self.push_to_s3(dataset)
            self.log.info('Upload successful!')

    def _execute(self, context) -> Any:
        if self.source_s3_key:
            self.collect_s3_source()
        callable_return = super().execute(context)
        if self.dest_s3_key:
            callable_return = self.check_callable_return(callable_return)
            self.upload_s3_dest(callable_return)

    def execute(self, context) -> Any:
        self._execute(context)
