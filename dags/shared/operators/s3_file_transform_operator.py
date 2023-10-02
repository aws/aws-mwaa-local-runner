from __future__ import annotations

import subprocess
import sys
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context

class S3FileTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as a first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    S3 Select is also available to filter the source contents. Users can
    omit the transformation script if S3 Select expression is specified.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3FileTransformOperator`

    :param source_s3_key: The key to be retrieved from S3. (templated)
    :param dest_s3_key: The key to be written from S3. (templated)
    :param transform_script: location of the executable transformation script
    :param select_expression: S3 Select expression
    :param script_args: arguments for transformation script (templated)
    :param source_aws_conn_id: source s3 connection
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :param dest_aws_conn_id: destination s3 connection
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        See: ``source_verify``
    :param replace: Replace dest S3 key if it already exists
    """

    template_fields: Sequence[str] = ("source_s3_key", "dest_s3_key", "script_args")

    template_ext: Sequence[str] = ()

    ui_color = "#f9c915"


    def __init__(
        self,
        *,
        source_s3_key: str,
        dest_s3_key: str,
        transform_script: str | None = None,
        select_expression=None,
        script_args: Sequence[str] | None = None,
        source_aws_conn_id: str = "aws_default",
        source_verify: bool | str | None = None,
        dest_aws_conn_id: str = "aws_default",
        dest_verify: bool | str | None = None,
        replace: bool = False,
        input_serialization: dict | None,
        output_serialization: dict | None,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.script_args = script_args or []
        self.output_encoding = sys.getdefaultencoding()
        self.input_serialization = input_serialization
        self.output_serialization = output_serialization

    def execute(self, context: Context):
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException("Either transform_script or select_expression must be specified")

        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id, verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id, verify=self.dest_verify)

        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(f"The source key {self.source_s3_key} does not exist")
        source_s3_key_object = source_s3.get_key(self.source_s3_key)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info("Dumping S3 file %s contents to local file %s", self.source_s3_key, f_source.name)

            if self.select_expression is not None:
                content = source_s3.select_key(
                    key=self.source_s3_key,
                    expression=self.select_expression, 
                    input_serialization=self.input_serialization,
                    output_serialization=self.output_serialization)
                f_source.write(content.encode("utf-8"))
            else:
                source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            if self.transform_script is not None:
                with subprocess.Popen(
                    [self.transform_script, f_source.name, f_dest.name, *self.script_args],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True,
                ) as process:
                    self.log.info("Output:")
                    if process.stdout is not None:
                        for line in iter(process.stdout.readline, b""):
                            self.log.info(line.decode(self.output_encoding).rstrip())

                    process.wait()

                    if process.returncode:
                        raise AirflowException(f"Transform script failed: {process.returncode}")
                    else:
                        self.log.info(
                            "Transform script successful. Output temporarily located at %s", f_dest.name
                        )

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name if self.transform_script else f_source.name,
                key=self.dest_s3_key,
                replace=self.replace,
            )
            self.log.info("Upload successful")
