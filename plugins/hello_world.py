import logging
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class HelloWorldOperator(BaseOperator):
    @apply_defaults
    def __init__(self, my_operator_param, *args, **kwargs):
        self.operator_param = my_operator_param
        super().__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Hello World!")
        log.info(f"operator_param: {self.operator_param}")

class HelloWorldPlugin(AirflowPlugin):
    name = "Hello_World_Plugin"
    operators = [HelloWorldOperator]
