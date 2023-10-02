from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.models import SkipMixin


class VirtualenvShortCircuitOperator(PythonVirtualenvOperator, SkipMixin):

    def execute(self, context):
        condition = super().execute(context)
        self.log.info("Condition result is %s", condition)

        if condition:
            self.log.info('Proceeding with downstream tasks...')
            return

        self.log.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")
