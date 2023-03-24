from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from datetime import datetime

class StoreDsMacroOperator(BaseOperator):
    """
    Custom operator to store the 'ds' macro to an Airflow variable.
    """

    @apply_defaults
    def __init__(self, variable_name, *args, **kwargs):
        """
        Initialize the operator.

        :param variable_name: The name of the Airflow variable to store the 'ds' macro.
        :type variable_name: str
        """
        super().__init__(*args, **kwargs)
        self.variable_name = variable_name

    def execute(self, context):
        """
        Store the 'ds' macro to the Airflow variable.

        :param context: The context dictionary for the task.
        :type context: dict
        """
        ds = context.get('ds')
        Variable.set(self.variable_name, ds)
        self.log.info(f"Stored ds macro {ds} to variable {self.variable_name}")
