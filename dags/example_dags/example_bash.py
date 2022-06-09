# from airflow.models import DAG
from utils.dag_factory import create_dag, add_bash_task

dag = create_dag(name="example_bash", schedule="@daily")

bash_1 = add_bash_task(name="print_date", command="date", dag=dag)
bash_2 = add_bash_task(name="echo", command="echo woot", dag=dag)

bash_1 >> bash_2
