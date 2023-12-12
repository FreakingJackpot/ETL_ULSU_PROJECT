from datetime import datetime, timedelta

from airflow import DAG
from airflow_django.operators import DjangoOperator


dag = DAG(
    'amazing_dag',
    description='This is an amazing Dag',
    schedule_interval=timedelta(days=1),
default_args={'start_date': datetime.now()},
)

def run(ds, **kwargs):
    from apps.etl.management.commands.import_gogov_data import Command
    Command().handle()

run_this = DjangoOperator(
    task_id='create_an_amazing_instance',
    provide_context=True,
    python_callable=run,
    dag=dag,
)
