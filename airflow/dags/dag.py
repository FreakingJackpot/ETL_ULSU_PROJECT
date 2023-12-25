from datetime import datetime
from functools import partial

from airflow.decorators import dag
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import ShortCircuitOperator
from airflow_django.operators import DjangoOperator

LOAD_DATA_TI_TEMPLATE = "{{{{ti.xcom_pull(key='data', task_ids='{task_id}')}}}}"


def load_global_data(data):
    from apps.etl.utils.mappers.transformed_data_mappers import GlobalTransformedDataMapper
    GlobalTransformedDataMapper().map(data)


def load_region_data(data):
    from apps.etl.utils.mappers.transformed_data_mappers import RegionTransformedDataMapper
    RegionTransformedDataMapper().map(data)


load_global_data_task_template = partial(DjangoOperator, task_id='load_global_data',
                                         python_callable=load_global_data)

load_region_data_task_template = partial(DjangoOperator, task_id='load_region_data',
                                         python_callable=load_region_data)


@dag(dag_id='etl_legacy_data', start_date=datetime(2023, 1, 1),
     description='ETL process for legacy data', schedule_interval="@once", render_template_as_native_obj=True, )
def etl_legacy_data():
    def csv_import():
        from django.core.management import call_command
        call_command('import_from_csv')

    def transform_legacy_global_data(ti):
        # Не запускаю с помощью call_command, тк надо будет преобразовывать data в str в handle
        from apps.etl.management.commands.transform_legacy_global_data import Command
        data = Command().handle(debug=False)
        ti.xcom_push(key='data', value=data)

    def transform_legacy_region_data(ti):
        from apps.etl.management.commands.transform_legacy_region_data import Command
        data = Command().handle(debug=False)
        ti.xcom_push(key='data', value=data)

    csv_import_task = DjangoOperator(task_id='csv_import', python_callable=csv_import)

    transform_legacy_global_data_task = DjangoOperator(
        task_id='transform_legacy_global_data',
        python_callable=transform_legacy_global_data,
    )

    transform_legacy_region_data_task = DjangoOperator(
        task_id='transform_legacy_region_data',
        python_callable=transform_legacy_region_data,
    )

    load_global_data_task = load_global_data_task_template(
        op_kwargs={"data": LOAD_DATA_TI_TEMPLATE.format(task_id='transform_legacy_global_data')})
    load_region_data_task = load_region_data_task_template(
        op_kwargs={"data": LOAD_DATA_TI_TEMPLATE.format(task_id='transform_legacy_region_data')})

    chain(csv_import_task, [transform_legacy_global_data_task, transform_legacy_region_data_task],
          [load_global_data_task, load_region_data_task])


def transform_global_data(ti, latest):
    from apps.etl.management.commands.transform_global_data import Command
    data = Command().handle(debug=False, latest=latest)
    ti.xcom_push(key='data', value=data)


def transform_region_data(ti, latest):
    from apps.etl.management.commands.transform_region_data import Command
    data = Command().handle(debug=False, latest=latest)
    ti.xcom_push(key='data', value=data)


transform_global_data_task_template = partial(DjangoOperator, task_id='transform_global_data',
                                              python_callable=transform_global_data, )

transform_region_data_task_template = partial(DjangoOperator, task_id='transform_region_data',
                                              python_callable=transform_region_data, )


def etl_data_dag_template(latest):
    transformers_kwargs = {"ti": "{{ti}}", "latest": False}
    transform_global_data_task = transform_global_data_task_template(op_kwargs=transformers_kwargs)

    transform_region_data_task = transform_region_data_task_template(op_kwargs=transformers_kwargs)

    load_global_data_task = load_global_data_task_template(
        op_kwargs={"data": LOAD_DATA_TI_TEMPLATE.format(task_id='transform_global_data')}
    )
    load_region_data_task = load_region_data_task_template(
        op_kwargs={"data": LOAD_DATA_TI_TEMPLATE.format(task_id='transform_region_data')}
    )

    chain([transform_global_data_task, transform_region_data_task], [load_global_data_task, load_region_data_task])


@dag(dag_id='etl_data_full', start_date=datetime(2023, 1, 1), description='ETL process for all non legacy data',
     schedule_interval="@monthly", render_template_as_native_obj=True, )
def etl_data_full():
    etl_data_dag_template(False)


@dag(dag_id='etl_data_latest', start_date=datetime(2023, 1, 1), description='ETL process for latest non legacy data',
     schedule_interval="0 20 * * 7", render_template_as_native_obj=True, )
def etl_data_latest():
    etl_data_dag_template(True)


@dag(dag_id='import_external_data', start_date=datetime(2023, 1, 1), description='ETL process for external sources data',
     schedule_interval="00 18 * * *", render_template_as_native_obj=True, )
def import_external_data():
    def gogov_import():
        from django.core.management import call_command
        call_command('import_gogov_data')

    def stopcorona_import(ti):
        from django.core.management import call_command
        value = call_command('import_stopcorona_data')
        ti.xcom_push(key='is_updated', value=value)

    gogov_import_task = DjangoOperator(
        task_id='gogov_import',
        python_callable=gogov_import,
    )

    stopcorona_import_task = DjangoOperator(
        task_id='stopcorona_import',
        python_callable=stopcorona_import,
    )

    check_for_stopcorona_update_task = ShortCircuitOperator(
        task_id='check_for_stopcorona_update',
        python_callable=lambda x: x,
        op_args=["{{ti.xcom_pull(key='is_updated', task_ids='stopcorona_import')}}"],
    )

    trigger_dag_run_task = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id='etl_data_latest'
    )
    chain([gogov_import_task, stopcorona_import_task], check_for_stopcorona_update_task, trigger_dag_run_task)


etl_legacy_data()
etl_data_full()
etl_data_latest()
import_external_data()
