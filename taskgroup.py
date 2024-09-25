from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task, task_group
from airflow.models.xcom_arg import XComArg

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

def _process_user(ti):
	print("Printing Process User")
	
def _process_director(ti):
	print("Printing Process Director")
	
@task_group()
def parent_group(report_dates) -> list[TaskGroup]:
    dates = []
    for value in report_dates['dates']:
            dates.append(value)
    return list(map(etl_tasks, dates))
		
	
def etl_tasks(report_date: str) -> list[TaskGroup]:

	envVars = {'TEST_USER':report_date,'TEST_PASSWORD':test_connection.password}

	with TaskGroup(f"trading_performance_etl_{report_date}", tooltip=f"Trading Performance ETL {report_date}") as group:
	
		process_user = PythonOperator(task_id='process_user',python_callable=_process_user)
		process_director = PythonOperator(task_id='process_director',python_callable=_process_director)
		
		k8s_job = KubernetesPodOperator(
		    task_id="job-task",
		    namespace="airflow",
		    image="distiya/etl-olap:envtest1",
		    name="jobtask",
		    env_vars = envVars,
		    get_logs=True
		)
		
		process_user >> process_director >> k8s_job	
		
		return group

	
