from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

def _process_user(ti):
	print("Printing Process User")
	
def _process_director(ti):
	print("Printing Process Director")	
	
def etl_tasks(testUser):

	envVars = {'TEST_USER':testUser,'TEST_PASSWORD':test_connection.password}

	with TaskGroup(f"trading_performance_etl{testUser}", tooltip=f"Trading Performance ETL {testUser}") as group:
	
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

	
