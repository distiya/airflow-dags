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
import json

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

def _process_user(ti):
	print("Printing Process User")
	
def _process_director(ti):
	print("Printing Process Director")
	
def _process_databricks(**kwargs):
	ti = kwargs['ti']
	returnStatus = ti.xcom_pull(task_ids="jobtask", key="return_value")
	#returnStatus = json.loads(return_status_string)	
	if(returnStatus["status"] == 0):
		print("Return Status is OK")
		print(f"Return file is {returnStatus["fileName"]}")
		

@task_group()	
def etl_tasks(report_date: str):

	envVars = {'TEST_USER':report_date,'TEST_PASSWORD':test_connection.password}

	process_user = PythonOperator(task_id='process_user',python_callable=_process_user)
	process_director = PythonOperator(task_id='process_director',python_callable=_process_director)
	
	k8s_job = KubernetesPodOperator(
	    task_id="job-task",
	    namespace="airflow",
	    image="distiya/etl-olap:envtest3",
	    name="jobtask",
	    env_vars = envVars,
	    get_logs=True,
	    do_xcom_push=True
	)
	
	process_databricks = PythonOperator(task_id='process_databricks',python_callable=_process_databricks,provide_context=True)
	
	process_user >> process_director >> k8s_job >> process_databricks

	
