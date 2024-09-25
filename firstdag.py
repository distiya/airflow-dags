from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from taskgroup import etl_tasks

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

envVars = {'TEST_USER':datetime.now(),'TEST_PASSWORD':test_connection.password}

def _process_first(ti):
	print("Printing Process First")	

with DAG('first_dag_5',start_date=datetime(2024,9,23),schedule_interval=None,catchup=False, params={"x":Param("2023-04-11", type="string", format="date")}) as dag:

	process_first = PythonOperator(task_id='process_first',python_callable=_process_first)
	
	etl = etl_tasks()
	
	process_first >> etl
