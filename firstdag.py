from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from taskgroup import parent_group

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

envVars = {'TEST_USER':datetime.now(),'TEST_PASSWORD':test_connection.password}

list_dates = ['2024-04-13', '2024-05-13']

def _process_first(ti):
	print("Printing Process First")	

with DAG('first_dag_6',start_date=datetime(2024,9,23),schedule_interval=None,catchup=False, params={"startDate":Param((date.today() - timedelta(days=1)).strftime('%Y-%m-%d'), type="string", format="date"),"endDate":Param(date.today().strftime('%Y-%m-%d'), type="string", format="date")}) as dag:

	process_first = PythonOperator(task_id='process_first',python_callable=_process_first)
	
	process_first >> parent_group(list_dates)
