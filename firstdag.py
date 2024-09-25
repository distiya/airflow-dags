from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator

test_connection = Connection.get_connection_from_secrets("test-connection")

envVars = {'TEST_USER':test_connection.login,'TEST_PASSWORD':test_connection.password}

def _process_user(ti):
	print("Printing user " + test_connection.login)
	
def _process_director(ti):
	print("Printing user " + test_connection.password)	

with DAG('first_dag',start_date=datetime(2024,9,23),schedule_interval='@daily',catchup=False) as dag:

	process_user = PythonOperator(task_id='process_user',python_callable=_process_user)
	process_director = PythonOperator(task_id='process_director',python_callable=_process_director)
	
	k8s_job = KubernetesJobOperator(
	    task_id="job-task",
	    namespace="airflow",
	    image="distiya/etl-olap:envtest",
	    name="jobtask",
	    env_vars = envVars
	)
	
	process_user >> process_director >> k8s_job
