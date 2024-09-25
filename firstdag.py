from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.models import Connection

test_connection = Connection.get_connection_from_secrets("test-connection")

def _process_user(ti):
	print("Printing user " + test_connection.login)
	
def _process_director(ti):
	print("Printing user " + test_connection.password)	

with DAG('first_dag',start_date=datetime(2024,9,23),schedule_interval='@daily',catchup=False) as dag:

	process_user = PythonOperator(task_id='process_user',python_callable=_process_user)
	process_director = PythonOperator(task_id='process_director',python_callable=_process_director)
	
	process_user >> process_director
