from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def _process_user(ti):
	print("Printing user")
	
def _process_director(ti):
	print("Printing director")	

with DAG('first_dag',start_date=datetime(2024,9,23),schedule_interval='@daily',catchup=False) as dag:

	process_user = PythonOperator(task_id='process_user',python_callabale=_process_user)
	process_director = PythonOperator(task_id='process_director',python_callabale=_process_director)
	
	process_user >> process_director
