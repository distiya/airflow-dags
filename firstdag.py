from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from taskgroup import parent_group
from airflow.decorators import dag, task, task_group
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun

#test_connection = Connection.get_connection_from_secrets("test-connection")
test_connection = BaseHook.get_connection("test-connection")

envVars = {'TEST_USER':datetime.now(),'TEST_PASSWORD':test_connection.password}

list_dates = ['2024-04-13', '2024-05-13']   

with DAG('first_dag_10',start_date=datetime(2024,9,23),schedule_interval=None,catchup=False, params={"startDate":Param((date.today() - timedelta(days=1)).strftime('%Y-%m-%d'), type="string", format="date"),"endDate":Param(date.today().strftime('%Y-%m-%d'), type="string", format="date")}) as dag:

	@task
	def process_dates(**kwargs):
		dr: DagRun = kwargs["dag_run"]
		params = dr.conf["params"]
		startDate = date_object = datetime.strptime(params["startDate"], "%Y-%m-%d").date()
		endDate = date_object = datetime.strptime(params["endDate"], "%Y-%m-%d").date()
		dates = []
		delta = timedelta(days=1)
		while startDate <= endDate:
		    dates.append(startDate.strftime('%Y-%m-%d'))
		    startDate += delta
		return dates 
	
	parent_group(process_dates())
