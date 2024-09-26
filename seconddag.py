from airflow import DAG
from datetime import date, datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow.providers.cncf.kubernetes.operators.job import KubernetesJobOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from taskgroup import etl_tasks
from airflow.decorators import dag, task, task_group
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

params = {
	"startDate":Param((date.today() - timedelta(days=1)).strftime('%Y-%m-%d'), type="string", format="date"),
	"endDate":Param(date.today().strftime('%Y-%m-%d'), type="string", format="date")
}


@dag(
    dag_id="second_dag_7",
    default_args=default_args,
    schedule_interval=None,  # No schedule for now
    start_date=days_ago(1),
    catchup=False,
    params = params
)
def second_dag():
	@task
	def process_dates(params: dict) -> list[str]:
		startDate = date_object = datetime.strptime(params["startDate"], "%Y-%m-%d").date()
		endDate = date_object = datetime.strptime(params["endDate"], "%Y-%m-%d").date()
		dates = []
		delta = timedelta(days=1)
		while startDate <= endDate:
			dates.append(startDate.strftime('%Y-%m-%d'))
			startDate += delta
		return dates
	
	etl_tasks.expand(report_date=process_dates()) 

second_dag()
