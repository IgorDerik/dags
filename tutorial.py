from builtins import range
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from airflow.operators.http_operator import SimpleHttpOperator
import json

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='convert_csv2avro_spark_and_livy-4',
    default_args=args,
    schedule_interval=None,
    tags=['tutorial']
)

sparkConverter = SimpleHttpOperator(task_id="converter",
		http_conn_id="sparkLivyEndpoint",
		endpoint="/batches/", 
		method="POST",
		data=json.dumps({
			  "file": "/tmp/hdfs_data/sparktask-1.0-SNAPSHOT-jar-with-dependencies.jar",
			  "className": "com.kozachuk.learning.spark.SparkTransfer",
			  "args": [
				"spark://hdfs.host.1.com:40123"
			  ],
			  "conf": {
				"master": "hdfs.host.1.com:40123",
				"driver-memory": "6g",
				"executor-memory": "6g",
				"packages": "org.apache.spark:spark-avro_2.11:2.4.6"
			  }
			})
		, headers={"Content-Type" : "application/json"}
		, log_response=True
		, xcom_push=True
		, dag=dag
	)

start_task = DummyOperator( task_id='start', dag=dag)
end_task = DummyOperator( task_id='end', dag=dag)

start_task >> sparkConverter
sparkConverter >> end_task
