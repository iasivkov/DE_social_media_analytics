from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime.now().strftime('%Y-%m-%d'),
                                }

dag_spark = DAG(
                        dag_id = "dag_tag_7",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_tags',
                        dag=dag_spark,
                        application ='/lessons/verified_tags_candidates.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', '7', '100', '/user/iasivkov/data/events',
                        '/user/master/data/snapshots/tags_verified/actual', '/user/iasivkov/data/analytics'],
                        )

spark_submit_local