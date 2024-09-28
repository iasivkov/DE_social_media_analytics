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
                        dag_id = "dag_calc_marts",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_ods = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application ='/lessons/partition.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ["2022-05-31", '/user/master/data/events', '/user/iasivkov/data/events'],
                        )
spark_submit_tag_7 = SparkSubmitOperator(
                        task_id='spark_submit_tags_7',
                        dag=dag_spark,
                        application ='/lessons/verified_tags_candidates.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', '7', '100', '/user/iasivkov/data/events',
                        '/user/master/data/snapshots/tags_verified/actual', '/user/iasivkov/data/analytics'],
                        )
spark_submit_tag_84 = SparkSubmitOperator(
                        task_id='spark_submit_tags_84',
                        dag=dag_spark,
                        application ='/lessons/verified_tags_candidates.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', '84', '1000', '/user/iasivkov/data/events',
                        '/user/master/data/snapshots/tags_verified/actual', '/user/iasivkov/data/analytics'],
                        )
                        
spark_submit_tag_like_7 = SparkSubmitOperator(
                        task_id='spark_submit_tag_like_7',
                        dag=dag_spark,
                        application ='/lessons/user_interests.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-25', '7', '/user/iasivkov/data/events',
                        '/user/iasivkov/data/analytics/user_interests_d7'],
                        )
spark_submit_tag_like_28 = SparkSubmitOperator(
                        task_id='spark_submit_tag_like_28',
                        dag=dag_spark,
                        application ='/lessons/user_interests.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-31', '28', '/user/iasivkov/data/events',
                        '/user/iasivkov/data/analytics/user_interests_d28'],
                        )   
spark_submit_conn_like_7 = SparkSubmitOperator(
                        task_id='spark_submit_conn_like_7',
                        dag=dag_spark,
                        application ='/lessons/connection_interests.py' ,
                        conn_id= 'yarn_spark',
                        application_args = ['2022-05-25', '7', '/user/iasivkov/data/events',
                        '/user/iasivkov/data/analytics/connection_interests_d7'],
                        )                     

spark_submit_ods>>[spark_submit_tag_7, spark_submit_tag_84, spark_submit_tag_like_7, spark_submit_tag_like_28]
spark_submit_tag_like_7>>spark_submit_conn_like_7