#import the dag object
from airflow import DAG
#import the operators for our tasks
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
#import the date time
from datetime import datetime, timedelta
import logging

#default DAG arguments - all tasks will get these 
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'email': ['airflow@my_first_dag.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def add_to_logs(num, **kwargs):
    logging.info("task number {} succeeded.".format(num))


dag = DAG('test_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5), #the interval between two runs of our DAG [day after day run]
)

with dag:

    task_1 = PythonOperator(
        task_id = 'first_task',
        python_callable=add_to_logs,
        op_kwargs={'num': '1'},
    )

    task_2 = PythonOperator(
        task_id = 'second_task',
        python_callable=add_to_logs,
        op_kwargs={'num': '2'},
    )

    task_1 >> task_2 