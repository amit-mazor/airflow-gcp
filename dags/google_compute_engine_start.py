from airflow import DAG
#from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator
#import os
from datetime import datetime
import google.cloud 
from airflow.operators.python_operator import PythonOperator
import googleapiclient.discovery


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}


#GCP_PROJECT_ID     = os.environ.get('GCP_PROJECT_ID', 'airflow-gcp')
#GCE_ZONE = os.environ.get('GCE_ZONE', 'europe-west3-b')
#GCE_INSTANCE = os.environ.get('GCE_INSTANCE', 'instance-airflow')

def creating_instance(project, bucket, zone, instance_name, **kwargs):
    credentials = service_account.Credentials.from_service_account_file(credentials_path) if credentials_path else None
    storage_client = storage.Client(project=project, credentials=credentials)
    compute = googleapiclient.discovery.build('compute', 'v1')
    #creating instance
    operation = create_instance(compute, project, zone, instance_name, bucket)
    wait_for_operation(compute, project, zone, operation['name'])


dag = DAG('compute_engine',
            default_args=default_args,
            catchup=False)

with dag:

    instance_start_task = PythonOperator(
        task_id='create_new_instance',
        python_callable=creating_instance,
        op_kwargs={'project': 'airflow-gcp', 'bucket': 'airflow-gcp-bucket', 'zone': 'europe-west3-b', 'instance_name': 'instance-new'}
    )

    instance_start_task