from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'first_dag',    # Should Name this Dag the same as Pipeline Name for better readability
    default_args=default_args,
    description='My first Airflow DAG with an empty operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def hello_world():
    print("hello world")

# Mock Operator, Do nothing
start = EmptyOperator(  
    task_id='start',    # taks_id can be name the same as sthe variable name
    dag=dag,
)

hello_world = PythonOperator(
    task_id='hello-world',  # This will call hello_world function above
    dag=dag,
    python_callable=hello_world
)

# Mock Operator, Same as `start`
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Dependency Tasks
start >> hello_world >> end