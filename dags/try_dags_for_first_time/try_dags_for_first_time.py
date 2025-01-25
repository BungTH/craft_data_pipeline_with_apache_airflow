from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'try_dags_for_first_time',
    default_args=default_args,
    description='Try Dags for First Time',
)

def intermediate_task():
    print("This is an intermediate task")
    
def second_intermediate_task():
    print("This is the second intermediate task")
    
def send_email():
    print("Email sent")
    
def send_ms_team():
    print("MS Team Message sent")
    
def send_line_message():
    print("Line Message sent")
    
start_task = EmptyOperator(
    task_id='start_task',
    dag=dag,
)

intermediate_task = PythonOperator(
    task_id='intermediate_task',
    dag=dag,
    python_callable=intermediate_task
)

second_intermediate_task = PythonOperator( 
    task_id='second_intermediate_task',
    dag=dag,
    python_callable=second_intermediate_task
)

send_email = PythonOperator(
    task_id='send_email',
    dag=dag,
    python_callable=send_email
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=dag,
)

send_line_message = PythonOperator(
    task_id='send_line_message',
    dag=dag,
    python_callable=send_line_message
)

send_ms_team = PythonOperator(
    task_id='send_ms_team',
    dag=dag,
    python_callable=send_ms_team
)

start_task >> [ intermediate_task, second_intermediate_task ] >> end_task >> [ send_line_message, send_ms_team ]
second_intermediate_task >> send_email