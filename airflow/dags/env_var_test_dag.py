from airflow.sdk import Variable
from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id='variable_dag',
    start_date=datetime(2025, 6, 17),
    schedule=None,
    catchup=False,
    tags=["example", "variable"],
    doc_md="""
    ### Variable DAG example
    This DAG demonstrates how to retrieve and display an Airflow Variable.
    """,
) 
def variable_dag():    
    @task
    def display_variable():
        my_var = Variable.get("my_test_var")
        print(f"The value of my_var is: {my_var}")
        return my_var

    display_task = display_variable()

# instantiate the DAG
variable_dag()


