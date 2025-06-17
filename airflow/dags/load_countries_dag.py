import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="load_countries_dag",
    start_date=pendulum.datetime(2025, 6, 16, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["geospatial", "onboarding"],
) as dag:
    load_countries_task = BashOperator(
        task_id="load_countries_to_postgis",
        # This command tells an Airflow worker to execute my script.
        # It works because I mounted ./src to /opt/airflow/src in the docker-compose.yaml
        bash_command="python /opt/airflow/src/load_countries.py",
    )
