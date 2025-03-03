import os
from datetime import datetime, timedelta
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from dotenv import load_dotenv

dbt_env_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dbt-capstone', 'dbt.env')
load_dotenv(dbt_env_path)

# Retrieve environment variables
airflow_home = os.getenv('AIRFLOW_HOME')
PATH_TO_DBT_PROJECT = f'{airflow_home}/capstone-dbt'
PATH_TO_DBT_PROFILES = f'{airflow_home}/capstone-dbt/profiles.yml'

profile_config = ProfileConfig(
    profile_name="capstone_dbt",
    target_name="dev",
    profiles_yml_filepath=PATH_TO_DBT_PROFILES,
)

default_args = {
  "owner": "Sam Do & Albert Campillo",
  "retries": 0,
  "execution_timeout": timedelta(hours=1),
}

@dag(
    start_date=datetime(2024, 5, 9),
    schedule='@once',
    catchup=False,
    default_args=default_args,
)

def main():
    dbt_build_whole_project = BashOperator(
        task_id = 'dbt_build_whole_project',
        bash_command='export DBT_SCHEMA="acampi" && cd /usr/local/airflow/capstone-dbt_2 && dbt build'
    )

# def dbt_dag_cosmos_DbtTaskGroup_whole_capstone():
#     pre_dbt_workflow = EmptyOperator(task_id="pre_dbt_workflow")

#     dbt_run_staging = DbtTaskGroup(
#         group_id="capstone_dbt_cosmos_dag",
#         project_config=ProjectConfig(PATH_TO_DBT_PROJECT),
#         profile_config=profile_config,
#         render_config=RenderConfig(
#             exclude=["path:models/blank"],
#         ),
#     )

    # Post DBT workflow task
    post_dbt_workflow = EmptyOperator(task_id="post_dbt_workflow", trigger_rule="all_done")

    # Define task dependencies
    # pre_dbt_workflow >> dbt_run_staging >> post_dbt_workflow
    dbt_build_whole_project >> post_dbt_workflow

# dbt_dag_cosmos_DbtTaskGroup_whole_capstone()
main()
