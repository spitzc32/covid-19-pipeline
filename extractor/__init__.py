from dagster import schedule, Definitions, EnvVar, config_from_files, file_relative_path
from dagster_docker import docker_executor
from os.path import join

from extractor.job.pipeline import etl_dag_graph
from extractor.resources.sql_alchemy import SqlAlchemyClientResource


etl_pipeline = etl_dag_graph.to_job(
    name="etl_extractor",
    config= config_from_files(
        [
            file_relative_path(__file__, join('config', 'extract.yml'))
        ]
    ),
    executor_def=docker_executor
)


@schedule(cron_schedule="*/5 * * * *", job=etl_pipeline)
def schedule_etl(_context):
    return {}


defs = Definitions(
    resources={
        
        "postgres_io_manager": SqlAlchemyClientResource(
            database_ip=EnvVar("DATABASE_IP"),
            database_port=EnvVar("DATABASE_PORT"),
            database_user=EnvVar("DATABASE_USER"),
            database_password=EnvVar("DATABASE_PASSWORD"),
            database_name=EnvVar("DATABASE_NAME")
        )
    },
    schedules=[schedule_etl])