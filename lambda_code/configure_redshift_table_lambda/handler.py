import json
import os
import re
import time

import boto3

DETAILS_ON_TOPICS = json.loads(os.environ["DETAILS_ON_TOPICS"])
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_SECRET_NAME = os.environ["REDSHIFT_SECRET_NAME"]

redshift_data_client = boto3.client("redshift-data")
secrets_manager_client = boto3.client("secretsmanager")


def remove_extra_whitespaces(sql_statement: str) -> str:
    "Be careful to avoid unexpected problems"
    return re.sub(r"\s+", " ", sql_statement)


def execute_sql_statement(sql_statement: str, redshift_database_name: str) -> None:
    redshift_secret_arn = secrets_manager_client.describe_secret(
        SecretId=REDSHIFT_SECRET_NAME
    )["ARN"]
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        SecretArn=redshift_secret_arn,  # secret currently supports 1 database
        Database=redshift_database_name,
        Sql=sql_statement,
    )
    time.sleep(1)
    while True:
        response = redshift_data_client.describe_statement(Id=response["Id"])
        status = response["Status"]
        if status == "FINISHED":
            print(f"Finished executing the following SQL statement: {sql_statement}")
            return
        elif status in ["SUBMITTED", "PICKED", "STARTED"]:
            time.sleep(1)
        elif status == "FAILED":
            print(response)
            raise  ### figure out useful message in exception
        else:
            print(response)
            raise  ### figure out useful message in exception


def lambda_handler(event, context) -> None:
    for topic_details in DETAILS_ON_TOPICS:
        redshift_database_name = topic_details["REDSHIFT_DATABASE_NAME"]
        redshift_schema_name = topic_details["REDSHIFT_SCHEMA_NAME"]
        redshift_table_name = topic_details["REDSHIFT_TABLE_NAME"]
        redshift_table_columns_and_types = topic_details[
            "REDSHIFT_TABLE_COLUMNS_AND_TYPES"
        ]
        sql_statements = [
            f'create schema if not exists "{redshift_schema_name}";',
            f"""create table if not exists
                "{redshift_database_name}"."{redshift_schema_name}"."{redshift_table_name}"
                ({redshift_table_columns_and_types});""",
        ]
        for sql_statement in sql_statements:
            execute_sql_statement(
                sql_statement=remove_extra_whitespaces(sql_statement=sql_statement),
                redshift_database_name=redshift_database_name,
            )
