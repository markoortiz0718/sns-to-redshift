import json
import os
import re
from datetime import datetime, timedelta

import boto3

DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
DYNAMODB_TTL_IN_DAYS = json.loads(os.environ["DYNAMODB_TTL_IN_DAYS"])
REDSHIFT_CLUSTER_NAME = os.environ["REDSHIFT_CLUSTER_NAME"]
REDSHIFT_ROLE_NAME = os.environ["REDSHIFT_ROLE_NAME"]
REDSHIFT_SECRET_NAME = os.environ["REDSHIFT_SECRET_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

dynamodb_resource = boto3.resource("dynamodb")
iam_client = boto3.client("iam")
redshift_data_client = boto3.client("redshift-data")
secrets_manager_client = boto3.client("secretsmanager")


def remove_extra_whitespaces(sql_statement: str) -> str:
    "Be careful to avoid unexpected problems"
    return re.sub(r"\s+", " ", sql_statement)


def lambda_handler(event, context) -> None:
    # print(f"event: {event}")
    redshift_manifest_file_name = event["redshift_manifest_file_name"]
    task_token = event["task_token"]
    eventbridge_payload = event["eventbridge_payload"]
    redshift_database_name = eventbridge_payload["REDSHIFT_DATABASE_NAME"]
    redshift_schema_name = eventbridge_payload["REDSHIFT_SCHEMA_NAME"]
    redshift_table_name = eventbridge_payload["REDSHIFT_TABLE_NAME"]
    file_type = eventbridge_payload["FILE_TYPE"]
    redshift_copy_additional_arguments = eventbridge_payload[
        "REDSHIFT_COPY_ADDITIONAL_ARGUMENTS"
    ]
    truncate_table = eventbridge_payload["TRUNCATE_TABLE"]
    sns_topic_name = eventbridge_payload["SNS_TOPIC_NAME"]

    sql_statements = []
    if truncate_table:
        sql_statements.append(
            f"truncate {redshift_database_name}.{redshift_schema_name}.{redshift_table_name};"
        )
    redshift_role_arn = iam_client.get_role(RoleName=REDSHIFT_ROLE_NAME)["Role"]["Arn"]
    sql_statements.extend(
        [
            f"""copy {redshift_database_name}.{redshift_schema_name}.{redshift_table_name}
            from '{redshift_manifest_file_name}'
            iam_role '{redshift_role_arn}'
            format as {file_type} {redshift_copy_additional_arguments} manifest;
            """.replace(
                "\n", ""
            ),
            f"select count(*) from {redshift_database_name}.{redshift_schema_name}.{redshift_table_name};",
        ]
    )
    sql_statements = [
        remove_extra_whitespaces(sql_statement=sql_statement)
        for sql_statement in sql_statements
    ]
    redshift_secret_arn = secrets_manager_client.describe_secret(
        SecretId=REDSHIFT_SECRET_NAME
    )["ARN"]
    response = redshift_data_client.batch_execute_statement(
        ClusterIdentifier=REDSHIFT_CLUSTER_NAME,
        SecretArn=redshift_secret_arn,
        Database=redshift_database_name,
        Sqls=sql_statements,
        WithEvent=True,
    )
    # print(response)
    response.pop(
        "CreatedAt", None
    )  # has a datetime() object that is not JSON serializable
    utc_now = datetime.utcnow()
    records_expires_on = utc_now + timedelta(days=DYNAMODB_TTL_IN_DAYS)
    dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).put_item(
        Item={
            "full_table_name": f"{redshift_database_name}.{redshift_schema_name}.{redshift_table_name}",  # pk in primary index
            "utc_now_human_readable": utc_now.strftime("%Y-%m-%d %H:%M:%S")
            + " UTC",  # sk in primary index
            "redshift_statements_id": response["Id"],  # pk in GSI
            "is_still_processing_sql?": "yes",  # sk in GSI
            "task_token": task_token,
            "sql_statements": json.dumps(sql_statements),
            "truncate_table": truncate_table,  # needed by `redshift_statements_finished_lamba`
            "redshift_manifest_file_name": redshift_manifest_file_name,  # needed by `redshift_statements_finished_lambda`
            "redshift_response": json.dumps(response),
            "sns_topic_name": sns_topic_name,  # needed by `redshift_statements_finished_lambda`
            "delete_record_on": int(records_expires_on.timestamp()),
            "delete_record_on_human_readable": records_expires_on.strftime(
                "%Y-%m-%d %H:%M:%S" + " UTC"
            ),
        }
    )
