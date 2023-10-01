import json
import os

import boto3

DYNAMODB_TABLE_NAME = os.environ["DYNAMODB_TABLE_NAME"]
S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]

dynamodb_resource = boto3.resource("dynamodb")
redshift_data_client = boto3.client("redshift-data")
s3_resource = boto3.resource("s3")
sfn_client = boto3.client("stepfunctions")


def get_s3_files_loaded_into_redshift(
    redshift_manifest_file_name: str,
) -> list[str]:
    redshift_manifest_file_key = redshift_manifest_file_name.replace(
        f"s3://{S3_BUCKET_NAME}/", ""
    )
    redshift_manifest_file = s3_resource.Object(
        bucket_name=S3_BUCKET_NAME, key=redshift_manifest_file_key
    )
    redshift_manifest_content = (
        redshift_manifest_file.get()["Body"].read().decode("utf-8")
    )
    redshift_manifest_entries = json.loads(redshift_manifest_content)["entries"]
    s3_files_loaded_into_redshift = [dct["url"] for dct in redshift_manifest_entries]
    return s3_files_loaded_into_redshift


def rename_s3_files(
    s3_file_names: list[str], s3_folder_old: str, s3_folder_new: str
) -> int:
    for s3_file_name in s3_file_names:
        s3_file_key = s3_file_name.replace(f"s3://{S3_BUCKET_NAME}/", "")
        s3_file_key_new = s3_file_key.replace(s3_folder_old, s3_folder_new)
        s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_key_new).copy_from(
            CopySource=f"{S3_BUCKET_NAME}/{s3_file_key}"
        )
        s3_resource.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_key).delete()
        print(
            f"Renamed s3://{S3_BUCKET_NAME}/{s3_file_key} to "
            f"s3://{S3_BUCKET_NAME}/{s3_file_key_new}"
        )
    print(f"Successfully renamed {len(s3_file_names)} files ✨")


def get_redshift_failure_cause(redshift_statements_id: str) -> str:
    sub_statements = redshift_data_client.describe_statement(Id=redshift_statements_id)[
        "SubStatements"
    ]
    for sub_statement in sub_statements:
        for key, value in list(sub_statement.items()):
            try:
                json.dumps(value)
            except TypeError:
                sub_statement.pop(key)  # remove all non-json elements
    return json.dumps(sub_statements)


def lambda_handler(event, context) -> None:
    # print("event", event)
    redshift_statements_state = event["detail"]["state"]
    if redshift_statements_state in ["SUBMITTED", "PICKED", "STARTED"]:
        print(f"Redshift state is {redshift_statements_state}, so ignore")
        return
    redshift_statements_id = event["detail"]["statementId"]

    response = dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).query(
        IndexName="is_still_processing_sql",  # hard coded
        KeyConditionExpression=boto3.dynamodb.conditions.Key(
            "redshift_statements_id"
        ).eq(redshift_statements_id),
        # Select='ALL_ATTRIBUTES'|'ALL_PROJECTED_ATTRIBUTES'|'SPECIFIC_ATTRIBUTES'|'COUNT',
        # AttributesToGet=['string'],
    )
    assert response["Count"] == 1, (
        f'For `redshift_statements_id` "{redshift_statements_id}", there should be exactly 1 record '
        f"but got {response['Count']} records. The records are: {response['Items']}"
    )
    record = response["Items"][0]
    task_token = record["task_token"]
    if redshift_statements_state == "FINISHED":
        num_statements = len(
            json.loads(record["sql_statements"])
        )  # assumes that 'select count(*)' is last statement
        row_count = redshift_data_client.get_statement_result(
            Id=f"{redshift_statements_id}:{num_statements}"
        )["Records"][0][0]["longValue"]

        sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps(
                {  # figure out if any other useful info to add here
                    "table_load_success": True,
                    "full_table_name": record["full_table_name"],
                    "row_count": row_count,
                    "truncate_table": record["truncate_table"],
                    "sns_topic_name": record["sns_topic_name"],
                }
            ),
        )

        dynamodb_resource.Table(name=DYNAMODB_TABLE_NAME).update_item(
            Key={
                "full_table_name": record["full_table_name"],
                "utc_now_human_readable": record["utc_now_human_readable"],
            },
            # REMOVE is more important than SET. REMOVE deletes attribute that
            # makes record show up in the table's GSI.
            UpdateExpression="REMOVE #isp SET row_count = :rc",
            ExpressionAttributeValues={":rc": row_count},
            ExpressionAttributeNames={"#isp": "is_still_processing_sql?"},
        )

        redshift_manifest_file_name = record["redshift_manifest_file_name"]
        s3_files_loaded_into_redshift = get_s3_files_loaded_into_redshift(
            redshift_manifest_file_name=redshift_manifest_file_name
        )
        rename_s3_files(
            s3_file_names=s3_files_loaded_into_redshift,
            s3_folder_old="/processing/",
            s3_folder_new="/processed/",
        )
        rename_s3_files(
            s3_file_names=[redshift_manifest_file_name],
            s3_folder_old="/processing/",
            s3_folder_new="/processed/",
        )
    elif redshift_statements_state in ["ABORTED", "FAILED"]:
        error_message = (
            f"`redshift_statements_id` {redshift_statements_id} unsuccessful "
            f"with state being {redshift_statements_state}"
        )
        cause_message = get_redshift_failure_cause(
            redshift_statements_id=redshift_statements_id
        )
        sfn_client.send_task_failure(
            taskToken=task_token,
            error=error_message,  ### figure out what to write here
            cause=cause_message,  ### figure out what to write here
        )
    else:  # 'ALL'
        error_message = (
            f"`redshift_statements_id` {redshift_statements_id} unsuccessful "
            f"with state being {redshift_statements_state}"
        )
        cause_message = get_redshift_failure_cause(
            redshift_statements_id=redshift_statements_id
        )
        sfn_client.send_task_failure(
            taskToken=task_token,
            error=error_message,  ### figure out what to write here
            cause=cause_message,  ### figure out what to write here
        )
