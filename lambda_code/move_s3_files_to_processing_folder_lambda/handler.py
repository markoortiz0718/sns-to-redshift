import json
import os
from typing import Union

import boto3

S3_BUCKET_NAME = os.environ["S3_BUCKET_NAME"]
S3_BUCKET_PREFIX_FOR_FIREHOSE = os.environ["S3_BUCKET_PREFIX_FOR_FIREHOSE"]
s3 = boto3.resource("s3")


def rename_s3_files(s3_prefix_old: str, s3_prefix_new: str) -> int:
    s3_bucket = s3.Bucket(name=S3_BUCKET_NAME)
    s3_file_count = 0
    s3_file_names_new = []
    for object_summary in s3_bucket.objects.filter(
        Prefix=s3_prefix_old
    ):  # uses pagination behind the scenes
        s3_file_name_old = object_summary.key
        s3_file_name_new = s3_file_name_old.replace(s3_prefix_old, s3_prefix_new)
        s3.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_new).copy_from(
            CopySource=f"{S3_BUCKET_NAME}/{s3_file_name_old}"
        )
        s3.Object(bucket_name=S3_BUCKET_NAME, key=s3_file_name_old).delete()
        s3_file_names_new.append(f"s3://{S3_BUCKET_NAME}/{s3_file_name_new}")
        print(
            f"Renamed s3://{S3_BUCKET_NAME}/{s3_file_name_old} to "
            f"s3://{S3_BUCKET_NAME}/{s3_file_name_new}"
        )
        s3_file_count += 1
    if s3_file_count:
        print(f"Successfully renamed {s3_file_count} files ✨")
    return s3_file_count, s3_file_names_new


def lambda_handler(event, context) -> dict[str, Union[str, dict[str, str]]]:
    s3_bucket_prefix_for_firehose = S3_BUCKET_PREFIX_FOR_FIREHOSE.format(
        SNS_TOPIC_NAME=event["eventbridge_payload"]["SNS_TOPIC_NAME"]
    )
    s3_prefix_processing = (
        s3_bucket_prefix_for_firehose.replace(
            "/unprocessed/", "/processing/"
        )  # hard coded
        + "firehose_files/"
    )
    s3_prefix_unexpected = s3_prefix_processing.replace(
        "/processing/", "/__should_have_been_processed_but_not__/"  # hard coded
    )
    s3_file_count, _ = rename_s3_files(
        s3_prefix_old=s3_prefix_processing,
        s3_prefix_new=s3_prefix_unexpected,
    )
    if s3_file_count:
        print(
            f"There should be 0 files in {s3_bucket_prefix_for_firehose} but got "
            f"{s3_file_count} files, so moved them to {s3_prefix_unexpected}"
        )
    s3_file_count, s3_files_to_load_into_redshift = rename_s3_files(
        s3_prefix_old=s3_bucket_prefix_for_firehose,
        s3_prefix_new=s3_prefix_processing,
    )
    if s3_file_count:
        manifest_files = {
            "entries": [
                {"url": s3_file, "mandatory": True}
                for s3_file in s3_files_to_load_into_redshift
            ]
        }
        s3_prefix_processing_manifest = s3_prefix_processing.replace(
            "/firehose_files/", "/manifest_files"
        )
        redshift_manifest_file_name = (
            f"{s3_prefix_processing_manifest}/"
            f"{event['Execution']}_sfn_run.manifest"  # assumes `Execution` is unique
        )
        manifest_file = s3.Object(
            bucket_name=S3_BUCKET_NAME, key=redshift_manifest_file_name
        )
        manifest_file.put(Body=json.dumps(manifest_files).encode("utf-8"))
    response = {
        "s3_prefix_processing": s3_prefix_processing,
        "s3_file_count": s3_file_count,
        "s3_files_to_load_to_redshift": s3_files_to_load_into_redshift,
        "other_metadata": {
            "s3_prefix_unprocessed": s3_bucket_prefix_for_firehose,
            "s3_prefix_unexpected": s3_prefix_unexpected,
        },
        "eventbridge_payload": event["eventbridge_payload"],
        "redshift_load_every_x_seconds": event["eventbridge_payload"][
            "REDSHIFT_LOAD_EVERY_X_MINUTES"
        ]
        * 60,  # needed by next Lambda's timeout in SFN
    }
    if s3_file_count:
        response[
            "redshift_manifest_file_name"
        ] = f"s3://{S3_BUCKET_NAME}/{redshift_manifest_file_name}"
    return response
