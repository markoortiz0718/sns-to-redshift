#!/usr/bin/env python3
import aws_cdk as cdk
import boto3

from sns_to_redshift.sns_to_redshift_stack import SnsToRedshiftStack


app = cdk.App()
account = boto3.client("sts").get_caller_identity()["Account"]
environment = app.node.try_get_context("environment")
environment["SHARED_STACK_VARS"]["AWS_ACCOUNT"] = account
SnsToRedshiftStack(
    app,
    "SnsToRedshiftStack",
    env=cdk.Environment(account=account, region=environment["SHARED_STACK_VARS"]["AWS_REGION"]),
    environment=environment,
)
app.synth()