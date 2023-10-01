import json
from collections import defaultdict

from aws_cdk import (
    CfnOutput,
    Duration,
    NestedStack,
    RemovalPolicy,
    SecretValue,
    Stack,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_lambda as _lambda,
    aws_redshift as redshift,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    triggers,
)
from constructs import Construct


class PreexistingStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.redshift_role = iam.Role(
            self,
            "RedshiftRole",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            role_name=environment["SHARED_STACK_VARS"]["REDSHIFT_ROLE_NAME"],
        )
        self.redshift_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=["*"],
            ),
        )
        self.lambda_publish_to_sns_role = iam.Role(
            self,
            "LambdaPublishToSnsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.lambda_publish_to_sns_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=["*"],
            ),
        )

        self.redshift_cluster = redshift.CfnCluster(
            self,
            "RedshiftCluster",
            cluster_type="single-node",  # for demo purposes
            number_of_nodes=1,  # for demo purposes
            node_type="dc2.large",  # for demo purposes
            cluster_identifier=environment["SHARED_STACK_VARS"][
                "REDSHIFT_CLUSTER_NAME"
            ],
            db_name=environment["PREEXISTING_STACK_VARS"]["REDSHIFT_DATABASE_NAME"],
            master_username=environment["PREEXISTING_STACK_VARS"]["REDSHIFT_USER"],
            master_user_password=environment["PREEXISTING_STACK_VARS"][
                "REDSHIFT_PASSWORD"
            ],
            iam_roles=[self.redshift_role.role_arn],
            publicly_accessible=False,
        )
        self.redshift_secret = secretsmanager.Secret(
            self,
            "RedshiftSecret",
            secret_name=environment["SHARED_STACK_VARS"]["REDSHIFT_SECRET_NAME"],
            secret_object_value={
                "username": SecretValue.unsafe_plain_text(
                    environment["PREEXISTING_STACK_VARS"]["REDSHIFT_USER"]
                ),
                "password": SecretValue.unsafe_plain_text(
                    environment["PREEXISTING_STACK_VARS"]["REDSHIFT_PASSWORD"]
                ),
            },
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.publish_sns_messages_lambda = _lambda.Function(
            self,
            "PublishSnsMessages",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_sns_messages_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # pretty fast
            memory_size=128,
            role=self.lambda_publish_to_sns_role,
        )

        # instantiating AWS resources per SNS topic
        self.sns_topics = {}
        self.scheduled_eventbridge_rules = {}
        for topic_details in environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]:
            sns_topic_name = topic_details["SNS_TOPIC_NAME"]
            sns_topic = sns.Topic(
                self, f"SnsTopic--{sns_topic_name}", topic_name=sns_topic_name
            )
            self.sns_topics[sns_topic_name] = sns_topic

            scheduled_eventbridge_rule = events.Rule(
                self,
                f"RunPeriodicallyForTopic--{sns_topic_name}",
                rule_name=f"start-publishing-messages-to-topic--{sns_topic_name}",
                event_bus=None,  # scheduled events must be on "default" bus
                schedule=events.Schedule.rate(
                    Duration.minutes(
                        topic_details["SNS_GENERATE_MESSAGES_EVERY_X_MINUTES"]
                    )
                ),
            )
            self.scheduled_eventbridge_rules[
                sns_topic_name
            ] = scheduled_eventbridge_rule
        assert (
            len(environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"])
            == len(self.sns_topics)
            == len(self.scheduled_eventbridge_rules)
        )

        # connect the AWS resources
        for topic_details in environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]:
            sns_topic_name = topic_details["SNS_TOPIC_NAME"]
            sns_topic_arn = (
                f"arn:aws:sns:{environment['SHARED_STACK_VARS']['AWS_REGION']}:"
                f"{environment['SHARED_STACK_VARS']['AWS_ACCOUNT']}:{sns_topic_name}"
            )
            scheduled_eventbridge_rule = self.scheduled_eventbridge_rules[
                sns_topic_name
            ]
            scheduled_eventbridge_rule.add_target(
                events_targets.LambdaFunction(
                    handler=self.publish_sns_messages_lambda,
                    event=events.RuleTargetInput.from_object(
                        {
                            **topic_details,
                            **{"sns_topic_arn": sns_topic_arn},
                        }
                    ),
                    # dead_letter_queue=None,
                )
            )


class UpgradeStack(NestedStack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        environment: dict,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.eventbridge_sfn_role = iam.Role(
            self,
            "EventbridgeSfnRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("events.amazonaws.com"),
                # iam.ServicePrincipal("scheduler.amazonaws.com"),
                iam.ServicePrincipal(
                    f"states.{environment['SHARED_STACK_VARS']['AWS_REGION']}.amazonaws.com"
                ),
            ),
            role_name="eventbridge_sfn_role",  # hard coded
        )
        self.eventbridge_sfn_role.add_to_policy(
            statement=iam.PolicyStatement(  # Eventbridge trigger SFN
                actions=["states:StartExecution"],
                resources=["*"],
            ),
        )
        self.eventbridge_sfn_role.add_to_policy(
            statement=iam.PolicyStatement(  # SFN trigger Lambda
                actions=["lambda:InvokeFunction"],
                resources=["*"],
            ),
        )
        self.sns_write_to_firehose_role = iam.Role(
            self,
            "SnsWriteToFirehoseRole",
            assumed_by=iam.ServicePrincipal("sns.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonSNSRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.sns_write_to_firehose_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "firehose:DescribeDeliveryStream",
                    "firehose:ListDeliveryStreams",
                    "firehose:ListTagsForDeliveryStream",
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch",
                ],
                resources=["*"],
            ),
        )
        self.firehose_write_to_s3_role = iam.Role(
            self,
            "FirehoseWriteToS3Role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("firehose.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    # "s3:AbortMultipartUpload",
                    # "s3:GetBucketLocation",
                    # "s3:GetObject",
                    # "s3:ListBucket",
                    # "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                ],
                resources=["*"],
            ),
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "logs:PutLogEvents",
                    # "logs:CreateLogGroup",  # extra permission
                    # "logs:CreateLogStream",  # extra permission
                ],
                resources=["*"],
            ),
        )
        self.firehose_write_to_s3_role.add_to_policy(
            statement=iam.PolicyStatement(  # for ExtendedS3DestinationConfigurationProperty
                actions=["lambda:InvokeFunction"],
                resources=["*"],
            ),
        )
        self.lambda_redshift_access_role = iam.Role(
            self,
            "LambdaRedshiftAccessRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        # for `configure_redshift_table_lambda` and `truncate_and_load_redshift_table_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    # for `configure_redshift_table_lambda` and `redshift_statements_finished_lambda`
                    "redshift-data:DescribeStatement",
                    # for `configure_redshift_table_lambda` and `truncate_and_load_redshift_table_lambda`
                    "redshift:GetClusterCredentials",
                    "redshift-data:ExecuteStatement",
                    "redshift-data:BatchExecuteStatement",
                    "secretsmanager:DescribeSecret",  # needed to get secret ARN
                    "secretsmanager:GetSecretValue",  # needed for authenticating BatchExecuteStatement
                    # for `truncate_and_load_redshift_table_lambda`
                    "iam:GetRole",  # needed to get Redshift role ARN
                ],
                resources=["*"],
            ),
        )
        # for `move_s3_files_to_processing_folder_lambda` and `redshift_statements_finished_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "s3:GetObject*",
                    "s3:List*",
                    "s3:DeleteObject*",
                    "s3:PutObject",
                ],
                resources=["*"],
            ),
        )
        # for `truncate_and_load_redshift_table_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=["dynamodb:PutItem"],
                resources=["*"],
            ),
        )
        # for `redshift_statements_finished_lambda`
        self.lambda_redshift_access_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "redshift-data:GetStatementResult",
                    "states:SendTaskSuccess",
                    "states:SendTaskFailure",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                ],
                resources=["*"],
            ),
        )

        self.s3_bucket_for_sns_messages = s3.Bucket(
            self,
            "S3BucketForSnsMessages",
            bucket_name=environment["UPGRADE_STACK_VARS"]["S3_BUCKET_NAME"],
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=False,  # if versioning disabled, then expired files are deleted
            # lifecycle_rules=[
            #     s3.LifecycleRule(
            #         id="expire_files_with_certain_prefix_after_1_day",
            #         expiration=Duration.days(1),
            #         prefix=f"{environment['PROCESSED_DYNAMODB_STREAM_FOLDER']}/",
            #     ),
            # ],
        )

        self.dynamodb_table = dynamodb.Table(
            self,
            "DynamoDBTableForRedshiftStatements",
            table_name=environment["UPGRADE_STACK_VARS"]["DYNAMODB_TABLE_NAME"],
            partition_key=dynamodb.Attribute(  # hard coded
                name="full_table_name", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(  # hard coded
                name="utc_now_human_readable", type=dynamodb.AttributeType.STRING
            ),
            time_to_live_attribute="delete_record_on",  # hard coded
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.dynamodb_table.add_global_secondary_index(
            index_name="is_still_processing_sql",  # hard coded
            partition_key=dynamodb.Attribute(  # hard coded
                name="redshift_statements_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(  # hard coded
                name="is_still_processing_sql?", type=dynamodb.AttributeType.STRING
            ),
        )

        self.event_rule_to_trigger_redshift_statements_finished_lambda = events.Rule(
            self,
            "EventRuleToTriggerRedshiftStatementsFinishedLambda",
            rule_name="redshift-statements-finished-rule",  # hard coded
            event_bus=None,  # Redshift update messages go to "default" bus
        )

        # will be used once in Trigger defined below
        self.configure_redshift_table_lambda = _lambda.Function(
            self,  # create the schema and table in Redshift
            "ConfigureRedshiftTable",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/configure_redshift_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(30),  # depends on number of tables
            memory_size=128,
            environment={
                "DETAILS_ON_TOPICS": json.dumps(
                    environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]
                ),
                "REDSHIFT_CLUSTER_NAME": environment["SHARED_STACK_VARS"][
                    "REDSHIFT_CLUSTER_NAME"
                ],
                "REDSHIFT_SECRET_NAME": environment["SHARED_STACK_VARS"][
                    "REDSHIFT_SECRET_NAME"
                ],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.move_s3_files_to_processing_folder_lambda = _lambda.Function(
            self,
            "MoveS3FilesToProcessingFolder",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/move_s3_files_to_processing_folder_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(5),  # depends on number of files to move
            memory_size=128,
            environment={
                "S3_BUCKET_NAME": environment["UPGRADE_STACK_VARS"]["S3_BUCKET_NAME"],
                "S3_BUCKET_PREFIX_FOR_FIREHOSE": environment["UPGRADE_STACK_VARS"][
                    "S3_BUCKET_PREFIX_FOR_FIREHOSE"
                ],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.truncate_and_load_redshift_table_lambda = _lambda.Function(
            self,
            "TruncateAndLoadRedshiftTable",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/truncate_and_load_redshift_table_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(3),  # pretty fast
            memory_size=128,
            environment={
                "DYNAMODB_TABLE_NAME": environment["UPGRADE_STACK_VARS"][
                    "DYNAMODB_TABLE_NAME"
                ],
                "DYNAMODB_TTL_IN_DAYS": json.dumps(
                    environment["UPGRADE_STACK_VARS"]["DYNAMODB_TTL_IN_DAYS"]
                ),
                "REDSHIFT_CLUSTER_NAME": environment["SHARED_STACK_VARS"][
                    "REDSHIFT_CLUSTER_NAME"
                ],
                "REDSHIFT_ROLE_NAME": environment["SHARED_STACK_VARS"][
                    "REDSHIFT_ROLE_NAME"
                ],
                "REDSHIFT_SECRET_NAME": environment["SHARED_STACK_VARS"][
                    "REDSHIFT_SECRET_NAME"
                ],
                "S3_BUCKET_NAME": environment["UPGRADE_STACK_VARS"]["S3_BUCKET_NAME"],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )
        self.redshift_statements_finished_lambda = _lambda.Function(
            self,
            "RedshiftStatementsFinished",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset(
                "lambda_code/redshift_statements_finished_lambda",
                exclude=[".venv/*"],
            ),
            handler="handler.lambda_handler",
            timeout=Duration.seconds(5),  # depends on number of files to move
            memory_size=128,
            environment={
                "DYNAMODB_TABLE_NAME": environment["UPGRADE_STACK_VARS"][
                    "DYNAMODB_TABLE_NAME"
                ],
                "S3_BUCKET_NAME": environment["UPGRADE_STACK_VARS"]["S3_BUCKET_NAME"],
            },
            role=self.lambda_redshift_access_role,
            retry_attempts=0,
        )

        # Step Function definition
        move_s3_files_to_processing_folder = sfn_tasks.LambdaInvoke(
            self,
            "move_s3_files_to_processing_folder",
            lambda_function=self.move_s3_files_to_processing_folder_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "Execution.$": "$$.Execution.Name",
                    "eventbridge_payload.$": "$",
                }
            ),
            payload_response_only=True,
            task_timeout=sfn.Timeout.duration(
                self.move_s3_files_to_processing_folder_lambda.timeout
            ),
            retry_on_service_exceptions=False,
        )
        truncate_and_load_redshift_table = sfn_tasks.LambdaInvoke(
            self,
            "truncate_and_load_redshift_table",
            lambda_function=self.truncate_and_load_redshift_table_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object(
                {
                    "redshift_manifest_file_name.$": "$.redshift_manifest_file_name",
                    "task_token": sfn.JsonPath.task_token,
                    "eventbridge_payload.$": "$.eventbridge_payload",
                }
            ),
            task_timeout=sfn.Timeout.at(path="$.redshift_load_every_x_seconds"),
            retry_on_service_exceptions=False,
        )
        empty_manifest_file = sfn.Succeed(self, "empty_manifest_file")
        self.state_machine = sfn.StateMachine(
            self,
            "load_redshift_table",
            state_machine_name="load_redshift_steps",  # hard coded
            definition=move_s3_files_to_processing_folder.next(
                sfn.Choice(self, "non-empty_manifest_file?")
                .when(
                    sfn.Condition.is_present(variable="$.redshift_manifest_file_name"),
                    truncate_and_load_redshift_table,
                )
                .otherwise(empty_manifest_file)
            ),
            # role=self.eventbridge_sfn_role,  # somehow creates circular dependency
        )

        # instantiating AWS resources per SNS topic
        self.sns_topics = {}
        self.scheduled_eventbridge_rules = {}
        for topic_details in environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]:
            sns_topic_name = topic_details["SNS_TOPIC_NAME"]
            sns_topic = sns.Topic.from_topic_arn(
                self,
                f"SnsTopic--{sns_topic_name}",
                topic_arn=(
                    f"arn:aws:sns:{environment['SHARED_STACK_VARS']['AWS_REGION']}:"
                    f"{environment['SHARED_STACK_VARS']['AWS_ACCOUNT']}:{sns_topic_name}"
                ),
            )
            self.sns_topics[sns_topic_name] = sns_topic

            scheduled_eventbridge_rule = events.Rule(
                self,
                f"RunPeriodicallyForSnsTopic--{sns_topic_name}",
                rule_name=f"load-to-redshift-for-topic--{sns_topic_name}",
                event_bus=None,  # scheduled events must be on "default" bus
                schedule=events.Schedule.rate(
                    Duration.minutes(topic_details["REDSHIFT_LOAD_EVERY_X_MINUTES"])
                ),
            )
            self.scheduled_eventbridge_rules[
                sns_topic_name
            ] = scheduled_eventbridge_rule
        assert (
            len(environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"])
            == len(self.sns_topics)
            == len(self.scheduled_eventbridge_rules)
        )

        # connect the AWS resources
        self.trigger_configure_redshift_table_lambda = triggers.Trigger(
            self,
            "TriggerConfigureRedshiftTableLambda",
            handler=self.configure_redshift_table_lambda,  # this is underlying Lambda
            # runs once before Redshift loads are triggered by Eventbridge
            execute_before=list(self.scheduled_eventbridge_rules.values()),
            # execute_before=list(self.scheduled_eventbridge_schedules.values()),
            # execute_on_handler_change=True,
            # invocation_type=triggers.InvocationType.REQUEST_RESPONSE,
            # timeout=self.configure_redshift_table_lambda.timeout,
        )
        self.event_rule_to_trigger_redshift_statements_finished_lambda.add_event_pattern(
            source=["aws.redshift-data"],
            detail_type=["Redshift Data Statement Status Change"],
            detail={
                "principal": [
                    {
                        "suffix": self.truncate_and_load_redshift_table_lambda.function_name
                    }
                ],
                "statementId": [{"exists": True}],
                "state": [{"exists": True}],
            },
            resources=events.Match.suffix(
                environment["SHARED_STACK_VARS"]["REDSHIFT_CLUSTER_NAME"]
            ),
            account=[environment["SHARED_STACK_VARS"]["AWS_ACCOUNT"]],
        )
        self.event_rule_to_trigger_redshift_statements_finished_lambda.add_target(
            events_targets.LambdaFunction(
                handler=self.redshift_statements_finished_lambda,
                # dead_letter_queue=None,
            )
        )

        # instantiating/connecting AWS resources per SNS topic
        self.firehoses_with_s3_target = {}
        self.firehose_subscriptions = {}
        for topic_details in environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]:
            sns_topic_name = topic_details["SNS_TOPIC_NAME"]
            firehose_name = f"firehose-to-s3-for-topic--{sns_topic_name}"
            validate_and_transform_message = topic_details[
                "VALIDATE_AND_TRANSFORM_MESSAGE"
            ]
            if validate_and_transform_message:
                validate_and_transform_message_lambda = _lambda.Function(
                    self,
                    f"ValidateAndTransformMessage--{firehose_name}",
                    function_name=f"validate_and_transform_message_for_topic--{sns_topic_name}",
                    runtime=_lambda.Runtime.PYTHON_3_9,
                    code=_lambda.Code.from_asset(
                        "lambda_code/validate_and_transform_message_lambda",
                        exclude=[".venv/*"],
                    ),
                    handler="handler.lambda_handler",
                    timeout=Duration.seconds(3),  # should be instantaneous
                    memory_size=128,
                    environment={"SNS_TOPIC_NAME": sns_topic_name},
                    role=self.firehose_write_to_s3_role,
                    retry_attempts=0,
                )
                processor = firehose.CfnDeliveryStream.ProcessorProperty(
                    type="Lambda",
                    # the properties below are optional
                    parameters=[
                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="RoleArn",
                            parameter_value=self.firehose_write_to_s3_role.role_arn,  # connect AWS resource
                        ),
                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="LambdaArn",  # there are also "Delimiter" and "NumberOfRetries"
                            parameter_value=validate_and_transform_message_lambda.function_arn,  # connect AWS resource
                        ),
                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="BufferSizeInMBs",
                            parameter_value=str(
                                environment["UPGRADE_STACK_VARS"][
                                    "FIREHOSE_BUFFER_SIZE_IN_MBS"
                                ]
                            ),
                        ),
                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="BufferIntervalInSeconds",
                            parameter_value=str(
                                environment["UPGRADE_STACK_VARS"][
                                    "FIREHOSE_BUFFER_INTERVAL_IN_SECONDS"
                                ]
                            ),
                        ),
                    ],
                )
            extended_s3_destination_configuration = firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=self.s3_bucket_for_sns_messages.bucket_arn,  # connect AWS resource
                role_arn=self.firehose_write_to_s3_role.role_arn,  # connect AWS resource
                # the properties below are optional
                prefix=environment["UPGRADE_STACK_VARS"][
                    "S3_BUCKET_PREFIX_FOR_FIREHOSE"
                ].format(SNS_TOPIC_NAME=sns_topic_name),
                error_output_prefix=f"error/topic={sns_topic_name}/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=environment["UPGRADE_STACK_VARS"][
                        "FIREHOSE_BUFFER_INTERVAL_IN_SECONDS"
                    ],
                    size_in_m_bs=environment["UPGRADE_STACK_VARS"][
                        "FIREHOSE_BUFFER_SIZE_IN_MBS"
                    ],
                ),
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=f"/aws/kinesisfirehose/{firehose_name}",  # hard coded
                    log_stream_name="DestinationDelivery",  # hard coded  ### currently doesn't work
                ),
                processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=validate_and_transform_message,
                    processors=[processor] if validate_and_transform_message else [],
                ),
                # compression_format="compressionFormat",
                # s3_backup_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(...),
                # s3_backup_mode="s3BackupMode",
            )
            firehose_with_s3_target = firehose.CfnDeliveryStream(
                self,
                f"FirehoseToS3ForTopic--{sns_topic_name}",
                extended_s3_destination_configuration=extended_s3_destination_configuration,
                delivery_stream_name=firehose_name,
            )
            self.firehoses_with_s3_target[sns_topic_name] = firehose_with_s3_target
            firehose_subscription = sns.Subscription(
                self,
                f"FirehoseSubscriptionForTopic--{sns_topic_name}",
                topic=self.sns_topics[sns_topic_name],
                endpoint=firehose_with_s3_target.attr_arn,
                protocol=sns.SubscriptionProtocol.FIREHOSE,
                subscription_role_arn=self.sns_write_to_firehose_role.role_arn,
                raw_message_delivery=True,
                # dead_letter_queue=None,
            )
            self.firehose_subscriptions[sns_topic_name] = firehose_subscription

            self.scheduled_eventbridge_rules[sns_topic_name].add_target(
                target=events_targets.SfnStateMachine(
                    self.state_machine,
                    input=events.RuleTargetInput.from_object(topic_details),
                    role=self.eventbridge_sfn_role,
                    # dead_letter_queue=None,
                )
            )
        assert (
            len(environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"])
            == len(self.firehoses_with_s3_target)
            == len(self.firehose_subscriptions)
        )


class SnsToRedshiftStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        kwargs.pop("env")  # NestedStack does not have `env` argument
        self.preexisting_stack = PreexistingStack(
            self, "PreexistingStack", environment=environment, **kwargs
        )
        self.upgrade_stack = UpgradeStack(
            self,
            "UpgradeStack",
            environment=environment,
            **kwargs,
        )
        self.upgrade_stack.node.add_dependency(
            self.preexisting_stack
        )  # preexisting stack is deployed first

        # write Cloudformation Outputs per SNS topic
        self.outputs = defaultdict(dict)
        for idx, topic_details in enumerate(
            environment["SHARED_STACK_VARS"]["DETAILS_ON_TOPICS"]
        ):
            sns_topic_name = topic_details["SNS_TOPIC_NAME"]
            self.outputs[sns_topic_name]["sns_topic_arn"] = CfnOutput(
                self,
                f"SnsTopicArn{idx}",  # Output omits underscores and hyphens
                value=self.preexisting_stack.sns_topics[sns_topic_name].topic_arn,
            )
            firehose_with_s3_target = self.upgrade_stack.firehoses_with_s3_target[
                sns_topic_name
            ]
            self.outputs[sns_topic_name]["firehose_arn"] = CfnOutput(
                self,
                f"FirehoseArn{idx}",  # Output omits underscores and hyphens
                value=firehose_with_s3_target.attr_arn,
            )
            self.outputs[sns_topic_name]["s3_bucket_prefix"] = CfnOutput(
                self,
                f"FirehoseS3BucketPrefix{idx}",  # Output omits underscores and hyphens
                value=firehose_with_s3_target.extended_s3_destination_configuration.prefix,
            )
            self.outputs[sns_topic_name]["s3_bucket_prefix"] = CfnOutput(
                self,
                f"FirehoseS3BucketErrorOutputPrefix{idx}",  # Output omits underscores and hyphens
                value=firehose_with_s3_target.extended_s3_destination_configuration.error_output_prefix,
            )
