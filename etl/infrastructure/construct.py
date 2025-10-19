"""
CDK construct for gtfs-realtime-etl Eventbridge scheduler.
"""

import os

from aws_cdk import (
    Duration,
    aws_s3,
    aws_lambda,
    aws_logs,
    aws_scheduler_alpha,
    aws_scheduler_targets_alpha,
    aws_iam,
    aws_sqs,
    aws_ec2,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as stepfunction_tasks,
    RemovalPolicy,
)
from constructs import Construct

from .config import ETLSettings


class SubMinuteStepFunctionConstruct(Construct):
    """CDK construct for gtfs-realtime-etl Sub Minute Lambda Trigger.

    Sub Minute Lambda Trigger
    https://github.com/MauriceBrg/snippets/blob/main/sub-minute-lambda-trigger/sub_minute_lambda_trigger/infrastructure.py

    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        lambda_function: aws_lambda.Function,
        interval: int,
    ) -> None:
        super().__init__(scope, construct_id)

        seconds_per_minute = 60
        assert seconds_per_minute % interval == 0, (
            "A minute has to be evenly divisible by interval! (60 mod interval = 0)"
        )

        invocations_per_minute = int(seconds_per_minute / interval)
        wait_state_duration = interval

        wait_time_list = [
            wait_state_duration if i < (invocations_per_minute - 1) else 0
            for i in range(invocations_per_minute)
        ]

        state_machine_definition = stepfunctions.Pass(
            scope=self,
            id="wait-time-list",
            comment="Sets up the number of invocations per minute",
            result=stepfunctions.Result(value={"iterator": wait_time_list}),
        )

        invoke_loop = stepfunctions.Map(
            scope=self,
            id="invoke-loop",
            comment="Invokes function and waits",
            max_concurrency=1,
            items_path="$.iterator",
        )

        invoke_loop.item_processor(
            stepfunction_tasks.CallAwsService(
                self,
                "invoke-lambda-async",
                action="invoke",
                service="lambda",
                iam_action="lambda:InvokeFunction",
                iam_resources=[lambda_function.function_arn],
                parameters={
                    "FunctionName": lambda_function.function_arn,
                    "InvocationType": "Event",
                },
                result_path=stepfunctions.JsonPath.DISCARD,  # Discard the output and pass on the input
            ).next(
                stepfunctions.Wait(
                    self,
                    "wait-until-next-iteration",
                    time=stepfunctions.WaitTime.seconds_path("$"),
                )
            )
        )

        state_machine_definition.next(invoke_loop)

        self.step_function = stepfunctions.StateMachine(
            self,
            id="sub-minute-trigger",
            definition=state_machine_definition,
            state_machine_name=lambda_function.function_name,
            state_machine_type=stepfunctions.StateMachineType.STANDARD,
            timeout=Duration.seconds(60),
            logs=stepfunctions.LogOptions(
                destination=aws_logs.LogGroup(
                    self,
                    id="log-group-for-trigger",
                    removal_policy=RemovalPolicy.DESTROY,
                    retention=aws_logs.RetentionDays.ONE_DAY,
                )
            ),
        )


class EventBridgeConstruct(Construct):
    """CDK construct for gtfs-realtime-etl EventBridge Scheduler."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc_id: str | None,
        stage: str,
        code_dir: str = "./",
    ) -> None:
        """Initialized construct."""
        super().__init__(scope, construct_id)
        
        env_file = self.node.try_get_context("env_file")
        if env_file:
            etl_settings = ETLSettings(_env_file=f"envs/{env_file}.env")
        else:
            etl_settings = ETLSettings()

        lambda_env = {
            "VEH_POSITION_URL": etl_settings.veh_position_url,
            "TIMEZONE": etl_settings.timezone,
            "DESTINATION_BUCKET": etl_settings.destination_bucket,
            "STAGE": stage,
            "API_KEY": etl_settings.api_key if etl_settings.api_key else "",
            "API_KEY_HEADER": etl_settings.api_key_header if etl_settings.api_key_header else "",
        }

        destination_bucket = aws_s3.Bucket.from_bucket_name(
            self, "DestinationBucket", etl_settings.destination_bucket
        )
        
        if vpc_id:
            vpc = aws_ec2.Vpc.from_lookup(
                self,
                "VPC",
                vpc_id=vpc_id,
            )

        lambda_function = aws_lambda.Function(
            self,
            "lambda",
            handler="handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(code_dir),
                file="etl/runtime/Dockerfile",
            ),
            vpc=vpc if vpc_id else None,
            environment=lambda_env,
            allow_public_subnet=True,
            log_retention=aws_logs.RetentionDays.ONE_DAY,
            memory_size=512,
            timeout=Duration.seconds(10),
            application_log_level_v2=aws_lambda.ApplicationLogLevel.ERROR,
            logging_format=aws_lambda.LoggingFormat.JSON,
        )

        lambda_function.add_to_role_policy(
            aws_iam.PolicyStatement(
                sid="AllowLambdaToWriteToS3",
                actions=["s3:PutObject"],
                resources=[
                    destination_bucket.arn_for_objects("*"),
                    destination_bucket.bucket_arn,
                ],
                effect=aws_iam.Effect.ALLOW,
            )
        )

        destination_bucket.grant_write(lambda_function)

        dlq = aws_sqs.Queue(self, "DLQ", queue_name=f"gtfs-realtime-etl-dlq-{stage}")

        if etl_settings.schedule_seconds < 60:
            step_function = SubMinuteStepFunctionConstruct(
                self,
                "SubMinuteLambdaStepFunction",
                lambda_function,
                etl_settings.schedule_seconds,
            )

            target = aws_scheduler_targets_alpha.StepFunctionsStartExecution(
                state_machine=step_function.step_function,
                dead_letter_queue=dlq,
                max_event_age=Duration.minutes(15),
                retry_attempts=0,
            )
        else:
            target = aws_scheduler_targets_alpha.LambdaInvoke(
                lambda_function,
                dead_letter_queue=dlq,
                max_event_age=Duration.minutes(15),
                retry_attempts=0,
            )

        aws_scheduler_alpha.Schedule(
            self,
            "Schedule",
            schedule=aws_scheduler_alpha.ScheduleExpression.rate(
                Duration.seconds(
                    60
                    if etl_settings.schedule_seconds < 60
                    else etl_settings.schedule_seconds
                )
            ),
            target=target,
        )
