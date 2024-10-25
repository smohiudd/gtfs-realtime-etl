"""
CDK construct for gtfs-realtime-etl Eventbridge scheduler.
"""

import os

from aws_cdk import (
    CfnOutput,
    Duration,
    Stack,
    aws_ec2,
    aws_lambda,
    aws_logs,
    aws_scheduler_alpha,
    aws_scheduler_targets_alpha,
    aws_sqs,
)
from constructs import Construct


class EventBridgeConstruct(Construct):
    """CDK construct for gtfs-realtime-etl EventBridge Scheduler."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        database,
        vpc,
        code_dir: str = "./",
    ) -> None:
        """Initialized construct."""
        super().__init__(scope, construct_id)
        stack_name = Stack.of(self).stack_name

        lambda_env = {
            "SECRET_NAME": database.postgis.secret.secret_name,
        }

        lambda_function = aws_lambda.Function(
            self,
            "lambda",
            handler="handler.handler",
            runtime=aws_lambda.Runtime.PYTHON_3_10,
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(code_dir),
                file="event_bridge/runtime/Dockerfile",
            ),
            vpc=vpc,
            environment=lambda_env,
            allow_public_subnet=True,
            log_retention=aws_logs.RetentionDays.ONE_WEEK,
            tracing=aws_lambda.Tracing.ACTIVE,
        )

        database.postgis.secret.grant_read(lambda_function)
        database.postgis.connections.allow_from(
            lambda_function, port_range=aws_ec2.Port.tcp(5432)
        )

        dlq = aws_sqs.Queue(self, "DLQ", queue_name="gtfs-realtime-etl-dlq")

        target = aws_scheduler_targets_alpha.LambdaInvoke(
            lambda_function,
            dead_letter_queue=dlq,
            max_event_age=Duration.minutes(15),
            retry_attempts=1,
        )

        aws_scheduler_alpha.Schedule(
            self,
            "Schedule",
            schedule=aws_scheduler_alpha.ScheduleExpression.rate(Duration.minutes(240)),
            target=target,
        )

        # CfnOutput(
        #     self,
        #     "eventbridge-lambda",
        #     value=self.lambda_function.arn,
        # )
