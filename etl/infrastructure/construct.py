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
    aws_ec2
)
from constructs import Construct

from .config import etl_settings

class EventBridgeConstruct(Construct):
    """CDK construct for gtfs-realtime-etl EventBridge Scheduler."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: aws_ec2.Vpc,
        code_dir: str = "./",
    ) -> None:
        """Initialized construct."""
        super().__init__(scope, construct_id)

        lambda_env = {
            "VEH_POSITION_URL": etl_settings.veh_position_url,
            "TIMEZONE": etl_settings.timezone,
            "DESTINATION_BUCKET": etl_settings.destination_bucket,
        }
        
        destination_bucket = aws_s3.Bucket.from_bucket_name(
            self, "DestinationBucket", 
            etl_settings.destination_bucket
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
            vpc=vpc,
            environment=lambda_env,
            allow_public_subnet=True,
            log_retention=aws_logs.RetentionDays.ONE_WEEK,
            tracing=aws_lambda.Tracing.ACTIVE,
            memory_size=1024,
            timeout=Duration.minutes(1),
        )
        
        lambda_function.add_to_role_policy(aws_iam.PolicyStatement(
            sid="AllowLambdaToWriteToS3",
            actions=["s3:PutObject"],
            resources=[destination_bucket.arn_for_objects("*"), destination_bucket.bucket_arn],
            effect=aws_iam.Effect.ALLOW,
        ))

        destination_bucket.grant_write(lambda_function)
    
        dlq = aws_sqs.Queue(self, "DLQ", queue_name="gtfs-realtime-etl-dlq")

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
                Duration.minutes(etl_settings.schedule_mins)
            ),
            target=target,
        )
