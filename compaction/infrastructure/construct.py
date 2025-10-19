"""
CDK construct for gtfs-realtime-etl compaction.

https://github.com/aws-samples/s3-small-object-compaction
"""

import os

from aws_cdk import (
    Duration,
    Size,
    TimeZone,
    aws_lambda,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs,
    aws_ec2,
    aws_scheduler,
    aws_scheduler_targets,
)
from constructs import Construct

from .config import CompactionSettings


class CompactionConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc_id: str | None,
        stage: str,
        code_dir: str = "./",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        env_file = self.node.try_get_context("env_file")
        if env_file:
            compaction_settings = CompactionSettings(_env_file=f"envs/{env_file}.env")
        else:
            compaction_settings = CompactionSettings()

        destination_s3_bucket = s3.Bucket.from_bucket_name(
            self, "DestinationBucket", compaction_settings.destination_bucket
        )
        
        if vpc_id:
            vpc = aws_ec2.Vpc.from_lookup(
                self,
                "VPC",
                vpc_id=vpc_id,
            )

        compactionFunction = aws_lambda.Function(
            self,
            "CompactFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(code_dir),
                file="compaction/runtime/Dockerfile",
            ),
            vpc=vpc if vpc_id else None,
            handler="handler.handler",
            timeout=Duration.minutes(15),
            ephemeral_storage_size=Size.mebibytes(2048),
            memory_size=2048,
            tracing=aws_lambda.Tracing.ACTIVE,
            log_retention=aws_logs.RetentionDays.ONE_MONTH,
        )

        compactionFunction.grant_invoke(iam.ServicePrincipal("events.amazonaws.com"))

        compactionFunction.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:GetObject", "s3:ListBucket", "s3:PutObject"],
                resources=[
                    destination_s3_bucket.bucket_arn,
                    destination_s3_bucket.bucket_arn + "/*",
                ],
            )
        )
        destination_s3_bucket.grant_read(compactionFunction)
        destination_s3_bucket.grant_write(compactionFunction)
        
        target_daily = aws_scheduler_targets.LambdaInvoke(
            compactionFunction,
            input=aws_scheduler.ScheduleTargetInput.from_object({
                "s3_bucket": compaction_settings.destination_bucket,
                "previous_days": int(compaction_settings.previous_days),
                "timezone": compaction_settings.timezone,
                "stage": stage,
            }),
            max_event_age=Duration.minutes(15),
            retry_attempts=0,
        )
        
        aws_scheduler.Schedule(
            self,
            "DailySchedule",
            schedule=aws_scheduler.ScheduleExpression.cron(
                time_zone=TimeZone.of(compaction_settings.timezone),
                day="*",
                hour="1",
                minute="0",
            ),
            target=target_daily,
        )
        
        target_monthly = aws_scheduler_targets.LambdaInvoke(
            compactionFunction,
            input=aws_scheduler.ScheduleTargetInput.from_object({
                "s3_bucket": compaction_settings.destination_bucket,
                "previous_months": int(compaction_settings.previous_months),
                "timezone": compaction_settings.timezone,
                "compact_to_now": False,
                "stage": stage,
            }),
            max_event_age=Duration.minutes(15),
            retry_attempts=0,
        )
        
        aws_scheduler.Schedule(
            self,
            "MonthlySchedule",
            schedule=aws_scheduler.ScheduleExpression.cron(
                time_zone=TimeZone.of(compaction_settings.timezone),
                day="1",
                hour="1",
                minute="0",
                month="*",
            ),
            target=target_monthly,
        )
