"""
CDK construct for gtfs-realtime-etl compaction.

https://github.com/aws-samples/s3-small-object-compaction
"""

import os

from aws_cdk import (
    Duration,
    Size,
    aws_lambda,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_logs,
    aws_ec2
)
from constructs import Construct

from .config import compaction_settings


class CompactionConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, vpc: aws_ec2.Vpc, code_dir: str = "./", **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        destination_s3_bucket = s3.Bucket.from_bucket_name(
            self, "DestinationBucket", compaction_settings.destination_bucket
        )
        
        compactionFunction = aws_lambda.Function(
            self,
            "standaloneCompactFunction",
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            code=aws_lambda.Code.from_docker_build(
                path=os.path.abspath(code_dir),
                file="compaction/runtime/Dockerfile",
            ),
            vpc=vpc,
            handler="handler.handler",
            timeout=Duration.minutes(15),
            ephemeral_storage_size=Size.mebibytes(2048),
            memory_size=2048,
            tracing=aws_lambda.Tracing.ACTIVE,
            log_retention=aws_logs.RetentionDays.ONE_WEEK,
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

        # Uncomment to add EventBridge rule for standalone Lambda schedule
        lambdaTriggerRule = events.Rule(
            self,
            "compactionRuleStandaloneLambda",
            enabled=False,
            schedule=events.Schedule.rate(
                Duration.days(int(compaction_settings.previous_days))
            ),
        )

        lambdaTriggerRule.add_target(
            targets.LambdaFunction(
                compactionFunction,
                event=events.RuleTargetInput.from_object(
                    {
                        "s3_bucket": compaction_settings.destination_bucket,
                        "duration": int(compaction_settings.previous_days),
                    }
                ),
            )
        )
