from aws_cdk import (
    App,
    Stack,
    Tags,
)
from constructs import Construct

from config import gtfs_app_settings
from etl.infrastructure.construct import EventBridgeConstruct
from compaction.infrastructure.construct import CompactionConstruct
from network.infrastructure.construct import VpcConstruct

app = App()


class GTFSETLStack(Stack):
    """CDK stack for the gtfs-realtime-etl etl stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """."""
        super().__init__(scope, construct_id, **kwargs)


class VPCStack(Stack):
    """CDK stack for the gtfs-realtime-etl vpc stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """."""
        super().__init__(scope, construct_id, **kwargs)

gtfs_etl_stack = GTFSETLStack(
    app,
    f"{gtfs_app_settings.app_name}-{gtfs_app_settings.stage}",
    env=gtfs_app_settings.cdk_env(),
)

vpc_stack = VPCStack(
    app,
    "gtfs-etl-vpc",
    env=gtfs_app_settings.cdk_env(),
)

vpc = VpcConstruct(
    vpc_stack,
    "network",
    vpc_id=gtfs_app_settings.vpc_id,
)

gtfs_etl = EventBridgeConstruct(
    gtfs_etl_stack,
    "gtfs-etl",
    vpc_id=gtfs_app_settings.vpc_id if gtfs_app_settings.vpc_id else None,
    stage=gtfs_app_settings.stage,
)

compaction = CompactionConstruct(
    gtfs_etl_stack,
    "compaction",
    vpc_id=gtfs_app_settings.vpc_id if gtfs_app_settings.vpc_id else None,
    stage=gtfs_app_settings.stage,
)

for key, value in {
    "Project": gtfs_app_settings.app_name,
    "Stack": gtfs_app_settings.stage_name(),
}.items():
    if value:
        Tags.of(app).add(key=key, value=value)

app.synth()
