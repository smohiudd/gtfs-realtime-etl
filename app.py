from aws_cdk import (
    App,
    Stack,
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


gtfs_etl_stack = GTFSETLStack(
    app,
    f"{gtfs_app_settings.app_name}",
    env=gtfs_app_settings.cdk_env(),
)


vpc = VpcConstruct(
    gtfs_etl_stack,
    "network",
    vpc_id=gtfs_app_settings.vpc_id,
)

gtfs_etl = EventBridgeConstruct(
    gtfs_etl_stack,
    "gtfs-etl",
    vpc=vpc.vpc,
)

compaction = CompactionConstruct(
    gtfs_etl_stack,
    "compaction",
    vpc=vpc.vpc,
)

app.synth()
