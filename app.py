from aws_cdk import (
    App,
    Stack,
    Tags,
)
from constructs import Construct

from config import gtfsAppSettings
from etl.infrastructure.construct import EventBridgeConstruct
from compaction.infrastructure.construct import CompactionConstruct
from network.infrastructure.construct import VpcConstruct

app = App()
env_file = app.node.try_get_context("env_file")
if env_file:
    settings = gtfsAppSettings(_env_file=f"envs/{env_file}.env")
else:  
    settings = gtfsAppSettings()

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
    f"{settings.app_name}-{settings.stage}",
    env=settings.cdk_env(),
)

vpc_stack = VPCStack(
    app,
    "gtfs-etl-vpc",
    env=settings.cdk_env(),
)

vpc = VpcConstruct(
    vpc_stack,
    "network",
    vpc_id=settings.vpc_id,
)

gtfs_etl = EventBridgeConstruct(
    gtfs_etl_stack,
    "gtfs-etl",
    vpc_id=settings.vpc_id if settings.vpc_id else None,
    stage=settings.stage,
)

compaction = CompactionConstruct(
    gtfs_etl_stack,
    "compaction",
    vpc_id=settings.vpc_id if settings.vpc_id else None,
    stage=settings.stage,
)

for key, value in {
    "Project": settings.app_name,
    "Stack": settings.stage_name(),
}.items():
    if value:
        Tags.of(app).add(key=key, value=value)

app.synth()
