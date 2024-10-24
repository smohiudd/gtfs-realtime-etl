from aws_cdk import (
    App,
    Stack,
)
from constructs import Construct

from config import gtfs_app_settings
from database.infrastructure.construct import GTFSRdsConstruct
from network.infrastructure.construct import VpcConstruct

app = App()


class GTFSStack(Stack):
    """CDK stack for the gtfs-realtime-etl stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        """."""
        super().__init__(scope, construct_id, **kwargs)


gtfs_stack = GTFSStack(
    app,
    f"{gtfs_app_settings.app_name}-{gtfs_app_settings.stage_name()}",
    env=gtfs_app_settings.cdk_env(),
)

if gtfs_app_settings.vpc_id:
    vpc = VpcConstruct(
        gtfs_stack,
        "network",
        vpc_id=gtfs_app_settings.vpc_id,
        stage=gtfs_app_settings.stage_name(),
    )
else:
    vpc = VpcConstruct(gtfs_stack, "network", stage=gtfs_app_settings.stage_name())


features_database = GTFSRdsConstruct(
    gtfs_stack,
    "gtfs-database",
    vpc=vpc.vpc,
    subnet_ids=gtfs_app_settings.subnet_ids,
    stage=gtfs_app_settings.stage_name(),
)

app.synth()
