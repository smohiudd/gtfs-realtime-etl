from aws_cdk import (
    App,
    Stack,
)
from constructs import Construct

from config import gtfs_app_settings
from event_bridge.infrastructure.construct import EventBridgeConstruct
from network.infrastructure.construct import VpcConstruct

app = App()

class GTFSNetworkStack(Stack):
    """CDK stack for the gtfs-realtime-etl network stack."""

    def __init__(self, scope: Construct, construct_id: str, stage: str, **kwargs) -> None:
        """."""
        super().__init__(scope, construct_id, **kwargs)
    
        network = VpcConstruct(
            self,
            "network",
            stage=stage,
        )
        
GTFSNetworkStack(
    app,
    "vpc-network",
    stack_name="gtfs-etl-vpc-network",
    stage=gtfs_app_settings.stage_name,
    env=gtfs_app_settings.cdk_env(),
)



class GTFSStack(Stack):
    """CDK stack for the gtfs-realtime-etl stack."""

    def __init__(self, scope: Construct, construct_id: str, vpc_id: str, stage: str, region: str, **kwargs) -> None:
        """."""
        super().__init__(scope, construct_id, **kwargs)
        
        gtfs_etl = EventBridgeConstruct(
            self,
            "gtfs-etl",
            stage=stage,
            region=region,
            vpc=vpc_id,
        )

GTFSStack(
    app,
    "gtfs-etl",
    stack_name=gtfs_app_settings.app_name,
    env=gtfs_app_settings.cdk_env(),
    vpc_id=gtfs_app_settings.vpc_id,
    stage=gtfs_app_settings.stage_name,
    region=gtfs_app_settings.region,
)

app.synth()
