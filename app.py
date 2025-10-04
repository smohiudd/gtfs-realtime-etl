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


# class GTFSNetworkStack(Stack):
#     """CDK stack for the gtfs-realtime-etl network stack."""

#     def __init__(self, scope: Construct, construct_id: str, stage: str, vpc_id: str, **kwargs) -> None:
#         """."""
#         super().__init__(scope, construct_id, **kwargs)
    
#         self.network = VpcConstruct(
#             self,
#             "network",
#             stage=stage,
#             vpc_id=vpc_id,
#         )
        
# vpc = GTFSNetworkStack(
#     app,
#     "vpc-network",
#     stack_name="gtfs-etl-vpc-network",
#     stage=gtfs_app_settings.stage_name,
#     vpc_id=gtfs_app_settings.vpc_id,
#     env=Environment(account="539042711016", region="us-west-2"),
# )



# class GTFSStack(Stack):
#     """CDK stack for the gtfs-realtime-etl stack."""

#     def __init__(self, scope: Construct, construct_id: str, vpc, stage: str, region: str, **kwargs) -> None:
#         """."""
#         super().__init__(scope, construct_id, **kwargs)
        
#         gtfs_etl = EventBridgeConstruct(
#             self,
#             "gtfs-etl",
#             stage=stage,
#             region=region,
#             vpc=vpc,
#         )

# GTFSStack(
#     app,
#     "gtfs-etl",
#     stack_name=gtfs_app_settings.app_name,
#     env=Environment(account="539042711016", region="us-west-2"),
#     vpc=vpc.network.vpc,
#     stage=gtfs_app_settings.stage_name,
#     region=gtfs_app_settings.region,
# )


# class CompactionStack(Stack):
#     """CDK stack for the gtfs-realtime-etl compaction stack."""

#     def __init__(self, scope: Construct, construct_id: str, vpc, region: str, **kwargs) -> None:
#         """."""
#         super().__init__(scope, construct_id, **kwargs)

#         compaction = CompactionConstruct(
#             self,
#             "compaction",
#             vpc=vpc,
#             region=region,
#         )

# CompactionStack(
#     app,
#     "compaction",
#     stack_name=f"{gtfs_app_settings.app_name}-compaction",
#     env=Environment(account="539042711016", region="us-west-2"),
#     vpc=vpc.network.vpc,
#     region=gtfs_app_settings.region,
# )

app.synth()
