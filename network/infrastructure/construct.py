"""
CDK construct for gtfs-realtime-etl VPC.
"""

from typing import Optional

from aws_cdk import CfnOutput, Stack, aws_ec2
from constructs import Construct

from .config import vpc_settings


# https://github.com/aws-samples/aws-cdk-examples/tree/master/python/new-vpc-alb-asg-mysql
# https://github.com/aws-samples/aws-cdk-examples/tree/master/python/docker-app-with-asg-alb
class VpcConstruct(Construct):
    """CDK construct for gtfs-realtime-etl VPC."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        vpc_id: Optional[str] = None,
    ) -> None:
        """Initialized construct."""
        super().__init__(scope, construct_id)
        stack_name = Stack.of(self).stack_name

        # Get existing VPC if provided
        if vpc_id:
            self.vpc = aws_ec2.Vpc.from_lookup(
                self,
                "vpc",
                vpc_id=vpc_id,
            )
        # Or create a new VPC using the deployment stage configuration
        else:
            # Union of pydantic base settings is unpredictable so set stage settings conditionally
            gtfs_vpc_settings = vpc_settings

            public_subnet = aws_ec2.SubnetConfiguration(
                name="public",
                subnet_type=aws_ec2.SubnetType.PUBLIC,
                cidr_mask=gtfs_vpc_settings.public_mask,
            )
            private_subnet = aws_ec2.SubnetConfiguration(
                name="private",
                subnet_type=aws_ec2.SubnetType.PRIVATE_WITH_EGRESS,
                cidr_mask=gtfs_vpc_settings.private_mask,
            )

            self.vpc = aws_ec2.Vpc(
                self,
                "vpc",
                max_azs=gtfs_vpc_settings.max_azs,
                cidr=gtfs_vpc_settings.cidr,
                subnet_configuration=[public_subnet, private_subnet],
                nat_gateways=gtfs_vpc_settings.nat_gateways,
            )

            vpc_endpoints = {
                "secretsmanager": aws_ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
                "cloudwatch-logs": aws_ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
                "s3": aws_ec2.GatewayVpcEndpointAwsService.S3,
            }

            for id, service in vpc_endpoints.items():
                if isinstance(service, aws_ec2.InterfaceVpcEndpointAwsService):
                    self.vpc.add_interface_endpoint(id, service=service)
                elif isinstance(service, aws_ec2.GatewayVpcEndpointAwsService):
                    self.vpc.add_gateway_endpoint(id, service=service)

        CfnOutput(
            self,
            "vpc-id",
            value=self.vpc.vpc_id,
            export_name=f"{stack_name}-vpc-id",
        )
