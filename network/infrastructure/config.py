"""Configuration options for the VPC."""

from pydantic_settings import BaseSettings


# https://medium.com/aws-activate-startup-blog/practical-vpc-design-8412e1a18dcc#.bmeh8m3si
# https://www.admin-magazine.com/Articles/The-AWS-CDK-for-software-defined-deployments/(offset)/6
class VpcSettings(BaseSettings):
    """VPC settings"""

    cidr: str = "10.100.0.0/16"
    max_azs: int = 2
    nat_gateways: int = 1
    public_mask: int = 24
    private_mask: int = 24


vpc_settings = VpcSettings()
