"""gtfs-realtime-etl event bridge construct configuration."""

from pydantic import Field
from pydantic_settings import BaseSettings


class ETLSettings(BaseSettings):
    """Application settings."""

    veh_position_url: str = Field(
        None,
        description="GTFS realtime vehicle position feed url",
    )

    schedule_seconds: int = Field(
        60,
        description="How often the event is scheduled",
    )

    timezone: str = Field(
        "America/Edmonton",
        description="IANA time zone name. https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab",
    )

    destination_bucket: str = Field(
        None,
        description="S3 bucket to upload the realtime data to",
    )

    class Config:
        """model config."""

        env_file = ".env"
        env_prefix = "GTFS_RT_EVENT_"
        extra = "allow"


etl_settings = ETLSettings()
