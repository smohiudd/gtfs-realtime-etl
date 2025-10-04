"""gtfs-realtime-etl compaction construct configuration."""

from pydantic import Field
from pydantic_settings import BaseSettings


class CompactionSettings(BaseSettings):
    """Application settings."""

    destination_bucket: str = Field(
        None,
        description="S3 bucket to upload the realtime data to",
    )

    previous_days: int = Field(
        1,
        description="Number of days to compact",
    )
    
    timezone: str = Field(
        "America/Edmonton",
        description="IANA time zone name. https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab",
    )

    class Config:
        """model config."""

        env_file = ".env"
        env_prefix = "GTFS_RT_EVENT_"
        extra = "allow"


compaction_settings = CompactionSettings()
