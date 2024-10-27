"""gtfs-realtime-etl event bridge construct configuration."""

from pydantic import Field
from pydantic_settings import BaseSettings


class EventBridgeSettings(BaseSettings):
    """Application settings."""

    veh_position_url: str = Field(
        None,
        description="GTFS realtime vehicle position feed url",
    )

    schedule_mins: int = Field(
        240,
        description="How often the event is scheduled",
    )

    timezone: int = Field(
        "America/Edmonton",
        description="IANA time zone name. https://data.iana.org/time-zones/tzdb-2021a/zone1970.tab",
    )

    class Config:
        """model config."""

        env_file = ".env"
        env_prefix = "GTFS_RT_EVENT_"
        extra = "allow"


event_bridge_settings = EventBridgeSettings()
