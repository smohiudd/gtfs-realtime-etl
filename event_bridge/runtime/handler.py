import logging
import os

import boto3
from botocore.exceptions import ClientError

import requests
from google.transit import gtfs_realtime_pb2
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import geoarrow.pyarrow as ga
from geoarrow.pyarrow import io

import datetime as dt

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3", region_name="us-west-2") 
logger.info(f"loaded s3 client")

def handler(event, context):
    """
    This saves GTFS vehicle position data to S3 bucket
    """

    position_url = os.environ.get("VEH_POSITION_URL")
    timezone = os.environ.get("TIMEZONE")
    destination_bucket = os.environ.get("DESTINATION_BUCKET")

    feed = gtfs_realtime_pb2.FeedMessage()
    try:
        response = requests.get(position_url)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.exception("Failed to fetch vehicle positions from %s", position_url)
        raise
    
    feed.ParseFromString(response.content)

    records = [
        (
            i.vehicle.trip.trip_id,
            dt.datetime.fromtimestamp(
                i.vehicle.timestamp, tz=ZoneInfo(timezone)
            ).isoformat(),
            i.vehicle.position.longitude,
            i.vehicle.position.latitude,
        )
        for i in feed.entity
    ]
    
    logger.info(f"Discovered {len(records)} vehicle position records")
    
    columns = ['trip_id', 'datetime', 'lon', 'lat']
    df = pd.DataFrame(records, columns=columns)
    
    df['geometry_wkt'] = df.apply(lambda x: f"POINT ({x['lon']} {x['lat']})",axis=1)

    pa_table = pa.Table.from_pandas(df, preserve_index=False)
    geom_array = ga.as_geoarrow(ga.with_crs(pa_table["geometry_wkt"].combine_chunks(), ga.OGC_CRS84))
    
    pa_table=pa_table.append_column("geometry", geom_array)
    pa_table = pa_table.drop_columns(['lon', 'lat','geometry_wkt'])
    
    output_file = "/tmp/positions.parquet"

    io.write_geoparquet_table(
        pa_table, 
        output_file,
        geometry_encoding='point', 
        primary_geometry_column='geometry'
    )
    
    logger.info(f"saved parquet file to {output_file}")

    latest_timestamp = dt.datetime.now(tz=ZoneInfo(timezone))
    object_key = f"positions/{latest_timestamp.strftime('%Y')}/{latest_timestamp.strftime('%m')}/{latest_timestamp.strftime('%d')}/{latest_timestamp.strftime('%H%M%S')}.parquet"
    
    try:
        logger.info("Uploading %s to bucket %s", object_key, destination_bucket)
        s3.upload_file(output_file, destination_bucket, object_key)
    except ClientError as e:
        logging.error(e)

