import logging
import os
import json
import array

import boto3
from botocore.exceptions import ClientError

import requests
from google.transit import gtfs_realtime_pb2
from zoneinfo import ZoneInfo

import pyarrow as pa

import geoarrow.pyarrow as ga

from geoarrow.rust.io import GeoParquetWriter
from geoarrow.rust.io.enums import GeoParquetEncoding

import pygeohash as pgh

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
        response = requests.get(position_url, timeout=5)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.exception("Failed to fetch vehicle positions from %s", position_url)
        raise

    feed.ParseFromString(response.content)

    records = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            records.append(
                {
                    # tripid
                    "trip_id": v.trip.trip_id if v.HasField("trip") else None,
                    "route_id": v.trip.route_id if v.HasField("trip") else None,
                    "direction_id": v.trip.route_id if v.HasField("trip") else None,
                    # vehicle
                    "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
                    # position
                    "latitude": v.position.latitude if v.HasField("position") else None,
                    "longitude": v.position.longitude
                    if v.HasField("position")
                    else None,
                    "bearing": v.position.bearing if v.HasField("position") else None,
                    "speed": v.position.speed if v.HasField("position") else None,
                    # timestamp
                    "timestamp": v.timestamp if v.HasField("timestamp") else None,
                }
            )

    logger.info(f"Discovered {len(records)} vehicle position records")

    schema = pa.schema(
        [
            pa.field("trip_id", pa.string()),
            pa.field("route_id", pa.string()),
            pa.field("direction_id", pa.string()),
            pa.field("vehicle_id", pa.string()),
            pa.field("latitude", pa.float64()),
            pa.field("longitude", pa.float64()),
            pa.field("bearing", pa.float64()),
            pa.field("speed", pa.float64()),
            pa.field("timestamp", pa.int64()),
        ],
    )

    pa_table = pa.Table.from_pylist(records, schema=schema)

    # parse timestamp

    parsed_time = pa.array(
        [
            dt.datetime.fromtimestamp(t, tz=ZoneInfo(timezone))
            for t in pa_table["timestamp"].to_pylist()
        ],
        type=pa.timestamp("ns", tz=timezone),
    )

    pa_table = pa_table.set_column(
        pa_table.schema.get_field_index("timestamp"), "timestamp", parsed_time
    )

    # geohash

    geohash_field = pa.field("geohash", pa.string())

    latitudes = pa_table["latitude"].to_pylist()
    longitudes = pa_table["longitude"].to_pylist()

    geohashes = [
        pgh.encode(lat, lon, precision=7) for lat, lon in zip(latitudes, longitudes)
    ]

    pa_table = pa_table.append_column(geohash_field, pa.array(geohashes))

    # geometry

    geom_field = geom_field = pa.field(
        "geometry", ga.wkb(), metadata={b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    points = ga.point().from_geobuffers(
        None, array.array("d", longitudes), array.array("d", latitudes)
    )

    wkb_array = ga.as_wkb(ga.with_crs(points, ga.OGC_CRS84))

    pa_table = pa_table.append_column(geom_field, wkb_array)

    output_file = "/tmp/positions.parquet"

    writer = GeoParquetWriter(
        output_file,
        pa_table.schema,
        encoding=GeoParquetEncoding.WKB,
        compression="uncompressed",
        generate_covering=True,
    )
    writer.write_table(pa_table)
    writer.close()

    logger.info(f"saved parquet file to {output_file}")

    latest_timestamp = dt.datetime.now(tz=ZoneInfo(timezone))
    object_key = f"positions_raw/{latest_timestamp.strftime('%Y')}/{latest_timestamp.strftime('%m')}/{latest_timestamp.strftime('%d')}/{latest_timestamp.strftime('%H%M%S')}.parquet"

    try:
        logger.info("Uploading %s to bucket %s", object_key, destination_bucket)
        s3.upload_file(output_file, destination_bucket, object_key)
    except ClientError as e:
        logging.error(e)
