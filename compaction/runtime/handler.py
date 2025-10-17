"""
This is the handler for the gtfs-realtime-etl compaction lambda function.

https://github.com/aws-samples/s3-small-object-compaction
"""

import os
import logging
import boto3
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from pyarrow import fs


s3 = boto3.client("s3", region_name=os.environ.get("AWS_DEFAULT_REGION"))
s3fs = fs.S3FileSystem(
    access_key=os.environ.get("AWS_ACCESS_KEY_ID"),
    secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    session_token=os.environ.get("AWS_SESSION_TOKEN"),
    region=os.environ.get("AWS_DEFAULT_REGION"),
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def list_objects_in_s3(bucket, prefix):
    contents = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3.list_objects_v2(
                Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token
            )
        else:
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
        contents.extend(response.get("Contents", []))

        if "NextContinuationToken" not in response:
            break

        continuation_token = response["NextContinuationToken"]

    return contents if contents else "None"


def upload_object_to_s3(bucket, key, file_path):
    s3.upload_file(file_path, bucket, key)


def merge_objects_from_s3(s3_bucket, date, period, city_name):
    if period == "days":
        objects = list_objects_in_s3(
            s3_bucket,
            f"{city_name}/positions_raw/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/",
        )
        
        if objects == "None":
            print(
                f"No objects found for {date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}"
            )
            return
    elif period == "months":
        objects = list_objects_in_s3(
            s3_bucket,
            f"{city_name}/positions/{date.strftime('%Y')}/{date.strftime('%m')}/",
        )

        if objects == "None":
            print(
                f"No objects found for {date.strftime('%Y')}/{date.strftime('%m')}"
            )
            return

    s3_uris = []
    for object in objects:
        s3_uris.append(f"{s3_bucket}/{object['Key']}")

    print(
        f"Found {len(s3_uris)} objects"
    )

    metadata = pq.read_metadata(
        s3_uris[0], filesystem=s3fs
    ).metadata  # get the file level metadata since GeoParquetWriter doesn't write table level metadata

    dataset = (
        pq.ParquetDataset(
            s3_uris,
            filesystem=s3fs,
        )
        .read()
        .sort_by("geohash")
    )

    schema = dataset.schema.with_metadata(metadata)

    # https://duckdb.org/docs/stable/guides/performance/file_formats.html
    min_rows_per_group = 61440
    max_rows_per_group = 122880

    pq.write_to_dataset(
        dataset,
        "/tmp",
        schema=schema,
        basename_template=f"positions_{{i}}.parquet",
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        existing_data_behavior="overwrite_or_ignore",
        use_threads=True,
        preserve_order=True,
        filesystem=fs.LocalFileSystem(),
        create_dir=False,
        compression="zstd",
        compression_level=3,
    )

    # loop through tmp and upload to s3
    for file in os.listdir("/tmp"):
        if file.endswith(".parquet"):
            if period == "days":
                s3_key = f"{city_name}/positions/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/{file}"
            elif period == "months":
                s3_key = f"{city_name}/positions/{date.strftime('%Y')}/{date.strftime('%m')}/{file}"
            upload_object_to_s3(
                s3_bucket,
                s3_key,
                f"/tmp/{file}",
            )
            print(f"Uploaded {file} to {s3_bucket}")


def get_dates_in_range(duration, timezone, period, compact_to_now):
    dates = []
    
    if period == "days":
        start_date = datetime.now(ZoneInfo(timezone)) - relativedelta(days=duration)
    elif period == "months":
        start_date = datetime.now(ZoneInfo(timezone)) - relativedelta(months=duration)

    if compact_to_now:
        duration+=1
        
    for n in range(duration):
        if period == "days":
            date = start_date + relativedelta(days=n)
        elif period == "months":
            date = start_date + relativedelta(months=n)
        dates.append(date)
    return dates


def handler(event, context):
    """
    This compresses the GTFS vehicle position data in S3 bucket
    """

    s3_bucket = event.get("s3_bucket")
    previous_days = event.get("previous_days")
    previous_months = event.get("previous_months")
    timezone = event.get("timezone")
    compact_to_now = event.get("compact_to_now")
    city_name = event.get("stage")
    
    if previous_days:
        duration = previous_days
        period = "days"
    elif previous_months:
        duration = previous_months
        period = "months"

    dates = get_dates_in_range(int(duration), timezone, period, compact_to_now)

    for date in dates:
        merge_objects_from_s3(s3_bucket, date, period, city_name)
        print(
            f"Compacted {date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')} of {len(dates)}"
        )

    print("Compaction complete!")
    return {"statusCode": 200, "body": "Compaction complete!"}
