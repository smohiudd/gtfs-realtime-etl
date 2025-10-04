"""
This is the handler for the gtfs-realtime-etl compaction lambda function.

https://github.com/aws-samples/s3-small-object-compaction
"""

import os
import logging
import boto3
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo

import pyarrow.dataset as ds
import geoarrow.pyarrow as ga
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


def merge_objects_from_s3(s3_bucket, date):
    objects = list_objects_in_s3(
        s3_bucket,
        f"positions_raw/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/",
    )
    if objects == "None":
        print(
            f"No objects found for {date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}"
        )
        return

    s3_uris = []
    for object in objects:
        s3_uris.append(f"{s3_bucket}/{object['Key']}")

    print(
        f"Found {len(s3_uris)} objects for {date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}"
    )

    dataset = ds.dataset(
        s3_uris,
        format="parquet",
        filesystem=s3fs,
    )

    # https://duckdb.org/docs/stable/guides/performance/file_formats.html
    min_rows_per_group = 15360
    max_rows_per_group = 122880

    ds.write_dataset(
        dataset,
        "/tmp",
        format="parquet",
        basename_template=f"positions_{{i}}.parquet",
        min_rows_per_group=min_rows_per_group,
        max_rows_per_group=max_rows_per_group,
        existing_data_behavior="overwrite_or_ignore",
        use_threads=True,
        filesystem=fs.LocalFileSystem(),
        create_dir=False
    )

    # loop through tmp and upload to s3
    for file in os.listdir("/tmp"):
        if file.endswith(".parquet"):
            upload_object_to_s3(
                s3_bucket,
                f"positions/{date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')}/{file}",
                f"/tmp/{file}",
            )
            print(f"Uploaded {file} to {s3_bucket}")


def get_dates_in_range(duration, timezone):
    start_date = datetime.now(tz=ZoneInfo(timezone)) - timedelta(days=duration)
    dates = []
    for n in range(duration):
        date = start_date + timedelta(days=n)
        dates.append(date)
    return dates


def handler(event, context):
    """
    This compresses the GTFS vehicle position data in S3 bucket
    """

    s3_bucket = event.get("s3_bucket")
    duration = event.get("duration")
    timezone = event.get("timezone")

    dates = get_dates_in_range(int(duration), timezone)

    for date in dates:
        merge_objects_from_s3(s3_bucket, date)
        print(
            f"Compacted {date.strftime('%Y')}/{date.strftime('%m')}/{date.strftime('%d')} of {len(dates)}"
        )

    print("Compaction complete!")
    return {"statusCode": 200, "body": "Compaction complete!"}
