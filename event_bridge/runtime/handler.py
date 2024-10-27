import base64
import json
import logging
import os
import sys
from datetime import datetime

import boto3
import psycopg
import requests
from google.transit import gtfs_realtime_pb2
from zoneinfo import ZoneInfo


def get_secret(secret_name: str) -> None:
    """Retrieve secrets from AWS Secrets Manager

    Args:
        secret_name (str): name of aws secrets manager secret containing database connection secrets

    Returns:
        secrets (dict): decrypted secrets in dict
    """

    # Create a Secrets Manager client
    session = boto3.session.Session(region_name=os.environ.get("AWS_REGION"))
    client = session.client(service_name="secretsmanager")

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    get_secret_value_response = client.get_secret_value(
        SecretId=os.environ.get("SECRET_NAME")
    )

    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if "SecretString" in get_secret_value_response:
        return json.loads(get_secret_value_response["SecretString"])
    else:
        return json.loads(base64.b64decode(get_secret_value_response["SecretBinary"]))


secret_name = os.environ.get("SECRET_NAME")
conn_secrets = get_secret(secret_name)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = psycopg.connect(
        host=conn_secrets["host"],
        dbname=conn_secrets["dbname"],
        user=conn_secrets["username"],
        password=conn_secrets["password"],
    )
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to RDS postgis instance.")
    logger.error(e)
    sys.exit(1)

logger.info("SUCCESS: Connection to RDS for PostGRES instance succeeded")


def handler(event, context):
    """
    This ingest GTFS vehicle position data to the database
    """

    position_url = os.environ.get("VEH_POSITION_URL")

    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(position_url)
    feed.ParseFromString(response.content)

    records = [
        (
            i.vehicle.trip.trip_id,
            datetime.fromtimestamp(
                i.vehicle.timestamp, tz=ZoneInfo("America/Edmonton")
            ).isoformat(),
            i.vehicle.position.longitude,
            i.vehicle.position.latitude,
        )
        for i in feed.entity
    ]

    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO vehicle_position (trip_id, time_stamp, location) VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326));",
                records,
            )
            conn.commit()
            print(f"Inserted {len(records)} vehicle position records")

    except Exception as e:
        print(f"Unable to ingest vehicle position to database with exception={e}")
