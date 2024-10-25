import base64
import json
import logging
import os
import sys

import boto3
import psycopg


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
    This function creates a new RDS database table and writes records to it
    """

    data = {"CustID": 1, "Name": "Saadiq M"}
    CustID = data["CustID"]
    Name = data["Name"]

    item_count = 0
    sql_string = "insert into Customer (CustID, Name) values(%s, %s)"

    with conn.cursor() as cur:
        cur.execute(
            "create table if not exists Customer ( CustID int NOT NULL, Name varchar(255) NOT NULL, PRIMARY KEY (CustID))"
        )
        cur.execute(sql_string, (CustID, Name))
        conn.commit()
        cur.execute("select * from Customer")
        logger.info("The following items have been added to the database:")
        for row in cur:
            item_count += 1
            logger.info(row)
    conn.commit()

    return "Added %d items to RDS for MySQL table" % (item_count)
