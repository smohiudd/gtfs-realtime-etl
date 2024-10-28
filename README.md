##  GTFS-REALTIME-ETL

Infrastructure as Code (IAC) to ingest [GTFS Realtime](https://gtfs.org/documentation/realtime/reference/#message-vehicleposition) vehicle position data into PostGIS database. 

## Deployment

This project uses AWS CDK to deploy all resource required to ingest GTFS Realtime data into a database on a given schedule including a VPC, Event Bridge Scheduler, Lambda Function and RDS. 

### Install CDK

- [CDK Documentation](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)

### Environment variables

See the [.example.env](.example.env) file for typical environment variables used for a deployment.

### Install Requirements

Create virtual environment and install deployment dependencies:

`pip install -r requirements.txt`

### Run Deployment

Review changes in deployment:

`ckd diff`

Run deployment of resources:

`cdk deploy`

### Architecture
![alt text](./gtfs-realtime-etl-arch-diagram.png)

