# companies-catalogue

A production-grade data pipeline to persist a history of events from Companies House.

- capture raw events from Companies House streaming API and store in compressed JSON.
- load captured events into a lakehouse (ducklake) of parquet files.
- merge events into a snapshot in the ducklake
- export the snapshot to bulk files

## Authentication

To run these scripts, you need to have the following environment variables set:

- `S3_ENDPOINT`: The endpoint URL for your S3 service.
- `S3_REGION`: The region where your S3 bucket is located.
- `S3_ACCESS_KEY_ID`: Your AWS access key ID.
- `S3_SECRET_ACCESS_KEY`: Your AWS secret access key.
- `SINK_BUCKET`: The name of the S3 bucket where you want to store the `.json.gz` files.
- `LAKE_BUCKET`: The name of the S3 bucket where you want to store the parquet files of the ducklake.
- `SNAPSHOT_BUCKET`: The name of the S3 bucket where you want to store the output snapshots.
- `STREAM_KEY`: The API key for Companies House streaming API.
