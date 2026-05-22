import boto3
from stream_read_xbrl import stream_read_xbrl_sync_s3_csv
import os


if __name__ == '__main__':
    s3_client = boto3.client('s3', region_name=os.getenv("S3_REGION"), endpoint_url=os.getenv("S3_ENDPOINT"))
    bucket_name = os.getenv('XBRL_CSV_BUCKET')
    key_prefix = 'xbrl/'

    stream_read_xbrl_sync_s3_csv(s3_client, bucket_name, key_prefix)