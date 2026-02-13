# companies-catalogue

 - capture events from Companies House streaming API and store in `.json.gz` files on S3
 - load events into a ducklake of parquet files from those `.json.gz` files
 - merge events into a snapshot in the ducklake
 - export the snapshot to bulk files