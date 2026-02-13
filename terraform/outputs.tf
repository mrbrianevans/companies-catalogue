output "sink_bucket" {
  value       = aws_s3_bucket.stream_sink.bucket
  description = "Bucket name to use as the event sink"
}
output "lake_bucket" {
  value       = aws_s3_bucket.stream_lake.bucket
  description = "Bucket where Ducklake Parquet resides"
}
output "snapshots_bucket" {
  value       = aws_s3_bucket.stream_snapshots.bucket
  description = "Bucket where output snapshots are stored"
}