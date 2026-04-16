

resource "aws_s3_bucket" "stream_snapshots" {
  bucket        = "${var.bucket_prefix}companies-stream-snapshots"
  force_destroy = false
  cors_rule {
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["http://localhost:5173", "https://companies-catalogue.co.uk"]
  }
}

resource "aws_s3_bucket" "private_snapshots" {
  bucket        = "${var.bucket_prefix}companies-stream-snapshots-private"
  force_destroy = false
}

resource "aws_s3_bucket" "stream_sink" {
  bucket        = "${var.bucket_prefix}companies-stream-sink"
  force_destroy = false
}

resource "aws_s3_bucket" "stream_lake" {
  bucket        = "${var.bucket_prefix}companies-stream-lake"
  force_destroy = false
}
