
variable "access_key" {
  type        = string
  description = "S3 provider access key ID"
}

variable "secret_key" {
  type        = string
  description = "S3 provider secret access key"
}

variable "s3_endpoint" {
  type        = string
  description = "S3 endpoint"
}

variable "s3_region" {
  type        = string
  description = "S3 region"
}

variable "bucket_prefix" {
  type        = string
  default     = ""
  description = "Prefix bucket names with something env specific like dev-"
}
