terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6"
    }
  }
}

# uses AWS environment for S3 compatibility, but doesn't need to be an AWS bucket. Can be any S3 compatible service.
provider "aws" {
  # Skip AWS-specific validation to allow other s3-compatible bucket providers
  skip_credentials_validation = true
  skip_region_validation      = true
  skip_requesting_account_id  = true
}
