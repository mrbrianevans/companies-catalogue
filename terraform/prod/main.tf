terraform {
  backend "s3" {
    bucket                      = "terraform-state"
    key                         = "companies-catalogue/prod/terraform.tfstate"
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
    use_path_style              = true

  }
}

module "companies_catalogue" {
  source      = "./.."
  access_key  = var.access_key
  secret_key  = var.secret_key
  s3_region   = var.s3_region
  s3_endpoint = var.s3_endpoint
}
