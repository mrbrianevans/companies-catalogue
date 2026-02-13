output "sink_bucket" {
  value       = module.companies_catalogue.sink_bucket
  description = "Bucket name to use as the event sink"
}
output "lake_bucket" {
  value       = module.companies_catalogue.lake_bucket
  description = "Bucket where Ducklake Parquet resides"
}
output "snapshots_bucket" {
  value       = module.companies_catalogue.snapshots_bucket
  description = "Bucket where output snapshots are stored"
}