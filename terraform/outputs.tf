output "datasets" {
  description = "Datasets under Terraform management"
  value = {
    bronze  = google_bigquery_dataset.bronze.dataset_id
    staging = google_bigquery_dataset.staging.dataset_id
    marts   = google_bigquery_dataset.marts.dataset_id
  }
}
