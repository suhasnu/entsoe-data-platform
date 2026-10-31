variable "project_id" {
  description = "GCP project holding the warehouse"
  type        = string
}

variable "region" {
  description = "Default region for regional resources"
  type        = string
  default     = "europe-west1"
}

variable "bq_location" {
  description = "BigQuery location. Changing it would replace every dataset, so it has to match what already exists."
  type        = string
  default     = "EU"
}
