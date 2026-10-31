# Credentials come from GOOGLE_APPLICATION_CREDENTIALS, the same service account
# key the pipeline already uses. Nothing authenticates through gcloud.
provider "google" {
  project = var.project_id
  region  = var.region
}
