locals {
  labels = {
    managed_by = "terraform"
    stack      = "gridflow"
  }
}

# Terraform owns the datasets. Table DDL stays with the SQL migrations and dbt,
# because two systems that both believe they own a table is how people lose one.

resource "google_bigquery_dataset" "bronze" {
  dataset_id    = "bronze"
  friendly_name = "Bronze"
  description   = "Raw ENTSO-E responses, append only, partitioned by month"
  location      = var.bq_location
  labels        = local.labels

  # A destroy should fail on a dataset that still holds tables, not cascade.
  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "staging" {
  dataset_id    = "gridflow_staging"
  friendly_name = "Staging"
  description   = "dbt views that dedupe revisions and conform to an hourly grain"
  location      = var.bq_location
  labels        = local.labels

  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_bigquery_dataset" "marts" {
  dataset_id    = "gridflow_marts"
  friendly_name = "Marts"
  description   = "dbt facts and dimensions, and the star schema behind Power BI"
  location      = var.bq_location
  labels        = local.labels

  delete_contents_on_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}
