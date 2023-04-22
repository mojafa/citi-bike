#Create variable names
variable "project_name" {
  description = "GCP project ID"
  default = "de-finnhub"
}

variable "region" {
  description = "Region for GCP resources"
  default = "northamerica-northeast1"
  type = string
}

variable "gcs_bucket_name" {
  description = "GCS bucket name"
  default = "finnhub-gcs"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BQ dataset name"
  default = "finnhub_bq"
}

variable "spark_cluster_name" {
  description = "DataProc cluster name"
  default = "finnhub-spark-cluster"
}
