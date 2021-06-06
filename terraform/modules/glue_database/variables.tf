locals {
  database_name = "deltalake_${lower(var.environment)}"
}

variable "bucket_name_curated" {
  type = string
}

variable "environment" {
  type = string
}