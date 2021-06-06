resource "aws_glue_catalog_database" "deltalake" {
  name         = local.database_name
  location_uri = "s3://${var.bucket_name_curated}/${local.database_name}"
}


