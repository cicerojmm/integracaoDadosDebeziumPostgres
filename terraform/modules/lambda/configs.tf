resource "aws_s3_bucket_object" "json_conf" {
  bucket  = var.bucket_config
  key     = "spark/conf/${var.function_name}.json"
  content = jsonencode(var.tables)
  # count = var.json_conf_conditional ? 1 : 0
  # lifecycle {
  #   prevent_destroy = true
  # }
}
