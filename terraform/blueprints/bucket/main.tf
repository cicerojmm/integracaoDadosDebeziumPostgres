resource "aws_s3_bucket" "template_bucket" {
  bucket = lower(local.bucket_name_concat)

  tags = merge({
    Name        = lower(local.bucket_name_concat)
    Environment = var.environment
  }, var.tags)
  force_destroy = true
}

locals {
  bucket_name_concat = var.suffix == "" ? "${var.prefix}-${var.account_id}-${var.environment}-${var.bucket_name}" : "${var.prefix}-${var.account_id}-${var.environment}-${var.bucket_name}-${var.suffix}"
}
