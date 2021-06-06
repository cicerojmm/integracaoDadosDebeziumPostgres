resource "aws_s3_bucket_object" "dist" {
  for_each = fileset("${path.module}/jars", "*.jar")

  bucket = var.bucket_name
  key    = "spark/jars/${each.value}"
  source = "${path.module}/jars/${each.value}"
  # etag makes the file update when it changes; see https://stackoverflow.com/questions/56107258/terraform-upload-file-to-s3-on-every-apply
  etag = filemd5("${path.module}/jars/${each.value}")
}
