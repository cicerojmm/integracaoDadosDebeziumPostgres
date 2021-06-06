resource "aws_s3_bucket_object" "stack_debezium" {
  for_each = fileset("${path.module}/files", "*.*")

  bucket = var.bucket_name
  key    = "dockerfiles/${each.value}"
  source = "${path.module}/files/${each.value}"
  # etag makes the file update when it changes; see https://stackoverflow.com/questions/56107258/terraform-upload-file-to-s3-on-every-apply
  etag = filemd5("${path.module}/files/${each.value}")
}
