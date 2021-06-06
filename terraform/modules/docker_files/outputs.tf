output "dockerfiles" {
  # value = "[ ${join(", ", [for jar_obj in aws_s3_bucket_object.dist : "s3://${jar_obj.bucket}/${jar_obj.key}"])} ]"
  value = join(",", [for obj in aws_s3_bucket_object.stack_debezium : "s3://${obj.bucket}/${obj.key}"])
}
