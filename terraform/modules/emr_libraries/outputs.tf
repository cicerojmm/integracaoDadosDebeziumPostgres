output "jars" {
  # value = "[ ${join(", ", [for jar_obj in aws_s3_bucket_object.dist : "s3://${jar_obj.bucket}/${jar_obj.key}"])} ]"
  value = join(",", [for jar_obj in aws_s3_bucket_object.dist : "s3://${jar_obj.bucket}/${jar_obj.key}"])
}
