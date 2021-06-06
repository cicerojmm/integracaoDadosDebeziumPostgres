output "python_script" {
  value = "s3://${aws_s3_bucket_object.pyspark.bucket}/${aws_s3_bucket_object.pyspark.key}"
}

output "jar_binaries" {
  value = [for jar_obj in aws_s3_bucket_object.scala_jars : "s3://${jar_obj.bucket}/${jar_obj.key}"]
}
