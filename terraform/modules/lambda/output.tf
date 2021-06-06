output "resources" {
  value = {
    aws_lambda_function = aws_lambda_function.lambda_function
    aws_iam_role        = aws_iam_role.iam_for_lambda
    archive_file        = data.archive_file.file
  }
}

output "tables" {
  value = aws_s3_bucket_object.json_conf
}
