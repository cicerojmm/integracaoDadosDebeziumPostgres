resource "aws_lambda_function" "lambda_function" {
  filename      = data.archive_file.file.output_path
  function_name = local.function_name
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "main.handler"
  memory_size   = 128
  timeout       = 30

  source_code_hash = data.archive_file.file.output_base64sha256

  runtime = "python3.8"

  # Envinronment variables to add in Lambda Function
  dynamic "environment" {
    for_each = var.lambda_environment == null ? [] : [var.lambda_environment]
    content {
      variables = merge(environment.value.variables, var.tags, {
        TABLES_CONF_PATH = "spark/conf/${var.function_name}.json"
      })
    }
  }

  tags = merge(
    {
      Name = local.function_name
    },
    var.tags
  )
}

data "archive_file" "file" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_code"
  output_path = "build/${local.function_name}.zip"
}