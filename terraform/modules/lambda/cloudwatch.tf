resource "aws_cloudwatch_event_rule" "every_time_interval" {
  name        = "every-${local.time_interval}-minute"
  description = "Fires every ${local.time_interval} minutes"
  schedule_expression = "rate(${local.time_interval} minutes)"
}

resource "aws_cloudwatch_event_target" "check_lambda_every_time_interval" {
  rule        = aws_cloudwatch_event_rule.every_time_interval.name
  target_id   = "lambda_function"
  arn         = aws_lambda_function.lambda_function.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_check_lambda" {
  statement_id = "AllowExecutionFromCloudWatch"
  action       = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_function.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_time_interval.arn
}