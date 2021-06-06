resource "aws_iam_role" "iam_for_lambda" {
  name = upper("${local.function_name}_ROLE")

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_policy_attachment" "basic_attach" {
  name       = upper("${local.function_name}_basic_attachment")
  roles      = [aws_iam_role.iam_for_lambda.name]
  policy_arn = aws_iam_policy.basic_role.arn
}

resource "aws_iam_policy" "basic_role" {
  name        = upper("${local.function_name}_AWSLambdaBasicExecutionRole")
  description = "Basic policy to Lambda ${local.function_name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "logs:CreateLogGroup",
      "Resource": "arn:aws:logs:${var.region}:${var.account_id}:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/lambda/${local.function_name}:*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_policy_attachment" "custom_attach" {
  name       = upper("${local.function_name}_custom_attachment")
  roles      = [aws_iam_role.iam_for_lambda.name]
  policy_arn = aws_iam_policy.custom_policy.arn
}

resource "aws_iam_policy" "custom_policy" {
  name        = upper("${local.function_name}_Custom")
  description = "Extra policies to Lambda ${local.function_name}"

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "iam:PassRole",
      "Effect": "Allow",
      "Resource": [
          "*"
      ]
    },
    {
      "Sid": "EMRRunJobFlow",
      "Effect": "Allow",
      "Action": "elasticmapreduce:*",
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
                    "arn:aws:s3:::${var.bucket_config}/*"
                  ]
    }
  ]
}
EOF
}
