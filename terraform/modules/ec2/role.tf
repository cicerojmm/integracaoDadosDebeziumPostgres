resource "aws_iam_role" "assume_role" {
  name               = upper("${var.instance_name}-${var.environment}-role-assume")
  assume_role_policy = file("${path.module}/roles/assume_role.json")
}

resource "aws_iam_policy" "s3_access_policy" {
  name        = upper("${var.instance_name}-${var.environment}-policy")
  description = "S3 Full Access to Sink Connector"
  policy      = data.template_file.policy.rendered
}

data "template_file" "policy" {
  template = file("${path.module}/roles/policy.json")

  vars = {
    BUCKET_CONFIGS   = var.bucket_configs
  }
}

resource "aws_iam_policy_attachment" "role_attachment" {
  name       = upper("${var.instance_name}-${var.environment}-role-attach")
  roles      = [aws_iam_role.assume_role.name]
  policy_arn = aws_iam_policy.s3_access_policy.arn
}

resource "aws_iam_instance_profile" "role_profile" {
  name = "${var.instance_name}-${var.environment}-role"
  role = aws_iam_role.assume_role.name
}
