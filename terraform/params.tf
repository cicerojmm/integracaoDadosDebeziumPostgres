# Generate params. With you want to override any of this params use in this
locals {
  # AccoundID of AWS account where will deploy this solution.
  # The reason to concat with this is to not conflitct with
  # bucket names and replicated this solution to multiple environments
  account_id = data.aws_caller_identity.current.id
  region     = data.aws_region.current.name

  # If you want to mantain with multiple solutions with same service_name edit this
  service_name = upper(var.service_name)

  # Prefix for buckets
  prefix = lower(var.service_name)

  # Environment where the resources are deployed. 
  environment = upper(var.environment)

  # Password to authenticate in Portainer
  generated_password = random_password.password.result

  lambda_params = var.lambda_environment

  conf_tables = var.conf_tables

  tags = {
    TDC = "TDC_Delta"
  }

  
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

resource "random_password" "password" {
  length           = 16
  special          = true
  override_special = "_%@"
}


