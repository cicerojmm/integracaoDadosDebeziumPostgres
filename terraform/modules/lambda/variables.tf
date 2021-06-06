locals {
  function_name = upper("${var.function_name}-${var.env}")
  time_interval = var.time_interval
}

variable "account_id" {
  type        = string
  description = "Account ID of aws provider to inject in IAM Roles."
}

variable "function_name" {
  type        = string
  description = "Lambda function name"
}

variable "env" {
  type        = string
  description = "Environment to add in resource name. Eg: dev."
}

variable "region" {
  type        = string
  description = "Region name to inject in IAM Roles."
}

variable "lambda_environment" {
  type = object({
    variables = map(string)
  })
  description = "Envinronment variables to add in Lambda Function"
  default     = null
}

variable "bucket_config" {
  type        = string
  description = "Pass Bucket name with config to give access in the policy."
}

variable "tables" {}

variable "tags" {
  type = map(string)
}


variable "time_interval"{
  type = number
  description = "Time interval to launch lambda"
}
