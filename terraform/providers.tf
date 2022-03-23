provider "aws" {
  region = "us-east-1"
}

terraform {
  backend "s3" {
    bucket = "cjmm-datalake-configs"
    key    = "terraform/deltalake/terraform.tfstate"
    region = "us-east-1"
  }
}