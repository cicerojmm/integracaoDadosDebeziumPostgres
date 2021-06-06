locals {
  tags = {
    instance = {
      Name = upper(var.instance_name)
      Env  = var.environment
    }

    sg = {
      Name = upper(var.instance_name)
      Env  = var.environment
    }

    s3  = {
        Name = upper(var.instance_name)
        Env  = var.environment
    }
  }
}

variable "instance_name" {
    type        = string
    default     = "stack-debezium-postgres"
    description = "Name of the EC2 Instance on console"
}

variable "environment" {
    type        = string
    default     = "dev"
    description = "Environment for Applications"
}

variable "instance_type" {
    type        = string
    default     = "t2.micro"
    description = "Type of the EC2 instance"
}

variable "volume_size" {
    type        = string
    default     = "10"
    description = "Main volume size instance"
}
variable "volume_type" {
    type        = string
    default     = "gp2"
    description = "Type of volume. Can be standard, gp2, io1 or io2"
}

variable "keypair_name" { 
    type        = string
    default     = "nova-ec2-ohio"
    description = "Keypair ssh access name"
}

variable "vpc_id" { 
    type        = string
    default     = "vpc-0cde6f67"
    description = "Main VPC for instance"
}

variable "subnet_id" { 
    type        = string
    default     = "subnet-62fceb18"
    description = "Main subnet id for instance"
}

variable "bucket_raw" {
  type = string
}

variable "bucket_artifacts" {
  type = string
}

variable "bucket_configs" {
  type = string
}

variable "bucket_docker_files" {
  type = string
}