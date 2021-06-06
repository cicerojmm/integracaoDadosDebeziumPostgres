module "raw" {
  source = "../../blueprints/bucket"

  bucket_name = "kafka"
  account_id  = var.account_id
  environment = var.environment
  prefix      = var.prefix
  suffix      = "raw"

  tags = var.tags
}

module "staged" {
  source = "../../blueprints/bucket"

  bucket_name = "kafka"
  account_id  = var.account_id
  environment = var.environment
  prefix      = var.prefix
  suffix      = "staged"

  tags = var.tags
}

module "curated" {
  source = "../../blueprints/bucket"

  bucket_name = "kafka"
  account_id  = var.account_id
  environment = var.environment
  prefix      = var.prefix
  suffix      = "curated"

  tags = var.tags
}

module "configs" {
  source = "../../blueprints/bucket"

  bucket_name = "configs"
  account_id  = var.account_id
  environment = var.environment
  prefix      = var.prefix
  suffix      = ""
  
  tags = var.tags
}


