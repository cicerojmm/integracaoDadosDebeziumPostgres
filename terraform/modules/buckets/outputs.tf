output "buckets" {
  value = {
    raw     = module.raw.bucket
    staged  = module.staged.bucket
    curated = module.curated.bucket
    configs = module.configs.bucket
  }
}
