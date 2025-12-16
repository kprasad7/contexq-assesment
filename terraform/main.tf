# Module: S3 Buckets
module "s3_buckets" {
  source = "./modules/s3_buckets"

  name_prefix             = local.name_prefix
  enable_versioning       = local.s3_bucket_config.enable_versioning
  enable_access_logs      = local.s3_bucket_config.enable_access_logs
  enable_kms_encryption   = var.enable_kms_encryption
  kms_key_id              = var.enable_kms_encryption ? aws_kms_key.main[0].id : null
  raw_data_retention_days = var.raw_data_retention_days
  log_retention_days      = var.log_retention_days

  tags = local.common_tags
}

# Module: Glue Catalog
module "glue_catalog" {
  source = "./modules/glue_catalog"

  database_name        = local.glue_config.database_name
  table_name           = local.glue_config.table_name
  processed_bucket     = module.s3_buckets.processed_bucket_name
  processed_bucket_arn = module.s3_buckets.processed_bucket_arn
  account_id           = local.account_id
  region               = local.region

  tags = local.common_tags

  depends_on = [module.s3_buckets]
}

# Module: IAM Roles
module "iam_roles" {
  source = "./modules/iam_roles"

  role_name            = local.iam_config.glue_role_name
  raw_bucket_arn       = module.s3_buckets.raw_bucket_arn
  processed_bucket_arn = module.s3_buckets.processed_bucket_arn
  mlflow_bucket_arn    = module.s3_buckets.mlflow_bucket_arn
  glue_database_name   = local.glue_config.database_name
  account_id           = local.account_id
  region               = local.region

  tags = local.common_tags

  depends_on = [module.s3_buckets, module.glue_catalog]
}

# Module: Glue Jobs
module "glue_jobs" {
  source = "./modules/glue_jobs"

  job_name        = "${local.name_prefix}-etl"
  role_arn        = module.iam_roles.glue_service_role_arn
  glue_version    = local.glue_config.glue_version
  worker_type     = local.glue_config.worker_type
  num_workers     = local.glue_config.num_workers
  timeout_minutes = local.glue_config.timeout_minutes
  max_retries     = local.glue_config.max_retries
  python_version  = local.glue_config.python_version

  # S3 Paths and Buckets - single unified job
  script_location    = "s3://${module.s3_buckets.raw_bucket_name}/glue-scripts/comprehensive_etl_job.py"
  temp_dir           = "s3://${module.s3_buckets.raw_bucket_name}/glue-temp/"
  source_bucket_name = module.s3_buckets.raw_bucket_name
  target_bucket_name = module.s3_buckets.processed_bucket_name

  # Glue Catalog
  database_name = local.glue_config.database_name
  table_name    = local.glue_config.table_name

  # Logging
  log_group_name     = "/aws/glue/${local.name_prefix}-etl"
  log_retention_days = var.log_retention_days

  tags = local.common_tags

  depends_on = [module.iam_roles, module.glue_catalog]
}
