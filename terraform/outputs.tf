# S3 Outputs
output "s3_raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = module.s3_buckets.raw_bucket_name
}

output "s3_raw_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = module.s3_buckets.raw_bucket_arn
}

output "s3_processed_bucket_name" {
  description = "Name of the processed data S3 bucket"
  value       = module.s3_buckets.processed_bucket_name
}

output "s3_processed_bucket_arn" {
  description = "ARN of the processed data S3 bucket"
  value       = module.s3_buckets.processed_bucket_arn
}

output "s3_mlflow_bucket_name" {
  description = "Name of the MLflow artifacts S3 bucket"
  value       = module.s3_buckets.mlflow_bucket_name
}

output "s3_mlflow_bucket_arn" {
  description = "ARN of the MLflow artifacts S3 bucket"
  value       = module.s3_buckets.mlflow_bucket_arn
}

# Glue Catalog Outputs
output "glue_database_name" {
  description = "Name of the Glue database"
  value       = module.glue_catalog.database_name
}

output "glue_database_arn" {
  description = "ARN of the Glue database"
  value       = module.glue_catalog.database_arn
}

output "iceberg_table_name" {
  description = "Name of the Iceberg table"
  value       = module.glue_catalog.table_name
}

output "iceberg_table_arn" {
  description = "ARN of the Iceberg table"
  value       = module.glue_catalog.table_arn
}

output "iceberg_table_location" {
  description = "S3 location of the Iceberg table"
  value       = module.glue_catalog.table_location
}

# IAM Outputs
output "glue_service_role_arn" {
  description = "ARN of the Glue service role"
  value       = module.iam_roles.glue_service_role_arn
}

output "glue_service_role_name" {
  description = "Name of the Glue service role"
  value       = module.iam_roles.glue_service_role_name
}

# Glue Job Outputs
output "glue_job_name" {
  description = "Name of the Glue ETL job"
  value       = module.glue_jobs.job_name
}

output "glue_job_arn" {
  description = "ARN of the Glue ETL job"
  value       = module.glue_jobs.job_arn
}

output "glue_ml_training_job_name" {
  description = "Name of the Glue ML training job"
  value       = module.glue_ml_training_job.job_name
}

output "glue_ml_training_job_arn" {
  description = "ARN of the Glue ML training job"
  value       = module.glue_ml_training_job.job_arn
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name for Glue jobs"
  value       = module.glue_jobs.log_group_name
}

output "cloudwatch_log_group_arn" {
  description = "CloudWatch log group ARN"
  value       = module.glue_jobs.log_group_arn
}

# Infrastructure Summary
output "infrastructure_summary" {
  description = "Summary of deployed infrastructure"
  value = {
    account_id        = local.account_id
    region            = local.region
    project           = var.project_name
    environment       = var.environment
    name_prefix       = local.name_prefix
    deployment_date   = timestamp()
    terraform_version = var.terraform_version
  }
}

# KMS Key Output
output "kms_key_id" {
  description = "KMS key ID for encryption"
  value       = var.enable_kms_encryption ? aws_kms_key.main[0].id : "N/A"
  sensitive   = true
}

output "kms_key_arn" {
  description = "KMS key ARN"
  value       = var.enable_kms_encryption ? aws_kms_key.main[0].arn : "N/A"
  sensitive   = true
}
