# Production Environment Configuration
# Usage: terraform plan/apply -var-file=environments/prod.tfvars
# NOTE: Always enable remote state management in production

aws_region   = "us-east-1"
project_name = "contexq"
environment  = "prod"

# S3 Configuration
enable_versioning       = true
enable_kms_encryption   = true # Use KMS encryption for prod
raw_data_retention_days = 365

# Glue Configuration
glue_worker_type         = "G.2X"
glue_num_workers         = 20
glue_job_timeout_minutes = 180
glue_max_retries         = 2

# Logging
log_retention_days    = 90 # Extended retention for prod
enable_s3_access_logs = true

# Tagging
owner_email = "prasadlvv049@gmail.com"
cost_center = "engineering"

tags = {
  Team                = "DataEngineering"
  CostCenter          = "Engineering"
  DataClassification  = "Confidential"
  BackupPolicy        = "Continuous"
  DisasterRecovery    = "Required"
  MonitoringLevel     = "Enhanced"
  ComplianceFramework = "SOC2"
}
