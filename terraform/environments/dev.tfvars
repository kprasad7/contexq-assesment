# Development Environment Configuration
# Usage: terraform plan/apply -var-file=environments/dev.tfvars

aws_region   = "us-east-1"
project_name = "contexq"
environment  = "dev"

# S3 Configuration
enable_versioning       = true
enable_kms_encryption   = false # Use AES256 for dev
raw_data_retention_days = 90

# Glue Configuration
glue_worker_type         = "G.2X"
glue_num_workers         = 10
glue_job_timeout_minutes = 120
glue_max_retries         = 1

# Logging
log_retention_days    = 7 # Short retention for dev
enable_s3_access_logs = true

# Tagging
owner_email = "prasadlvv049@gmail.com"
cost_center = "engineering"

tags = {
  Team               = "DataEngineering"
  CostCenter         = "Engineering"
  DataClassification = "Internal"
  BackupPolicy       = "Daily"
  MonitoringLevel    = "Standard"
}
