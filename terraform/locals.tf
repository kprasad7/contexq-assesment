locals {
  # Resource naming
  name_prefix = "${var.project_name}-${var.environment}"

  # Common tags applied to all resources
  common_tags = merge(
    {
      Project         = var.project_name
      Environment     = var.environment
      Owner           = var.owner_email
      CostCenter      = var.cost_center
      ManagedBy       = "Terraform"
      CreatedDate     = timestamp()
      TerraformSource = "IaC"
      Version         = var.terraform_version
    },
    var.tags
  )

  # S3 configuration
  s3_bucket_config = {
    enable_versioning  = var.enable_versioning
    enable_access_logs = var.enable_s3_access_logs
    kms_key_id         = var.enable_kms_encryption ? aws_kms_key.main[0].id : null
  }

  # Glue configuration
  glue_config = {
    database_name   = replace("${var.project_name}_${var.environment}", "-", "_")
    table_name      = "corporate_registry"
    worker_type     = var.glue_worker_type
    num_workers     = var.glue_num_workers
    timeout_minutes = var.glue_job_timeout_minutes
    max_retries     = var.glue_max_retries
    glue_version    = "4.0"
    python_version  = "3.9"
  }

  # IAM configuration
  iam_config = {
    glue_role_name = "${local.name_prefix}-glue-service-role"
  }

  # Account ID and region
  account_id = data.aws_caller_identity.current.account_id
  region     = data.aws_region.current.name
}
