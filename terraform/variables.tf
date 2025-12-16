variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-\\d{1}$", var.aws_region))
    error_message = "AWS region must be valid (e.g., us-east-1)."
  }
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "contexq"

  validation {
    condition     = length(var.project_name) >= 3 && length(var.project_name) <= 20
    error_message = "Project name must be 3-20 characters."
  }
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "terraform_version" {
  description = "Terraform version for tracking"
  type        = string
  default     = "1.5.0"
}

# S3 Configuration
variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "enable_kms_encryption" {
  description = "Enable KMS encryption (vs AES256)"
  type        = bool
  default     = false
}

variable "kms_key_deletion_window" {
  description = "KMS key deletion window (7-30 days)"
  type        = number
  default     = 10

  validation {
    condition     = var.kms_key_deletion_window >= 7 && var.kms_key_deletion_window <= 30
    error_message = "Deletion window must be 7-30 days."
  }
}

variable "raw_data_retention_days" {
  description = "Raw data retention in days"
  type        = number
  default     = 90

  validation {
    condition     = var.raw_data_retention_days > 0
    error_message = "Retention days must be positive."
  }
}

# Glue Configuration
variable "glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, Z.2X)"
  type        = string
  default     = "G.2X"

  validation {
    condition     = contains(["G.1X", "G.2X", "Z.2X"], var.glue_worker_type)
    error_message = "Worker type must be G.1X, G.2X, or Z.2X."
  }
}

variable "glue_num_workers" {
  description = "Number of Glue workers"
  type        = number
  default     = 10

  validation {
    condition     = var.glue_num_workers >= 2 && var.glue_num_workers <= 100
    error_message = "Workers must be between 2-100."
  }
}

variable "glue_job_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 120

  validation {
    condition     = var.glue_job_timeout_minutes >= 1 && var.glue_job_timeout_minutes <= 1440
    error_message = "Timeout must be 1-1440 minutes."
  }
}

variable "glue_max_retries" {
  description = "Maximum Glue job retries"
  type        = number
  default     = 1

  validation {
    condition     = var.glue_max_retries >= 0 && var.glue_max_retries <= 5
    error_message = "Max retries must be 0-5."
  }
}

# Logging & Monitoring
variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.log_retention_days)
    error_message = "Log retention must be a valid CloudWatch value."
  }
}

variable "enable_s3_access_logs" {
  description = "Enable S3 access logging"
  type        = bool
  default     = true
}

# Tags
variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}

variable "owner_email" {
  description = "Owner email for resource tagging"
  type        = string
  default     = "data-engineering@contexq.com"
}

variable "cost_center" {
  description = "Cost center for billing"
  type        = string
  default     = "engineering"
}
