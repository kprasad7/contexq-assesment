variable "job_name" {
  description = "Name of the Glue job"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+$", var.job_name))
    error_message = "Job name must be alphanumeric with underscores and hyphens."
  }
}

variable "role_arn" {
  description = "IAM role ARN for Glue job"
  type        = string

  validation {
    condition     = can(regex("^arn:aws:iam::", var.role_arn))
    error_message = "Must be a valid IAM role ARN."
  }
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "4.0"

  validation {
    condition     = contains(["3.0", "4.0"], var.glue_version)
    error_message = "Glue version must be 3.0 or 4.0."
  }
}

variable "worker_type" {
  description = "Worker type (G.1X, G.2X, Z.2X)"
  type        = string
  default     = "G.2X"

  validation {
    condition     = contains(["G.1X", "G.2X", "Z.2X"], var.worker_type)
    error_message = "Worker type must be G.1X, G.2X, or Z.2X."
  }
}

variable "num_workers" {
  description = "Number of workers"
  type        = number
  default     = 10

  validation {
    condition     = var.num_workers >= 2 && var.num_workers <= 100
    error_message = "Workers must be between 2-100."
  }
}

variable "timeout_minutes" {
  description = "Job timeout in minutes"
  type        = number
  default     = 120

  validation {
    condition     = var.timeout_minutes >= 1 && var.timeout_minutes <= 1440
    error_message = "Timeout must be 1-1440 minutes."
  }
}

variable "max_retries" {
  description = "Maximum job retries"
  type        = number
  default     = 1

  validation {
    condition     = var.max_retries >= 0 && var.max_retries <= 5
    error_message = "Max retries must be 0-5."
  }
}

variable "python_version" {
  description = "Python version"
  type        = string
  default     = "3.9"
}

variable "script_location" {
  description = "S3 location of Glue job script"
  type        = string

  validation {
    condition     = can(regex("^s3://", var.script_location))
    error_message = "Script location must be S3 URI."
  }
}

variable "temp_dir" {
  description = "S3 temporary directory"
  type        = string

  validation {
    condition     = can(regex("^s3://", var.temp_dir))
    error_message = "Temp directory must be S3 URI."
  }
}

variable "database_name" {
  description = "Glue database name"
  type        = string
}

variable "table_name" {
  description = "Glue table name"
  type        = string
}

variable "log_group_name" {
  description = "CloudWatch log group name"
  type        = string
}

variable "log_retention_days" {
  description = "Log retention in days"
  type        = number
  default     = 30

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.log_retention_days)
    error_message = "Log retention must be valid CloudWatch value."
  }
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
