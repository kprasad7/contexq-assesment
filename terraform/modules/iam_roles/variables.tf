variable "role_name" {
  description = "IAM role name for Glue"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9-]+$", var.role_name))
    error_message = "Role name must be alphanumeric and hyphens."
  }
}

variable "raw_bucket_arn" {
  description = "ARN of raw data bucket"
  type        = string
}

variable "processed_bucket_arn" {
  description = "ARN of processed data bucket"
  type        = string
}

variable "mlflow_bucket_arn" {
  description = "ARN of MLflow artifacts bucket"
  type        = string
}

variable "glue_database_name" {
  description = "Glue database name for catalog access"
  type        = string
}

variable "account_id" {
  description = "AWS account ID"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
