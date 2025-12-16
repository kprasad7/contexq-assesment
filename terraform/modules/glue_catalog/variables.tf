variable "database_name" {
  description = "Glue database name"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.database_name))
    error_message = "Database name must be lowercase alphanumeric and underscores."
  }
}

variable "table_name" {
  description = "Glue table name"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.table_name))
    error_message = "Table name must be lowercase alphanumeric and underscores."
  }
}

variable "processed_bucket" {
  description = "Processed data S3 bucket name"
  type        = string
}

variable "processed_bucket_arn" {
  description = "Processed data S3 bucket ARN"
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
