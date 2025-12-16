variable "name_prefix" {
  description = "Prefix for bucket names"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.name_prefix))
    error_message = "Name prefix must be lowercase alphanumeric and hyphens."
  }
}

variable "enable_versioning" {
  description = "Enable S3 versioning"
  type        = bool
  default     = true
}

variable "enable_access_logs" {
  description = "Enable S3 access logging"
  type        = bool
  default     = true
}

variable "enable_kms_encryption" {
  description = "Use KMS encryption instead of AES256"
  type        = bool
  default     = false
}

variable "kms_key_id" {
  description = "KMS key ID for encryption"
  type        = string
  default     = null
}

variable "raw_data_retention_days" {
  description = "Retention days for raw data"
  type        = number
  default     = 90

  validation {
    condition     = var.raw_data_retention_days > 0
    error_message = "Retention days must be positive."
  }
}

variable "log_retention_days" {
  description = "Retention days for logs"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
