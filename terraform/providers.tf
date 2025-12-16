terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = merge(
      local.common_tags,
      {
        ProvisionedBy = "Terraform"
        Version       = var.terraform_version
      }
    )
  }

  # Enforce best practices
  skip_credentials_validation = false
  skip_region_validation      = false
}

# Data source for current AWS account
data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

# KMS key for encryption (optional, for enhanced security)
resource "aws_kms_key" "main" {
  count = var.enable_kms_encryption ? 1 : 0

  description             = "${var.project_name} KMS key for ${var.environment} environment"
  deletion_window_in_days = var.kms_key_deletion_window
  enable_key_rotation     = true
  tags                    = local.common_tags
}

resource "aws_kms_alias" "main" {
  count = var.enable_kms_encryption ? 1 : 0

  name          = "alias/${var.project_name}-${var.environment}"
  target_key_id = aws_kms_key.main[0].key_id
}
