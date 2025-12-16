# Additional MWAA and EventBridge Infrastructure
# Glue jobs are already managed by the glue_jobs module
# This file adds MWAA DAG bucket and EventBridge triggers

# ============================================================================
# S3 BUCKET FOR MWAA DAGS
# ============================================================================

resource "aws_s3_bucket" "mwaa_dags" {
  bucket = "${local.name_prefix}-mwaa-dags-${local.account_id}"
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa_dags" {
  bucket = aws_s3_bucket.mwaa_dags.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ============================================================================
# OUTPUTS
# ============================================================================

output "mwaa_dags_bucket" {
  value       = aws_s3_bucket.mwaa_dags.id
  description = "S3 bucket for MWAA DAGs"
}
