# S3 Bucket: Raw Data
resource "aws_s3_bucket" "raw_data" {
  bucket = "${var.name_prefix}-raw-data-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.name_prefix}-raw-data"
      BucketType  = "Raw"
      Purpose     = "Source data storage"
    }
  )
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status     = var.enable_versioning ? "Enabled" : "Suspended"
    mfa_delete = var.enable_versioning ? "Disabled" : null
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm      = var.enable_kms_encryption ? "aws:kms" : "AES256"
      kms_master_key_id  = var.enable_kms_encryption ? var.kms_key_id : null
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "transition-to-standard-ia"
    status = "Enabled"
    filter {}

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 60
      storage_class = "GLACIER"
    }

    expiration {
      days = var.raw_data_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_logging" "raw_data" {
  count = var.enable_access_logs ? 1 : 0

  bucket = aws_s3_bucket.raw_data.id

  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "raw-data/"
}

# S3 Bucket: Processed Data (Iceberg Warehouse)
resource "aws_s3_bucket" "processed_data" {
  bucket = "${var.name_prefix}-processed-data-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.name_prefix}-processed-data"
      BucketType  = "Processed"
      Purpose     = "Iceberg warehouse and transformed data"
    }
  )
}

resource "aws_s3_bucket_versioning" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm      = var.enable_kms_encryption ? "aws:kms" : "AES256"
      kms_master_key_id  = var.enable_kms_encryption ? var.kms_key_id : null
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "processed_data" {
  bucket = aws_s3_bucket.processed_data.id

  rule {
    id     = "archive-old-versions"
    status = "Enabled"
    filter {}

    transition {
      days          = 180
      storage_class = "GLACIER"
    }

    expiration {
      days = 730
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 180
    }
  }
}

resource "aws_s3_bucket_logging" "processed_data" {
  count = var.enable_access_logs ? 1 : 0

  bucket = aws_s3_bucket.processed_data.id

  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "processed-data/"
}

# S3 Bucket: MLflow Artifacts
resource "aws_s3_bucket" "mlflow_artifacts" {
  bucket = "${var.name_prefix}-mlflow-artifacts-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.name_prefix}-mlflow"
      BucketType  = "MLflow"
      Purpose     = "ML model artifacts and metadata"
    }
  )
}

resource "aws_s3_bucket_versioning" "mlflow_artifacts" {
  bucket = aws_s3_bucket.mlflow_artifacts.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mlflow_artifacts" {
  bucket = aws_s3_bucket.mlflow_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm      = var.enable_kms_encryption ? "aws:kms" : "AES256"
      kms_master_key_id  = var.enable_kms_encryption ? var.kms_key_id : null
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "mlflow_artifacts" {
  bucket = aws_s3_bucket.mlflow_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "mlflow_artifacts" {
  bucket = aws_s3_bucket.mlflow_artifacts.id

  rule {
    id     = "cleanup-old-artifacts"
    status = "Enabled"
    filter {}

    transition {
      days          = 180
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

resource "aws_s3_bucket_logging" "mlflow_artifacts" {
  count = var.enable_access_logs ? 1 : 0

  bucket = aws_s3_bucket.mlflow_artifacts.id

  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "mlflow-artifacts/"
}

# S3 Bucket: Logs (for access logging)
resource "aws_s3_bucket" "logs" {
  bucket = "${var.name_prefix}-logs-${data.aws_caller_identity.current.account_id}"

  tags = merge(
    var.tags,
    {
      Name        = "${var.name_prefix}-logs"
      BucketType  = "Logs"
      Purpose     = "S3 access logs aggregation"
    }
  )
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket = aws_s3_bucket.logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "cleanup-old-logs"
    status = "Enabled"
    filter {}

    expiration {
      days = var.log_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

data "aws_caller_identity" "current" {}
