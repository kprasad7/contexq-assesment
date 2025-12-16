# Terraform configuration for EventBridge DAG trigger and MWAA integration.
# Adds to existing infrastructure.

# ============================================================================
# EVENTBRIDGE RULE - 6-HOURLY DAG TRIGGER
# ============================================================================

resource "aws_cloudwatch_event_rule" "mwaa_dag_schedule" {
  name                = "${local.project_name}-mwaa-dag-trigger"
  description         = "Trigger MWAA DAG every 6 hours"
  schedule_expression = "cron(0 0,6,12,18 * * ? *)"

  tags = local.tags
}

resource "aws_cloudwatch_event_target" "mwaa_dag_target" {
  rule      = aws_cloudwatch_event_rule.mwaa_dag_schedule.name
  target_id = "MWAADagTrigger"
  arn       = "arn:aws:events:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:event-bus/default"
  role_arn  = aws_iam_role.eventbridge_role.arn

  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = jsonencode({
      dagName     = "olist_data_pipeline"
      environment = local.environment
      triggeredAt = "<time>"
    })
  }

  dead_letter_config {
    arn = aws_sqs_queue.eventbridge_dlq.arn
  }
}

# ============================================================================
# IAM ROLE FOR EVENTBRIDGE
# ============================================================================

resource "aws_iam_role" "eventbridge_role" {
  name = "${local.project_name}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${local.project_name}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "mwaa:CreateWebLoginToken",
          "mwaa:GetEnvironment",
        ]
        Resource = "arn:aws:airflow:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:environment/${local.project_name}-mwaa"
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
        ]
        Resource = aws_sqs_queue.eventbridge_dlq.arn
      }
    ]
  })
}

# ============================================================================
# SQS DEAD LETTER QUEUE FOR EVENTBRIDGE
# ============================================================================

resource "aws_sqs_queue" "eventbridge_dlq" {
  name                      = "${local.project_name}-eventbridge-dlq"
  message_retention_seconds = 1209600  # 14 days

  tags = local.tags
}

# ============================================================================
# S3 BUCKET FOR MWAA DAGS
# ============================================================================

resource "aws_s3_bucket" "mwaa_dags" {
  bucket = "${local.project_name}-mwaa-dags-${data.aws_caller_identity.current.account_id}"

  tags = local.tags
}

resource "aws_s3_bucket_versioning" "mwaa_dags_versioning" {
  bucket = aws_s3_bucket.mwaa_dags.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mwaa_dags_encryption" {
  bucket = aws_s3_bucket.mwaa_dags.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "mwaa_dags_public_access" {
  bucket = aws_s3_bucket.mwaa_dags.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload DAG to MWAA bucket
resource "aws_s3_object" "olist_dag" {
  bucket = aws_s3_bucket.mwaa_dags.id
  key    = "dags/olist_data_pipeline.py"
  source = "${path.root}/../src/airflow/dags/olist_data_pipeline.py"
  etag   = filemd5("${path.root}/../src/airflow/dags/olist_data_pipeline.py")

  tags = local.tags
}

# ============================================================================
# IAM ROLE FOR MWAA
# ============================================================================

resource "aws_iam_role" "mwaa_execution_role" {
  name = "${local.project_name}-mwaa-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "airflow-env.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "airflow.amazonaws.com"
        }
      }
    ]
  })

  tags = local.tags
}

# MWAA can read DAGs from S3
resource "aws_iam_role_policy" "mwaa_dags_policy" {
  name = "${local.project_name}-mwaa-dags-policy"
  role = aws_iam_role.mwaa_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.mwaa_dags.arn,
          "${aws_s3_bucket.mwaa_dags.arn}/*",
        ]
      }
    ]
  })
}

# MWAA can trigger Glue jobs
resource "aws_iam_role_policy" "mwaa_glue_policy" {
  name = "${local.project_name}-mwaa-glue-policy"
  role = aws_iam_role.mwaa_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJob",
          "glue:ListJobs",
        ]
        Resource = "*"
      }
    ]
  })
}

# MWAA can access S3 buckets
resource "aws_iam_role_policy" "mwaa_s3_policy" {
  name = "${local.project_name}-mwaa-s3-policy"
  role = aws_iam_role.mwaa_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
        ]
        Resource = [
          "arn:aws:s3:::${local.project_name}-*",
          "arn:aws:s3:::${local.project_name}-*/*",
        ]
      }
    ]
  })
}

# MWAA can read from Iceberg via Glue
resource "aws_iam_role_policy" "mwaa_iceberg_policy" {
  name = "${local.project_name}-mwaa-iceberg-policy"
  role = aws_iam_role.mwaa_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetTable",
          "glue:GetPartitions",
        ]
        Resource = "*"
      }
    ]
  })
}

# ============================================================================
# S3 BUCKET FOR MLFLOW (Model Registry)
# ============================================================================

resource "aws_s3_bucket" "mlflow" {
  bucket = "${local.project_name}-mlflow-${data.aws_caller_identity.current.account_id}"

  tags = local.tags
}

resource "aws_s3_bucket_versioning" "mlflow_versioning" {
  bucket = aws_s3_bucket.mlflow.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "mlflow_encryption" {
  bucket = aws_s3_bucket.mlflow.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "mlflow_lifecycle" {
  bucket = aws_s3_bucket.mlflow.id

  rule {
    id     = "archive-old-models"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 180
    }

    transition {
      days          = 30
      storage_class = "GLACIER"
    }
  }
}

# ============================================================================
# OUTPUTS
# ============================================================================

output "mwaa_dags_bucket" {
  value       = aws_s3_bucket.mwaa_dags.id
  description = "S3 bucket for MWAA DAGs"
}

output "mlflow_bucket" {
  value       = aws_s3_bucket.mlflow.id
  description = "S3 bucket for MLflow model registry"
}

output "mwaa_execution_role_arn" {
  value       = aws_iam_role.mwaa_execution_role.arn
  description = "ARN of MWAA execution role"
}

output "eventbridge_rule_name" {
  value       = aws_cloudwatch_event_rule.mwaa_dag_schedule.name
  description = "EventBridge rule for DAG triggering"
}
