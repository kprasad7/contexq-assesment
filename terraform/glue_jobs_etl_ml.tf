# Terraform configuration for ETL and ML training Glue jobs.
# Adds to existing infrastructure in terraform/ directory.

# ETL JOB - Entity Resolution & Iceberg Merge
resource "aws_glue_job" "etl" {
  name              = "${local.project_name}-etl"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = local.glue_worker_count

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw_data.id}/glue-scripts/etl_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                    = "s3://${aws_s3_bucket.raw_data.id}/glue-temp/"
    "--job-bookmark-option"        = "job-bookmark-enabled"
    "--enable-glue-datacatalog"    = "true"
    "--enable-metrics"             = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-language"               = "python"
    "--source_bucket"              = aws_s3_bucket.raw_data.id
    "--target_bucket"              = aws_s3_bucket.processed_data.id
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags

  depends_on = [
    aws_glue_job.data_preparation,
    aws_glue_catalog_table.corporate_registry,
  ]
}

# ETL JOB - CloudWatch Trigger (runs 6 AM UTC, after data prep at 2 AM)
resource "aws_cloudwatch_event_rule" "etl_schedule" {
  name                = "${local.project_name}-etl-schedule"
  description         = "Trigger ETL job daily at 6 AM UTC (after data prep)"
  schedule_expression = "cron(0 6 * * ? *)"

  tags = local.tags
}

resource "aws_cloudwatch_event_target" "etl_glue" {
  rule       = aws_cloudwatch_event_rule.etl_schedule.name
  target_id  = "ETLGlueJob"
  arn        = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.etl.name}"
  role_arn   = aws_iam_role.glue_service_role.arn

  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

# ETL CloudWatch Logs
resource "aws_cloudwatch_log_group" "etl_logs" {
  name              = "/aws/glue/${local.project_name}-etl"
  retention_in_days = local.glue_log_retention_days

  tags = local.tags
}

# ============================================================================
# ML TRAINING JOB - Profit Prediction Model
# ============================================================================

resource "aws_glue_job" "ml_training" {
  name              = "${local.project_name}-ml-training"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = local.glue_worker_count

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.raw_data.id}/glue-scripts/ml_training_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--TempDir"                    = "s3://${aws_s3_bucket.raw_data.id}/glue-temp/"
    "--job-bookmark-option"        = "job-bookmark-disabled"
    "--enable-glue-datacatalog"    = "true"
    "--enable-metrics"             = "true"
    "--job-language"               = "python"
    "--mlflow_tracking_uri"        = "http://localhost:5000"  # Update with MWAA ALB
    "--experiment_name"            = "olist-profit-prediction"
    "--additional-python-modules"  = "mlflow==2.8.0"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags

  depends_on = [
    aws_glue_job.etl,
  ]
}

# ML Training CloudWatch Logs
resource "aws_cloudwatch_log_group" "ml_training_logs" {
  name              = "/aws/glue/${local.project_name}-ml-training"
  retention_in_days = local.glue_log_retention_days

  tags = local.tags
}

# ============================================================================
# GLUE CATALOG - Corporate Registry Iceberg Table
# ============================================================================

resource "aws_glue_catalog_database" "contexq" {
  name = local.glue_database_name

  description = "OLIST Data Pipeline Iceberg Tables"

  tags = local.tags
}

resource "aws_glue_catalog_table" "corporate_registry" {
  name          = "corporate_registry"
  database_name = aws_glue_catalog_database.contexq.name
  table_type    = "ICEBERG"

  description = "Deduplicated and harmonized corporate entities from OLIST supply chain and financial data"

  parameters = {
    "EXTERNAL_TABLE_VERSION" = "2"
    "classification"         = "iceberg"
    "iceberg.format.version" = "2"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.processed_data.id}/iceberg/corporate_registry/"
    input_format  = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat"
    output_format = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat"

    serde_info {
      serialization_library = "org.apache.iceberg.mr.hive.serde.HiveIcebergSerDe"
    }

    columns {
      name    = "corporate_id"
      type    = "string"
      comment = "Unique identifier for entity"
    }

    columns {
      name    = "corporate_name"
      type    = "string"
      comment = "Name of company/supplier"
    }

    columns {
      name    = "address"
      type    = "string"
      comment = "Street address"
    }

    columns {
      name    = "city"
      type    = "string"
      comment = "City name"
    }

    columns {
      name    = "state"
      type    = "string"
      comment = "State/province code"
    }

    columns {
      name    = "activity_places"
      type    = "int"
      comment = "Number of locations"
    }

    columns {
      name    = "top_suppliers"
      type    = "array<string>"
      comment = "List of top supplier/product IDs"
    }

    columns {
      name    = "main_customers"
      type    = "string"
      comment = "Primary customer segment"
    }

    columns {
      name    = "revenue"
      type    = "decimal(18,2)"
      comment = "Total revenue in BRL"
    }

    columns {
      name    = "profit"
      type    = "decimal(18,2)"
      comment = "Calculated profit in BRL"
    }

    columns {
      name    = "source_system"
      type    = "string"
      comment = "Data source (olist_supply_chain, olist_financial)"
    }

    columns {
      name    = "load_date"
      type    = "timestamp"
      comment = "Timestamp of data load"
    }

    columns {
      name    = "entity_hash"
      type    = "string"
      comment = "MD5 hash of corporate_name + city for deduplication"
    }
  }

  tags = local.tags

  depends_on = [aws_glue_catalog_database.contexq]
}

# ============================================================================
# S3 BUCKET FOR GLUE SCRIPTS
# ============================================================================

resource "aws_s3_object" "glue_etl_script" {
  bucket = aws_s3_bucket.raw_data.id
  key    = "glue-scripts/etl_job.py"
  source = "${path.root}/../src/spark/etl_job.py"
  etag   = filemd5("${path.root}/../src/spark/etl_job.py")

  tags = local.tags
}

resource "aws_s3_object" "glue_ml_script" {
  bucket = aws_s3_bucket.raw_data.id
  key    = "glue-scripts/ml_training_job.py"
  source = "${path.root}/../src/spark/ml_training_job.py"
  etag   = filemd5("${path.root}/../src/spark/ml_training_job.py")

  tags = local.tags
}

# ============================================================================
# OUTPUTS
# ============================================================================

output "etl_job_name" {
  value       = aws_glue_job.etl.name
  description = "Name of the ETL Glue job"
}

output "ml_training_job_name" {
  value       = aws_glue_job.ml_training.name
  description = "Name of the ML training Glue job"
}

output "corporate_registry_table_name" {
  value       = aws_glue_catalog_table.corporate_registry.name
  description = "Name of the corporate registry Iceberg table"
}
