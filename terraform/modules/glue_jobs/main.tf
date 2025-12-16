# CloudWatch Log Group for Glue Jobs
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = var.log_group_name
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# Primary Glue ETL Job
resource "aws_glue_job" "etl_job" {
  name         = var.job_name
  role_arn     = var.role_arn
  glue_version = var.glue_version
  timeout      = var.timeout_minutes
  max_retries  = var.max_retries
  description  = "ETL job for corporate data harmonization and Iceberg merge"

  worker_type       = var.worker_type
  number_of_workers = var.num_workers

  command {
    name            = "glueetl"
    script_location = var.script_location
    python_version  = var.python_version
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = var.temp_dir
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = var.temp_dir
    "--job-language"                     = "python"

    # Custom parameters for Glue job
    "--source_bucket"         = var.source_bucket_name
    "--target_bucket"         = var.target_bucket_name
    "--database"              = var.database_name
    "--table"                 = var.table_name
    "--output-partition-keys" = "year,month"
    "--catalog"               = "glue"
  }

  tags = var.tags

  depends_on = [aws_cloudwatch_log_group.glue_jobs]
}

# Optional: Data Quality Job
resource "aws_glue_job" "data_quality_job" {
  name         = "${var.job_name}-dq"
  role_arn     = var.role_arn
  glue_version = var.glue_version
  timeout      = var.timeout_minutes
  max_retries  = 0
  description  = "Data quality checks for corporate registry"

  worker_type       = var.worker_type
  number_of_workers = var.num_workers

  command {
    name            = "glueetl"
    script_location = "${var.script_location}data-quality/"
    python_version  = var.python_version
  }

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--TempDir"                          = var.temp_dir
    "--database"                         = var.database_name
    "--table"                            = var.table_name
  }

  tags = var.tags

  depends_on = [aws_cloudwatch_log_group.glue_jobs]
}

# Trigger for scheduled execution (optional)
resource "aws_glue_trigger" "etl_schedule" {
  name              = "${var.job_name}-scheduled"
  type              = "SCHEDULED"
  schedule          = "cron(0 2 * * ? *)" # Run at 2 AM UTC daily
  description       = "Daily scheduled trigger for ETL job"
  start_on_creation = true

  actions {
    job_name = aws_glue_job.etl_job.name
  }

  tags = var.tags
}
