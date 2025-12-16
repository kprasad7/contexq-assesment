# Terraform module to create a Glue job for data preparation.
# Adds to existing infrastructure.

resource "aws_glue_job" "data_preparation" {
  name              = "${local.project_name}-data-prep"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = local.glue_worker_count

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.raw_data.id}/glue-scripts/data_preparation_job.py"
    python_version  = "3.9"
  }

  default_arguments = {
    "--TempDir"                    = "s3://${aws_s3_bucket.raw_data.id}/glue-temp/"
    "--job-bookmark-option"        = "job-bookmark-enabled"
    "--enable-glue-datacatalog"    = "true"
    "--job-language"               = "python"
    "--source_bucket"              = aws_s3_bucket.raw_data.id
    "--target_bucket"              = aws_s3_bucket.processed_data.id
  }

  execution_property {
    max_concurrent_runs = 1
  }

  tags = local.tags

  depends_on = [
    aws_s3_object.glue_data_prep_script,
  ]
}

# S3 object for data prep script
resource "aws_s3_object" "glue_data_prep_script" {
  bucket = aws_s3_bucket.raw_data.id
  key    = "glue-scripts/data_preparation_job.py"
  source = "${path.root}/../src/spark/data_preparation_job.py"
  etag   = filemd5("${path.root}/../src/spark/data_preparation_job.py")

  tags = local.tags
}

# CloudWatch trigger for data preparation (runs at 2 AM UTC daily)
resource "aws_cloudwatch_event_rule" "data_prep_schedule" {
  name                = "${local.project_name}-data-prep-schedule"
  description         = "Trigger data preparation job daily at 2 AM UTC"
  schedule_expression = "cron(0 2 * * ? *)"

  tags = local.tags
}

resource "aws_cloudwatch_event_target" "data_prep_glue" {
  rule       = aws_cloudwatch_event_rule.data_prep_schedule.name
  target_id  = "DataPrepGlueJob"
  arn        = "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${aws_glue_job.data_preparation.name}"
  role_arn   = aws_iam_role.glue_service_role.arn
  
  dead_letter_config {
    arn = aws_sqs_queue.dlq.arn
  }
}

# DLQ for failed job executions
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.project_name}-data-prep-dlq"
  message_retention_seconds = 1209600  # 14 days

  tags = local.tags
}

# CloudWatch log group for data prep job
resource "aws_cloudwatch_log_group" "data_prep_logs" {
  name              = "/aws/glue/${local.project_name}-data-prep"
  retention_in_days = local.glue_log_retention_days

  tags = local.tags
}

# Output
output "data_preparation_job_name" {
  value       = aws_glue_job.data_preparation.name
  description = "Name of the data preparation Glue job"
}
