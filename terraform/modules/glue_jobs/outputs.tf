output "job_name" {
  description = "Name of the primary Glue job"
  value       = aws_glue_job.etl_job.name
}

output "job_arn" {
  description = "ARN of the primary Glue job"
  value       = aws_glue_job.etl_job.arn
}

output "job_id" {
  description = "ID of the primary Glue job"
  value       = aws_glue_job.etl_job.id
}

output "dq_job_name" {
  description = "Name of the data quality Glue job"
  value       = aws_glue_job.data_quality_job.name
}

output "dq_job_arn" {
  description = "ARN of the data quality job"
  value       = aws_glue_job.data_quality_job.arn
}

output "log_group_name" {
  description = "CloudWatch log group name"
  value       = aws_cloudwatch_log_group.glue_jobs.name
}

output "log_group_arn" {
  description = "CloudWatch log group ARN"
  value       = aws_cloudwatch_log_group.glue_jobs.arn
}

output "trigger_name" {
  description = "Name of the scheduled trigger"
  value       = aws_glue_trigger.etl_schedule.name
}

output "trigger_arn" {
  description = "ARN of the scheduled trigger"
  value       = aws_glue_trigger.etl_schedule.arn
}
