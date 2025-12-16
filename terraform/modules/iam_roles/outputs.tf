output "glue_service_role_arn" {
  description = "ARN of Glue service role"
  value       = aws_iam_role.glue_service_role.arn
}

output "glue_service_role_name" {
  description = "Name of Glue service role"
  value       = aws_iam_role.glue_service_role.name
}

output "glue_service_role_id" {
  description = "ID of Glue service role"
  value       = aws_iam_role.glue_service_role.id
}

output "s3_policy_arn" {
  description = "ARN of S3 access policy"
  value       = aws_iam_policy.s3_access.arn
}

output "glue_catalog_policy_arn" {
  description = "ARN of Glue Catalog policy"
  value       = aws_iam_policy.glue_catalog_access.arn
}

output "cloudwatch_logs_policy_arn" {
  description = "ARN of CloudWatch Logs policy"
  value       = aws_iam_policy.cloudwatch_logs.arn
}
