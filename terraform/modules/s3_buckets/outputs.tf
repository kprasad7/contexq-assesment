output "raw_bucket_id" {
  description = "ID of raw data bucket"
  value       = aws_s3_bucket.raw_data.id
}

output "raw_bucket_name" {
  description = "Name of raw data bucket"
  value       = aws_s3_bucket.raw_data.bucket
}

output "raw_bucket_arn" {
  description = "ARN of raw data bucket"
  value       = aws_s3_bucket.raw_data.arn
}

output "processed_bucket_id" {
  description = "ID of processed data bucket"
  value       = aws_s3_bucket.processed_data.id
}

output "processed_bucket_name" {
  description = "Name of processed data bucket"
  value       = aws_s3_bucket.processed_data.bucket
}

output "processed_bucket_arn" {
  description = "ARN of processed data bucket"
  value       = aws_s3_bucket.processed_data.arn
}

output "mlflow_bucket_id" {
  description = "ID of MLflow artifacts bucket"
  value       = aws_s3_bucket.mlflow_artifacts.id
}

output "mlflow_bucket_name" {
  description = "Name of MLflow artifacts bucket"
  value       = aws_s3_bucket.mlflow_artifacts.bucket
}

output "mlflow_bucket_arn" {
  description = "ARN of MLflow artifacts bucket"
  value       = aws_s3_bucket.mlflow_artifacts.arn
}

output "logs_bucket_id" {
  description = "ID of logs bucket"
  value       = aws_s3_bucket.logs.id
}

output "logs_bucket_arn" {
  description = "ARN of logs bucket"
  value       = aws_s3_bucket.logs.arn
}
