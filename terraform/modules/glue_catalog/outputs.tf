output "database_name" {
  description = "Glue database name"
  value       = aws_glue_catalog_database.main.name
}

output "database_arn" {
  description = "Glue database ARN"
  value       = "arn:aws:glue:${var.region}:${var.account_id}:catalog"
}

output "table_name" {
  description = "Iceberg table name"
  value       = aws_glue_catalog_table.corporate_registry.name
}

output "table_arn" {
  description = "Iceberg table ARN"
  value       = "arn:aws:glue:${var.region}:${var.account_id}:table/${aws_glue_catalog_database.main.name}/${aws_glue_catalog_table.corporate_registry.name}"
}

output "table_location" {
  description = "S3 location of the Iceberg table"
  value       = "s3://${var.processed_bucket}/warehouse/${var.table_name}/"
}

output "database_id" {
  description = "Database ID"
  value       = aws_glue_catalog_database.main.id
}

output "table_id" {
  description = "Table ID"
  value       = aws_glue_catalog_table.corporate_registry.id
}
