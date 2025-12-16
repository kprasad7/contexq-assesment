# Glue Service Role
resource "aws_iam_role" "glue_service_role" {
  name                 = var.role_name
  assume_role_policy   = data.aws_iam_policy_document.assume_role.json
  description          = "Service role for AWS Glue jobs"
  max_session_duration = 3600

  tags = var.tags
}

# Trust Policy for Glue Service
data "aws_iam_policy_document" "assume_role" {
  version = "2012-10-17"

  statement {
    sid     = "AllowGlueAssumeRole"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

# S3 Access Policy
resource "aws_iam_policy" "s3_access" {
  name        = "${var.role_name}-s3"
  description = "S3 bucket access for Glue jobs"
  policy      = data.aws_iam_policy_document.s3_access.json

  tags = var.tags
}

data "aws_iam_policy_document" "s3_access" {
  version = "2012-10-17"

  statement {
    sid    = "ListBuckets"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
      "s3:GetBucketVersioning"
    ]
    resources = [
      var.raw_bucket_arn,
      var.processed_bucket_arn,
      var.mlflow_bucket_arn
    ]
  }

  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${var.raw_bucket_arn}/*",
      "${var.processed_bucket_arn}/*",
      "${var.mlflow_bucket_arn}/*"
    ]
  }

  statement {
    sid    = "ListAllBuckets"
    effect = "Allow"
    actions = [
      "s3:ListAllMyBuckets"
    ]
    resources = ["*"]
  }
}

# Glue Catalog Access Policy
resource "aws_iam_policy" "glue_catalog_access" {
  name        = "${var.role_name}-glue-catalog"
  description = "Glue Catalog access for table operations"
  policy      = data.aws_iam_policy_document.glue_catalog_access.json

  tags = var.tags
}

data "aws_iam_policy_document" "glue_catalog_access" {
  version = "2012-10-17"

  statement {
    sid    = "GlueCatalogRead"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetDatabases"
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:catalog",
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/*"
    ]
  }

  statement {
    sid    = "GlueCatalogWrite"
    effect = "Allow"
    actions = [
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchCreatePartition",
      "glue:BatchUpdatePartition",
      "glue:BatchDeletePartition"
    ]
    resources = [
      "arn:aws:glue:${var.region}:${var.account_id}:database/${var.glue_database_name}",
      "arn:aws:glue:${var.region}:${var.account_id}:table/${var.glue_database_name}/*"
    ]
  }
}

# CloudWatch Logs Policy
resource "aws_iam_policy" "cloudwatch_logs" {
  name        = "${var.role_name}-logs"
  description = "CloudWatch Logs access for Glue jobs"
  policy      = data.aws_iam_policy_document.cloudwatch_logs.json

  tags = var.tags
}

data "aws_iam_policy_document" "cloudwatch_logs" {
  version = "2012-10-17"

  statement {
    sid    = "CreateLogGroup"
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup"
    ]
    resources = [
      "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/glue/*"
    ]
  }

  statement {
    sid    = "ManageLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams"
    ]
    resources = [
      "arn:aws:logs:${var.region}:${var.account_id}:log-group:/aws/glue/*:*"
    ]
  }
}

# Attach AWS managed policy
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Attach custom policies
resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.s3_access.arn
}

resource "aws_iam_role_policy_attachment" "glue_catalog_access" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_catalog_access.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch_logs" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
}

data "aws_caller_identity" "current" {}
