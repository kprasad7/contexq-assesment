# Remote state configuration
# Uncomment and configure to enable remote state management
# This is recommended for team environments and CI/CD pipelines
#
# terraform {
#   backend "s3" {
#     bucket         = "contexq-terraform-state-prod"
#     key            = "contexq/phase1/terraform.tfstate"
#     region         = "us-east-1"
#     encrypt        = true
#     dynamodb_table = "terraform-lock"
#   }
# }
#
# To set up the backend:
# 1. aws s3api create-bucket --bucket contexq-terraform-state-prod --region us-east-1
# 2. aws s3api put-bucket-versioning --bucket contexq-terraform-state-prod \
#    --versioning-configuration Status=Enabled
# 3. aws dynamodb create-table \
#    --table-name terraform-lock \
#    --attribute-definitions AttributeName=LockID,AttributeType=S \
#    --key-schema AttributeName=LockID,KeyType=HASH \
#    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1
# 4. terraform init -migrate-state
