# Remote state configuration for team environments and CI/CD pipelines
terraform {
  backend "s3" {
    bucket         = "contexq-terraform-state-119287772129"
    key            = "contexq-dev/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "contexq-terraform-locks"
  }
}

# S3 bucket and DynamoDB table are now created. To migrate state:
# terraform init -migrate-state

