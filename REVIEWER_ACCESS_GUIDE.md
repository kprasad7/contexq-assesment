# 🔐 AWS Access Guide for Technical Reviewers

## Overview
This document provides instructions for reviewing the ContextQ Data & AI Engineering project on AWS.

**Important**: Read-only credentials provided separately via secure email channel.

---

## 🔑 Credentials (Provided via Email)

```
IAM Username: contexq-reviewer
AWS Account ID: 119287772129
AWS Region: us-east-1
Access Key ID: [provided in email]
Secret Access Key: [provided in email]
Policy: ContextQProjectReviewPolicy (read-only)
```

**⚠️ Security Note**: Keep credentials secure. Valid for 90 days.

---

## 🚀 Quick Setup

### Option 1: AWS CLI
```bash
# Configure with credentials from email
aws configure set aws_access_key_id [YOUR_KEY]
aws configure set aws_secret_access_key [YOUR_SECRET]
aws configure set default.region us-east-1

# Verify
aws sts get-caller-identity
```

### Option 2: Environment Variables
```bash
export AWS_ACCESS_KEY_ID=[YOUR_KEY]
export AWS_SECRET_ACCESS_KEY=[YOUR_SECRET]
export AWS_DEFAULT_REGION=us-east-1
```

### Option 3: AWS Console
1. Visit: https://console.aws.amazon.com/
2. Select: IAM user
3. Account ID: **119287772129**
4. Username: **contexq-reviewer**
5. Password: Use CLI to set if needed

---

## 🖥️ AWS Console Verification Guide

### 1. S3 Buckets - Data Storage

**Navigate**: https://s3.console.aws.amazon.com/s3/buckets?region=us-east-1

**Search**: `contexq`

**Verify**:
- `contexq-dev-raw-data-*`: Source CSVs + PySpark scripts
- `contexq-dev-processed-data-*`: Iceberg warehouse (146+ parquet files)
- `contexq-dev-mlflow-artifacts-*`: Model artifacts and metrics
- `contexq-dev-mwaa-dags-*`: Airflow DAG files

### 2. AWS Glue Jobs

**Navigate**: https://console.aws.amazon.com/glue/home?region=us-east-1#/v2/etl-jobs

**Check**:
- **contexq-dev-etl**: Entity resolution job
  - Status: SUCCEEDED
  - Runtime: ~181s
  - Workers: 10 x G.2X
- **contexq-dev-ml-training**: ML training job
  - Status: SUCCEEDED
  - Runtime: ~165s
  - AUC: 0.944, F1: 0.868

### 3. Glue Catalog - Iceberg Table

**Navigate**: https://console.aws.amazon.com/glue/home?region=us-east-1#/v2/data-catalog/databases

**Verify**:
- Database: `contexq_dev`
- Table: `corporate_registry`
- Format: Apache Iceberg
- Partitions: year/month

### 4. Amazon Athena - Query Data

**Navigate**: https://console.aws.amazon.com/athena/home?region=us-east-1#/query-editor

**Sample Queries**:
```sql
-- View sample records
SELECT * FROM contexq_dev.corporate_registry LIMIT 10;

-- Aggregate stats
SELECT COUNT(*) as total,
       AVG(revenue) as avg_revenue,
       AVG(profit) as avg_profit
FROM contexq_dev.corporate_registry;

-- High-profit corporations
SELECT corporate_name, revenue, profit
FROM contexq_dev.corporate_registry
WHERE profit > 200
ORDER BY profit DESC LIMIT 20;
```

### 5. CloudWatch Logs

**Navigate**: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups

**Log Groups**:
- `/aws/glue/contexq-dev-etl`: ETL execution logs
- `/aws/glue/contexq-dev-ml-training`: ML training logs

**Search for**:
- "AUC: 0.944"
- "F1-Score: 0.868"
- "Processed 3,095 records"
- "Job run succeeded"

### 6. IAM Roles

**Navigate**: https://console.aws.amazon.com/iam/home?region=us-east-1#/roles

**Check**:
- `contexq-dev-glue-service-role`: Glue execution role
- Attached policies for S3, CloudWatch, Glue

---

## 📊 CLI Verification Commands

### S3 Data
```bash
# List buckets
aws s3 ls | grep contexq

# View raw data
aws s3 ls s3://contexq-dev-raw-data-119287772129/source_supply/ --recursive

# View Iceberg data
aws s3 ls s3://contexq-dev-processed-data-119287772129/warehouse/corporate_registry/ --recursive
```

### Glue Jobs
```bash
# List jobs
aws glue get-jobs --query 'Jobs[].Name'

# Get ETL job details
aws glue get-job --job-name contexq-dev-etl

# View recent runs
aws glue get-job-runs --job-name contexq-dev-etl --max-results 5
```

### Glue Catalog
```bash
# Get database
aws glue get-database --name contexq_dev

# Get table schema
aws glue get-table --database-name contexq_dev --name corporate_registry
```

### CloudWatch Logs
```bash
# List log groups
aws logs describe-log-groups --log-group-name-prefix /aws/glue/contexq

# Tail ETL logs
aws logs tail /aws/glue/contexq-dev-etl --follow

# Search for metrics
aws logs filter-log-events \
  --log-group-name /aws/glue/contexq-dev-ml-training \
  --filter-pattern "AUC" \
  --max-items 10
```

---

## 🎯 Key Metrics

### ETL Job
- Execution: 181 seconds
- Records: 3,095 processed
- Output: 146+ Parquet files
- Status: SUCCEEDED

### ML Training
- Execution: 165 seconds
- AUC: 0.944 (94.4%)
- F1-Score: 0.868 (86.8%)
- Status: SUCCEEDED

### Infrastructure
- Terraform Resources: 38+
- S3 Buckets: 4
- Glue Jobs: 2
- Iceberg Table: Partitioned by year/month

---

## 📋 Verification Checklist

- [ ] View raw CSV files in S3
- [ ] Check Glue job configurations
- [ ] Review job execution logs
- [ ] Query Iceberg table via Athena
- [ ] Check MLflow artifacts
- [ ] Inspect CloudWatch metrics
- [ ] Verify IAM permissions

---

## 🔗 Quick Links

- **Repository**: https://github.com/kprasad7/contexq-assesment
- **S3 Console**: https://s3.console.aws.amazon.com/s3/buckets?region=us-east-1
- **Glue Console**: https://console.aws.amazon.com/glue/home?region=us-east-1
- **Athena Console**: https://console.aws.amazon.com/athena/home?region=us-east-1
- **CloudWatch**: https://console.aws.amazon.com/cloudwatch/home?region=us-east-1

---

## 🆘 Support

**Issues?** Contact: prasadlvv049@gmail.com


