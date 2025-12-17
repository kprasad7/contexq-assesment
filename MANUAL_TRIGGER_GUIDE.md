# Manual Pipeline Trigger & Verification Guide

## ✅ Infrastructure Status Summary

All 4 Glue jobs are now **DEPLOYED**:
- ✅ `contexq-dev-data-prep` - CSV to Parquet conversion
- ✅ `contexq-dev-etl` - Entity resolution & Iceberg merge
- ✅ `contexq-dev-etl-dq` - Data quality validation
- ✅ `contexq-dev-ml-training` - ML model training

---

## 🚀 Manual Pipeline Trigger via AWS CLI

### Quick Start (Single Command)

Trigger the ETL job immediately:
```bash
aws glue start-job-run \
  --job-name contexq-dev-etl \
  --region us-east-1
```

### Sequential Pipeline Execution

Run the full data pipeline in order:

```bash
# 1. Data Preparation (CSV → Parquet)
echo "Starting Data Preparation..."
JOB_RUN_1=$(aws glue start-job-run \
  --job-name contexq-dev-data-prep \
  --region us-east-1 \
  --query 'JobRunId' \
  --output text)
echo "✓ Data Prep Job Started: $JOB_RUN_1"

# Wait for Data Prep to complete (~2-5 minutes)
echo "Waiting for Data Prep to complete..."
aws glue get-job-run \
  --job-name contexq-dev-data-prep \
  --run-id "$JOB_RUN_1" \
  --region us-east-1 \
  --query 'JobRun.State' \
  --output text

# 2. ETL Job (Entity Resolution + Iceberg MERGE)
echo "Starting ETL Job..."
JOB_RUN_2=$(aws glue start-job-run \
  --job-name contexq-dev-etl \
  --region us-east-1 \
  --query 'JobRunId' \
  --output text)
echo "✓ ETL Job Started: $JOB_RUN_2"

# 3. ML Training Job (Optional - after Iceberg data is available)
echo "Starting ML Training..."
JOB_RUN_3=$(aws glue start-job-run \
  --job-name contexq-dev-ml-training \
  --region us-east-1 \
  --query 'JobRunId' \
  --output text)
echo "✓ ML Training Job Started: $JOB_RUN_3"
```

---

## 📊 Monitor Job Execution

### Real-Time Logs

```bash
# Watch ETL job logs as they execute
aws logs tail /aws/glue/contexq-dev-etl --follow --region us-east-1

# Or for Data Prep
aws logs tail /aws/glue/contexq-dev-data-prep --follow --region us-east-1

# Or for ML Training
aws logs tail /aws/glue/contexq-dev-ml-training --follow --region us-east-1
```

### Check Job Status

```bash
# Get current state of a job run
# (Replace RUN_ID with the ID returned from start-job-run)
aws glue get-job-run \
  --job-name contexq-dev-etl \
  --run-id <RUN_ID> \
  --region us-east-1 \
  --query 'JobRun.[State,StartedOn,CompletedOn]' \
  --output text

# Example output: SUCCEEDED 2024-01-15T10:30:00 2024-01-15T10:35:45
```

### List Recent Job Runs

```bash
# Show last 5 runs of ETL job
aws glue list-job-runs \
  --job-name contexq-dev-etl \
  --region us-east-1 \
  --query 'JobRuns[0:5].[Id,State,StartedOn,CompletedOn]' \
  --output table
```

### Get Job Run Details

```bash
# Full details of a job run
aws glue get-job-run \
  --job-name contexq-dev-etl \
  --run-id <RUN_ID> \
  --region us-east-1 | jq '.JobRun'
```

---

## ✅ Verify Data Flow

### 1. Data Preparation Output (After Data Prep Job)

```bash
# Check Parquet files in processed-data bucket
aws s3 ls s3://contexq-dev-processed-data-119287772129/ \
  --recursive \
  --human-readable \
  --summarize
```

Expected output:
```
2024-01-15 10:35:45         0.0 B processed/order_items/
2024-01-15 10:35:45    15.3 MiB processed/order_items/part-00000.parquet
2024-01-15 10:35:45    12.8 MiB processed/order_payments/part-00000.parquet
```

### 2. Iceberg Table Contents (After ETL Job)

```bash
# Verify Iceberg table metadata
aws glue get-table \
  --database-name contexq_dev \
  --name corporate_registry \
  --region us-east-1 | jq '.Table'

# Count records in Iceberg
aws s3 ls s3://contexq-dev-processed-data-119287772129/warehouse/corporate_registry/ \
  --recursive \
  --human-readable
```

### 3. MLflow Models (After ML Training Job)

```bash
# Check models saved to MLflow artifacts
aws s3 ls s3://contexq-dev-mlflow-artifacts-119287772129/ \
  --recursive \
  --human-readable \
  --summarize
```

Expected output:
```
2024-01-15 10:40:15        2.5 MiB 0/artifacts/profit_prediction_model/model.pkl
2024-01-15 10:40:15      42.0 KiB 0/artifacts/profit_prediction_model/requirements.txt
```

---

## 🔄 Continuous Pipeline Execution

### Enable Automatic Scheduling (EventBridge)

Create EventBridge rules to trigger jobs on a schedule:

```bash
# Rule 1: Data Prep daily at 2:00 AM UTC
aws events put-rule \
  --name contexq-data-prep-schedule \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED \
  --region us-east-1

aws events put-targets \
  --rule contexq-data-prep-schedule \
  --targets \
    "Id=1" \
    "Arn=arn:aws:glue:us-east-1:119287772129:job/contexq-dev-data-prep" \
    "RoleArn=arn:aws:iam::119287772129:role/contexq-dev-glue-service-role" \
  --region us-east-1

# Rule 2: ETL daily at 6:00 AM UTC (4 hours after data prep)
aws events put-rule \
  --name contexq-etl-schedule \
  --schedule-expression "cron(0 6 * * ? *)" \
  --state ENABLED \
  --region us-east-1

aws events put-targets \
  --rule contexq-etl-schedule \
  --targets \
    "Id=1" \
    "Arn=arn:aws:glue:us-east-1:119287772129:job/contexq-dev-etl" \
    "RoleArn=arn:aws:iam::119287772129:role/contexq-dev-glue-service-role" \
  --region us-east-1

# Rule 3: ML Training daily at 7:00 AM UTC (1 hour after ETL)
aws events put-rule \
  --name contexq-ml-training-schedule \
  --schedule-expression "cron(0 7 * * ? *)" \
  --state ENABLED \
  --region us-east-1

aws events put-targets \
  --rule contexq-ml-training-schedule \
  --targets \
    "Id=1" \
    "Arn=arn:aws:glue:us-east-1:119287772129:job/contexq-dev-ml-training" \
    "RoleArn=arn:aws:iam::119287772129:role/contexq-dev-glue-service-role" \
  --region us-east-1

# Verify rules created
aws events list-rules --region us-east-1 --output table
```

### Check EventBridge Rule Status

```bash
# List all EventBridge rules
aws events list-rules --region us-east-1 --output table

# Get targets for a specific rule
aws events list-targets-by-rule \
  --rule contexq-etl-schedule \
  --region us-east-1 | jq '.Targets'

# Disable a rule (if needed)
aws events disable-rule --name contexq-etl-schedule --region us-east-1

# Enable a rule (if needed)
aws events enable-rule --name contexq-etl-schedule --region us-east-1
```

---

## 📈 Advanced Monitoring

### CloudWatch Metrics

```bash
# Get metrics for Glue job
aws cloudwatch get-metric-statistics \
  --namespace AWS/Glue \
  --metric-name glue.driver.aggregate.numFailedTasks \
  --dimensions Name=job_name,Value=contexq-dev-etl \
  --start-time 2024-01-15T00:00:00Z \
  --end-time 2024-01-15T23:59:59Z \
  --period 3600 \
  --region us-east-1 \
  --statistics Sum Average
```

### Iceberg Table History

```bash
# View Iceberg table snapshots (versions)
aws glue get-table-versions \
  --database-name contexq_dev \
  --table-name corporate_registry \
  --region us-east-1 | jq '.TableVersions[] | {UpdateTime, VersionId}'
```

---

## 🐛 Troubleshooting

### Job Failed - Check Logs

```bash
# Get error details from CloudWatch
aws logs get-log-events \
  --log-group-name /aws/glue/contexq-dev-etl \
  --log-stream-name "<JOB_RUN_ID>" \
  --region us-east-1 | jq '.events[-10:]'  # Last 10 events
```

### S3 Permissions Error

```bash
# Verify IAM role has S3 access
aws iam get-role-policy \
  --role-name contexq-dev-glue-service-role \
  --policy-name glue-s3-policy \
  --region us-east-1 | jq '.RolePolicyDocument'
```

### Iceberg Merge Failed

```bash
# Check table metadata
aws glue get-table \
  --database-name contexq_dev \
  --name corporate_registry \
  --region us-east-1 | jq '.Table.Parameters'

# View recent partitions
aws s3 ls s3://contexq-dev-processed-data-119287772129/warehouse/corporate_registry/ \
  --recursive | tail -20
```

---

## 📋 Quick Reference Commands

| Task | Command |
|------|---------|
| **List all Glue jobs** | `aws glue list-jobs --region us-east-1 --output table` |
| **Start ETL job** | `aws glue start-job-run --job-name contexq-dev-etl --region us-east-1` |
| **Get job status** | `aws glue get-job-run --job-name contexq-dev-etl --run-id <ID> --region us-east-1` |
| **Watch logs** | `aws logs tail /aws/glue/contexq-dev-etl --follow --region us-east-1` |
| **List S3 data** | `aws s3 ls s3://contexq-dev-processed-data-119287772129/ --recursive` |
| **Check Iceberg** | `aws glue get-table --database-name contexq_dev --name corporate_registry --region us-east-1` |
| **List EventBridge rules** | `aws events list-rules --region us-east-1 --output table` |

---

## 🎯 End-to-End Test Checklist

✅ **Pre-Execution:**
- [ ] All 4 Glue jobs created
- [ ] S3 buckets verified
- [ ] IAM role has permissions
- [ ] Iceberg table exists

✅ **During Execution:**
- [ ] Monitor CloudWatch logs
- [ ] Check job status
- [ ] Verify no errors in logs

✅ **Post-Execution:**
- [ ] Data in processed-data bucket
- [ ] Records in Iceberg table
- [ ] Models in MLflow artifacts
- [ ] CI/CD pipeline triggered (on GitHub push)

---

## 🚀 Production Deployment

For production, the pipeline is fully automated via:

1. **GitHub Actions CI**: Triggered on every PR (47+ tests)
2. **GitHub Actions CD**: Triggered on push to main (auto-deploys to AWS)
3. **EventBridge Scheduler**: Runs jobs on schedule (daily)
4. **MWAA Airflow**: Orchestrates 5-task sequential pipeline

See [CI_CD_PIPELINE.md](CI_CD_PIPELINE.md) for detailed CI/CD documentation.

---

**Created:** 2024  
**Account:** 119287772129  
**Region:** us-east-1  
**Status:** ✅ Production Ready
