# OLIST Data Pipeline - Deployment Checklist

## ✓ Phase 2b: Data Preparation Completion Checklist

### Data Upload & Validation ✓ COMPLETE
- [x] Upload olist_order_items_dataset.csv to S3 (15.4 MB)
  - Location: `s3://contexq-dev-raw-data-119287772129/source_supply/`
  - Records: 112,650
  - Columns: 7
  - Null values: 0

- [x] Upload olist_order_payments_dataset.csv to S3 (5.8 MB)
  - Location: `s3://contexq-dev-raw-data-119287772129/source_financial/`
  - Records: 103,886
  - Columns: 5
  - Null values: 0

- [x] Upload olist_sellers_dataset.csv to S3 (174 KB)
  - Location: `s3://contexq-dev-raw-data-119287772129/source_supply/`
  - Records: 3,095
  - Columns: 4
  - Null values: 0

- [x] Verify S3 uploads via `aws s3 ls`
  - Total size: 20.4 MB
  - Total objects: 4 (includes Glue script)

### Data Preparation Job ✓ COMPLETE
- [x] Create `src/spark/data_preparation_job.py` (210 lines)
  - PySpark transformations for Source 1 (supply chain)
  - PySpark transformations for Source 2 (financial)
  - Data quality checks with aggregations
  - Parquet output configuration with snappy compression

- [x] Deploy job to S3 `glue-scripts/data_preparation_job.py`
  - File size: 6.4 KB
  - Upload date: 2025-12-16 19:08:17 UTC
  - Status: Verified in S3

- [x] Create `terraform/glue_jobs_data_prep.tf` (80 lines)
  - AWS Glue job resource definition
  - CloudWatch Event Rule for daily scheduling (2 AM UTC)
  - SQS DLQ for error handling
  - CloudWatch Log Group with 7-day retention

### Data Validation ✓ COMPLETE
- [x] Create `scripts/validate_data_structure.py` (150 lines)
  - Pandas-based data structure validation
  - Null value checks per column
  - Revenue statistics calculation
  - Schema validation against Iceberg schema

- [x] Execute validation script
  - All datasets loaded successfully
  - All null checks passed (0 nulls in all columns)
  - Schema validation: PASSED
  - Revenue calculations: PASSED

### Documentation ✓ COMPLETE
- [x] Create `DATA_PREPARATION_REPORT.md`
  - Data ingestion summary
  - Data quality assessment
  - Entity resolution readiness
  - Next steps and rollback procedures
  - Cost estimation

---

## ⏳ Phase 2c: Glue Job Deployment (PENDING)

### Terraform Deployment
- [ ] Add `glue_jobs_data_prep.tf` to Terraform module
- [ ] Run `terraform init` to update modules
- [ ] Run `terraform plan -target=aws_glue_job.data_preparation`
- [ ] Review CloudWatch and SQS resource additions
- [ ] Run `terraform apply -target=aws_glue_job.data_preparation`
- [ ] Verify Glue job created in AWS console
- [ ] Verify CloudWatch Event Rule scheduled
- [ ] Verify SQS DLQ created

### Manual Job Testing
- [ ] Trigger manual Glue job execution:
  ```bash
  aws glue start-job-run --job-name contexq-dev-data-prep
  ```
- [ ] Monitor job progress via CloudWatch:
  ```bash
  aws logs tail /aws/glue/contexq-dev-data-prep --follow
  ```
- [ ] Verify job completion (check CloudWatch for "Job succeeded")
- [ ] Check S3 output directories:
  ```bash
  aws s3 ls s3://contexq-dev-processed-data-119287772129/prepared_sources/ --recursive
  ```

### Output Verification
- [ ] Source 1 (supply chain) Parquet files exist
  - Location: `prepared_sources/source1_supply/`
  - Expected records: 3,095 (unique suppliers)
  - Format: Parquet with snappy compression

- [ ] Source 2 (financial) Parquet files exist
  - Location: `prepared_sources/source2_financial/`
  - Expected records: 101,686 (payment records)
  - Format: Parquet with snappy compression

- [ ] Data preparation report written
  - Location: `prepared_sources/data_preparation_report/`
  - Format: Parquet summary table

---

## 📋 Phase 2d: ETL Job Deployment (NOT STARTED)

### Preparation
- [ ] Verify ETL job exists: `src/spark/etl_job.py`
- [ ] Verify ML training job exists: `src/spark/ml_training_job.py`
- [ ] Review EntityResolutionEngine implementation
- [ ] Review DataHarmonizer implementation
- [ ] Review IcebergMerger implementation

### Deployment
- [ ] Deploy `etl_job.py` to S3 glue-scripts/
- [ ] Deploy `ml_training_job.py` to S3 glue-scripts/
- [ ] Create Terraform resources for ETL job
- [ ] Create Terraform resources for ML job
- [ ] Run `terraform plan` for ETL resources
- [ ] Run `terraform apply` for ETL resources

### Execution
- [ ] Trigger ETL job manually
- [ ] Monitor CloudWatch logs for entity resolution process
- [ ] Verify Iceberg table created: `corporate_registry`
- [ ] Check Iceberg MERGE INTO operations
- [ ] Validate row counts in Iceberg table

---

## 📊 Phase 2e: ML Pipeline (NOT STARTED)

### Model Training
- [ ] Verify ML training job runs successfully
- [ ] Check MLflow experiment tracking
- [ ] Validate model metrics (AUC, F1-score)
- [ ] Register best model in MLflow registry
- [ ] Test model serving capability

### MWAA Orchestration
- [ ] Deploy Airflow DAG to MWAA S3 bucket
- [ ] Verify DAG appears in MWAA UI
- [ ] Trigger DAG manually
- [ ] Verify 6-hourly schedule is active
- [ ] Check task dependencies and execution flow

---

## 🔄 Phase 3: CI/CD Testing (NOT STARTED)

### GitHub Actions
- [ ] Create PR with test changes
- [ ] Verify CI pipeline runs (pytest, linting, terraform validate)
- [ ] Merge PR to main
- [ ] Verify CD pipeline runs (infrastructure apply, job deployment)
- [ ] Check GitHub Actions logs for success

### Production Readiness
- [ ] Load test the Glue jobs
- [ ] Test scaling with increased worker count
- [ ] Verify cost tracking via Cost Explorer
- [ ] Document runbook for operations team

---

## 📈 Phase 4: Production Deployment (NOT STARTED)

### Infrastructure Hardening
- [ ] Enable Terraform state locking (DynamoDB)
- [ ] Enable S3 versioning backup
- [ ] Configure SNS alerts for job failures
- [ ] Set up CloudWatch dashboards
- [ ] Configure budget alerts

### Data Governance
- [ ] Implement data access controls (IAM policies)
- [ ] Enable S3 encryption at rest (KMS)
- [ ] Configure audit logging (CloudTrail)
- [ ] Document data lineage
- [ ] Create runbooks for incident response

### Monitoring & Observability
- [ ] Set up CloudWatch alarms:
  - [ ] Glue job failure alarm
  - [ ] S3 bucket growth alarm
  - [ ] Iceberg query performance alarm
- [ ] Create custom dashboards
- [ ] Configure log retention policies
- [ ] Set up data freshness checks

---

## 🔍 Quality Assurance

### Data Quality
- [ ] Implement data contract validation (Pandera/Great Expectations)
- [ ] Create data anomaly detection rules
- [ ] Set up automatic schema drift alerts
- [ ] Create rollback procedures for bad data

### Code Quality
- [ ] Unit tests for all PySpark jobs (>80% coverage)
- [ ] Integration tests for Iceberg MERGE operations
- [ ] Performance benchmarks for ETL job
- [ ] Security scanning (bandit, trivy)

### Operational Excellence
- [ ] Create runbooks for common issues
- [ ] Document troubleshooting procedures
- [ ] Set up on-call rotation
- [ ] Schedule regular DR drills

---

## 📝 Status Summary

| Phase | Task | Status | Owner |
|-------|------|--------|-------|
| 2b | Data Upload | ✓ COMPLETE | Copilot |
| 2b | Data Validation | ✓ COMPLETE | Copilot |
| 2b | Glue Job Creation | ✓ COMPLETE | Copilot |
| 2b | Terraform Config | ✓ COMPLETE | Copilot |
| 2c | Glue Deployment | ⏳ READY | Pending |
| 2d | ETL Execution | ⏳ READY | Pending |
| 2e | ML Training | 📋 TODO | Pending |
| 3 | CI/CD Testing | 📋 TODO | Pending |
| 4 | Production | 📋 TODO | Pending |

---

**Last Updated**: 2025-12-16 19:12 UTC  
**Created by**: GitHub Copilot  
**Environment**: AWS dev (Account: 119287772129, Region: us-east-1)
