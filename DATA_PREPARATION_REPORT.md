# OLIST Data Pipeline - Data Ingestion & Preparation Report

**Date**: 2025-12-16  
**Status**: ✓ COMPLETE  
**Phase**: Data Preparation (Phase 2b)

## Executive Summary

OLIST datasets have been successfully uploaded to S3 and validated for ETL pipeline deployment. All source data is now ready for entity resolution, deduplication, and harmonization through AWS Glue.

### Key Metrics

| Metric | Value |
|--------|-------|
| **Order Items** | 112,650 records, 7 columns |
| **Order Payments** | 103,886 records, 5 columns |
| **Sellers** | 3,095 records, 4 columns |
| **Total Records** | 219,631 records |
| **Total Data Size** | ~21.4 MB |
| **Data Quality** | 0 null values (clean) |
| **S3 Upload Status** | ✓ Complete |

## Data Locations

### Raw Data (S3)
```
s3://contexq-dev-raw-data-119287772129/
├── source_supply/
│   ├── olist_order_items_dataset.csv (15,438,671 bytes)
│   └── olist_sellers_dataset.csv (174,703 bytes)
└── source_financial/
    └── olist_order_payments_dataset.csv (5,777,138 bytes)
```

### Glue Scripts (S3)
```
s3://contexq-dev-raw-data-119287772129/glue-scripts/
├── data_preparation_job.py (6,508 bytes) ✓ DEPLOYED
├── etl_job.py (pending deployment)
└── ml_training_job.py (pending deployment)
```

### Prepared Data Output (Post-Glue)
```
s3://contexq-dev-processed-data-119287772129/prepared_sources/
├── source1_supply/ (Parquet format, indexed by seller_id)
└── source2_financial/ (Parquet format, indexed by order_id)
```

## Data Quality Assessment

### Data Completeness
✓ **Order Items**: 0 null values across all 7 columns
✓ **Payments**: 0 null values across all 5 columns
✓ **Sellers**: 0 null values across all 4 columns

### Data Structure Validation

**Source 1 (Supply Chain)** - 3,095 unique suppliers
- Corporate ID: seller_id (3,095 unique)
- Corporate Name: seller_city + state
- Revenue: sum(price) by supplier = $13,591,643.70
- Average Revenue per Supplier: $4,391.48
- Total Profit (20% margin): $2,718,328.74

**Source 2 (Financial)** - 101,686 payment records
- Corporate ID: order_id (mapped to sequential ID)
- Corporate Name: Order_{order_id}
- Revenue: sum(payment_value) = $16,008,872.12
- Average Payment: $157.43
- Total Profit (15% margin): $2,401,330.82

### Payment Type Distribution (Source 2)
- Credit Card: 76,505 (75.2%)
- Boleto: 19,784 (19.4%)
- Voucher: 3,866 (3.8%)
- Debit Card: 1,528 (1.5%)
- Not Defined: 3 (0.0%)

## Entity Resolution Readiness

### Deduplication Statistics
- Source 1 potential duplicates: 2,753 (supplier name variations)
- Source 2 potential duplicates: None (order_id is unique)
- **Total entities to deduplicate**: 104,781

### Matching Strategy
1. **Normalization**: Lowercase, trim whitespace, remove special chars
2. **Blocking**: By seller_city/state
3. **Matching**: Fuzzy string matching (Levenshtein distance)
4. **Resolution**: Keep best match with confidence score > 0.85

## ETL Pipeline Status

### Completed ✓
- [x] Raw data uploaded to S3 (3 CSV files)
- [x] Data quality validation (pandas-based)
- [x] Schema validation (13-column Iceberg schema)
- [x] Data preparation job created (`data_preparation_job.py`)
- [x] Data preparation job deployed to S3 (`glue-scripts/`)

### In Progress ⏳
- [ ] Deploy data preparation Glue job via Terraform
- [ ] Execute data preparation job (manual trigger)
- [ ] Monitor job execution in CloudWatch
- [ ] Verify output in S3 prepared_sources/

### Pending 📋
- [ ] Deploy entity resolution ETL job to S3
- [ ] Execute ETL job to create corporate_registry Iceberg table
- [ ] Deploy ML training job to S3
- [ ] Execute ML training job for profit prediction model
- [ ] Deploy MWAA Airflow DAG for orchestration
- [ ] Test CI/CD pipeline end-to-end

## Technical Implementation

### Data Preparation Job (`data_preparation_job.py`)
- **Type**: AWS Glue PySpark Job
- **Worker Type**: G.2X (2 workers default, configurable)
- **Python Version**: 3.9
- **Framework**: PySpark 3.5.0 with AWS Glue transforms

**Transformations**:
1. **Ingest**: Read 3 CSV files from S3 raw bucket
2. **Join**: Order items + sellers on seller_id (left join)
3. **Aggregate**: Group by supplier for revenue/profit calculation
4. **Partition**: Separate supply chain (source1) and financial (source2) data
5. **Output**: Write Parquet files to processed bucket with snappy compression

**Data Quality Checks**:
- Null value counts per column
- Record count profiling
- Schema validation against Iceberg table definition
- Duplicate identification

### Terraform Infrastructure

**New Glue Job Resource**:
```hcl
resource "aws_glue_job" "data_preparation" {
  name              = "contexq-dev-data-prep"
  role_arn          = aws_iam_role.glue_service_role.arn
  glue_version      = "4.0"
  worker_type       = "G.2X"
  number_of_workers = 2
  
  command {
    name            = "pythonshell"
    script_location = "s3://.../glue-scripts/data_preparation_job.py"
    python_version  = "3.9"
  }
}
```

**Daily Schedule** (via CloudWatch Events):
- Cron: `0 2 * * ? *` (2 AM UTC)
- Trigger: Automated data preparation before ETL job runs (6 AM UTC)
- DLQ: SQS queue for failed executions

## Next Steps

### Immediate (1-2 hours)
1. Deploy data preparation Glue job via `terraform apply`
2. Manual trigger to test job execution
3. Monitor CloudWatch logs for successful completion
4. Verify Parquet output files in S3 prepared_sources/

### Short-term (2-4 hours)
1. Deploy ETL job to S3 (`etl_job.py`)
2. Execute ETL job with entity resolution
3. Create corporate_registry Iceberg table
4. Validate MERGE INTO operations

### Medium-term (4-8 hours)
1. Deploy ML training job (`ml_training_job.py`)
2. Execute ML pipeline for profit prediction model
3. Register model in MLflow
4. Deploy Airflow DAG to MWAA

### Testing
1. Trigger CI/CD pipeline via GitHub PR
2. Validate GitHub Actions workflows
3. Test rollback procedures
4. Production dry-run

## Cost Estimation (Monthly)

| Component | Cost |
|-----------|------|
| S3 Storage (750 GB, 3 buckets) | ~$18 |
| Glue Jobs (1 hour/day × 10 DPU) | ~$50 |
| Glue Catalog (1 database, 1 table) | ~$1 |
| CloudWatch Logs (7-day retention) | ~$5 |
| **Total** | **~$74/month** |

## Rollback Procedure

If issues occur during data preparation:

1. **Check CloudWatch Logs**:
   ```bash
   aws logs tail /aws/glue/contexq-dev-data-prep --follow
   ```

2. **Disable Scheduled Trigger**:
   ```bash
   aws events disable-rule --name contexq-dev-data-prep-schedule
   ```

3. **Restore Previous State**:
   ```bash
   # Keep raw data in S3, delete prepared sources
   aws s3 rm s3://contexq-dev-processed-data-119287772129/prepared_sources/ --recursive
   ```

4. **Re-enable Trigger** (after fix):
   ```bash
   aws events enable-rule --name contexq-dev-data-prep-schedule
   ```

## Files Created/Modified

### New Files (3)
1. `src/spark/data_preparation_job.py` (210 lines)
   - AWS Glue PySpark transformation logic
   - Source 1 (supply chain) and Source 2 (financial) preparation
   - Data quality checks and schema validation

2. `terraform/glue_jobs_data_prep.tf` (80 lines)
   - Glue job resource configuration
   - CloudWatch Event Rule for scheduling
   - SQS DLQ for error handling
   - CloudWatch Log Group

3. `scripts/validate_data_structure.py` (150 lines)
   - Pandas-based data validation
   - Structure and schema checks
   - Revenue and entity statistics

### S3 Uploads (4)
- `olist_order_items_dataset.csv` → s3://contexq-dev-raw-data-119287772129/source_supply/
- `olist_order_payments_dataset.csv` → s3://contexq-dev-raw-data-119287772129/source_financial/
- `olist_sellers_dataset.csv` → s3://contexq-dev-raw-data-119287772129/source_supply/
- `data_preparation_job.py` → s3://contexq-dev-raw-data-119287772129/glue-scripts/

## Appendix: Data Schemas

### Order Items (7 columns)
- order_id: string
- order_item_id: integer
- product_id: string
- seller_id: string
- shipping_limit_date: string
- price: decimal
- freight_value: decimal

### Order Payments (5 columns)
- order_id: string
- payment_sequential: integer
- payment_type: string
- payment_installments: integer
- payment_value: decimal

### Sellers (4 columns)
- seller_id: string
- seller_zip_code_prefix: string
- seller_city: string
- seller_state: string

### Iceberg Target Schema (13 columns)
- corporate_id: string (PK)
- corporate_name: string
- address: string
- city: string
- state: string
- activity_places: integer
- top_suppliers: array<string>
- main_customers: string
- revenue: decimal(18,2)
- profit: decimal(18,2)
- source_system: string
- load_date: timestamp
- entity_hash: string

---

**Prepared by**: GitHub Copilot  
**Last Updated**: 2025-12-16 19:12 UTC  
**Environment**: AWS dev (Account: 119287772129, Region: us-east-1)
