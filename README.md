# 🎯 OLIST Data Engineering Platform

[![CI](https://github.com/kprasad7/contexq-assesment/workflows/CI/badge.svg)](https://github.com/kprasad7/contexq-assesment/actions)
[![CD](https://github.com/kprasad7/contexq-assesment/workflows/CD/badge.svg)](https://github.com/kprasad7/contexq-assesment/actions)
[![Code Coverage](https://img.shields.io/badge/coverage-80%25-brightgreen)](https://github.com/kprasad7/contexq-assesment)

Production-grade data engineering platform for entity resolution, harmonization, and ML training using Apache Iceberg, PySpark, MLflow, and AWS Glue.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    OLIST Data Pipeline                          │
└─────────────────────────────────────────────────────────────────┘

   CSV Files (S3)                  PySpark ETL                 Iceberg Table
┌───────────────┐              ┌──────────────┐           ┌─────────────────┐
│ Sellers       │──┐           │ Entity       │           │ corporate_      │
│ Payments      │──┼──────────▶│ Resolution   │──────────▶│ registry        │
│ Order Items   │──┘           │ + Fuzzy      │           │                 │
└───────────────┘              │ Matching     │           │ (Glue Catalog)  │
                               └──────────────┘           └────────┬────────┘
                                                                   │
                                                                   │ Read
                                                                   ▼
┌─────────────────┐            ┌──────────────┐           ┌─────────────────┐
│ MLflow          │◀───────────│ PySpark ML   │◀──────────│ Iceberg Table   │
│ Artifacts (S3)  │   Log      │ Training     │           │                 │
│                 │            │              │           └─────────────────┘
│ - Metrics       │            │ - Features   │
│ - Parameters    │            │ - LogisticReg│
│ - Run IDs       │            │ - Evaluation │
└─────────────────┘            └──────────────┘

                    Orchestration: Airflow (AWS MWAA)
                    ┌─────────────────────────────────┐
                    │ data_prep → etl → validate →    │
                    │ quality_check → ml_training     │
                    └─────────────────────────────────┘

                    CI/CD: GitHub Actions
                    ┌─────────────────────────────────┐
                    │ Tests → Lint → Deploy →         │
                    │ Terraform Apply                 │
                    └─────────────────────────────────┘
```

## 🧠 Entity Resolution Algorithm

### Heuristic Approach

The pipeline uses a **fuzzy matching algorithm** to identify duplicate corporations across multiple data sources:

1. **Name Normalization**:
   ```python
   - Remove special characters: !@#$%^&*()
   - Lowercase conversion
   - Whitespace normalization
   - Remove common suffixes: LLC, Inc, Corp, Ltd
   ```

2. **Address Standardization**:
   ```python
   - Extract city, state, zip code
   - Normalize abbreviations (St → Street, Ave → Avenue)
   - Remove apartment/suite numbers
   ```

3. **Similarity Calculation**:
   ```python
   # Levenshtein distance for fuzzy matching
   similarity = 1 - (levenshtein(str1, str2) / max(len(str1), len(str2)))
   
   # Threshold: 0.85 (85% similarity)
   if similarity >= 0.85:
       same_corporation = True
   ```

4. **Unique ID Generation**:
   ```python
   # MD5 hash of normalized fields
   corporate_id = md5(cleansed_name + normalized_address + city)
   ```

### Performance Optimizations
- **Broadcast joins** for small lookup tables
- **DataFrame caching** to materialize UDF results
- **Partition pruning** on year/month columns

## 🚀 Quick Start

### Prerequisites
- AWS Account with credentials configured
- Terraform >= 1.5.0
- Python 3.9 or 3.11
- Git

### 1. Clone Repository
```bash
git clone https://github.com/kprasad7/contexq-assesment.git
cd contexq-assesment
```

### 2. Set Up Python Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### 3. Configure AWS Credentials
```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

### 4. Deploy Infrastructure
```bash
cd terraform
terraform init
terraform plan -var-file=environments/dev.tfvars
terraform apply -var-file=environments/dev.tfvars
```

### 5. Upload Data to S3
```bash
aws s3 cp olist_sellers_dataset.csv s3://contexq-dev-raw-data-<ACCOUNT_ID>/data/
aws s3 cp olist_order_payments_dataset.csv s3://contexq-dev-raw-data-<ACCOUNT_ID>/data/
aws s3 cp olist_order_items_dataset.csv s3://contexq-dev-raw-data-<ACCOUNT_ID>/data/
```

### 6. Deploy Spark Jobs
```bash
aws s3 cp src/spark/comprehensive_etl_job.py s3://contexq-dev-raw-data-<ACCOUNT_ID>/glue-scripts/
aws s3 cp src/spark/ml_training_job.py s3://contexq-dev-raw-data-<ACCOUNT_ID>/glue-scripts/
```

### 7. Trigger Pipeline
```bash
# Manual trigger via AWS Console or CLI
aws glue start-job-run --job-name contexq-dev-etl

# Or wait for scheduled Airflow DAG (every 6 hours)
```

## 📊 Querying the Iceberg Table

### Using AWS Athena
```sql
-- Query corporate registry
SELECT 
    corporate_id,
    corporate_name,
    revenue,
    profit,
    activity_places,
    top_suppliers
FROM contexq_dev.corporate_registry
WHERE year = 2025 AND month = 12
LIMIT 10;

-- Aggregate statistics
SELECT 
    source_system,
    COUNT(*) as total_corporations,
    AVG(revenue) as avg_revenue,
    AVG(profit) as avg_profit
FROM contexq_dev.corporate_registry
GROUP BY source_system;
```

### Using PySpark
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .getOrCreate()

# Read Iceberg table
df = spark.table("glue_catalog.contexq_dev.corporate_registry")
df.show(10)

# Time travel - query as of timestamp
df_snapshot = spark.read \
    .option("as-of-timestamp", "2025-12-17 10:00:00") \
    .table("glue_catalog.contexq_dev.corporate_registry")
```

## 🤖 Viewing ML Model Artifacts

### Option 1: CloudWatch Logs (Current)
```bash
# View metrics from latest ML training run
python scripts/view_ml_metrics.py
```

**Output**:
```
╔══════════════════╦═══════╦════════════╦═══════════╦════════════╗
║ Timestamp        ║  AUC  ║  F1-Score  ║  Records  ║  Time      ║
╠══════════════════╬═══════╬════════════╬═══════════╬════════════╣
║ 2025-12-17 11:17 ║ 0.944 ║ 0.8678     ║ 3,095     ║ 165s       ║
╚══════════════════╩═══════╩════════════╩═══════════╩════════════╝
```

### Option 2: MLflow UI (Persistent Server)
```bash
# Start local MLflow server
./scripts/setup_mlflow_server.sh
# Choose option 1 (local) or 3 (Docker)

# Access dashboard
open http://localhost:5000
```

### Option 3: Query S3 Artifacts
```bash
# List MLflow experiments
aws s3 ls s3://contexq-dev-mlflow-artifacts-<ACCOUNT_ID>/mlruns/

# Download specific run artifacts
aws s3 cp s3://contexq-dev-mlflow-artifacts-<ACCOUNT_ID>/mlruns/<experiment_id>/<run_id>/ ./artifacts/ --recursive
```

## 🧪 Running Tests

### Unit Tests
```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/unit/test_etl_job.py -v

# Run with coverage
pytest --cov=src --cov-report=html tests/
```

### Data Contract Validation
```bash
# Validate schemas
python tests/data_contracts/validate_schemas.py

# Validate transformations
python tests/data_contracts/validate_contracts.py
```

### Linting
```bash
# Format code
black src/ tests/

# Check imports
isort src/ tests/

# Run linters
flake8 src/ tests/
pylint src/ tests/
```

## 📁 Project Structure

```
contexq-assesment/
├── src/
│   ├── spark/
│   │   ├── comprehensive_etl_job.py    # Entity resolution + Iceberg merge
│   │   └── ml_training_job.py          # ML model training
│   └── airflow/
│       └── dags/
│           └── olist_data_pipeline.py  # Orchestration DAG
├── tests/
│   ├── unit/
│   │   ├── test_etl_job.py             # ETL unit tests (8 tests)
│   │   └── test_ml_job.py              # ML unit tests (14 tests)
│   └── data_contracts/
│       ├── data_contracts.py           # Pydantic models
│       ├── validate_schemas.py         # Schema validation
│       └── validate_contracts.py       # Transformation validation
├── terraform/
│   ├── main.tf                         # Main infrastructure
│   ├── modules/
│   │   ├── s3_buckets/                 # S3 bucket module
│   │   ├── glue_catalog/               # Glue catalog module
│   │   ├── glue_jobs/                  # Glue jobs module
│   │   └── iam_roles/                  # IAM roles module
│   └── environments/
│       ├── dev.tfvars                  # Dev environment config
│       └── prod.tfvars                 # Prod environment config
├── scripts/
│   ├── setup_mlflow_server.sh          # MLflow server setup
│   └── view_ml_metrics.py              # Metrics viewer
├── .github/
│   └── workflows/
│       ├── ci.yml                      # Continuous Integration
│       └── cd.yml                      # Continuous Deployment
├── *.csv                               # Sample OLIST datasets
├── pyproject.toml                      # Python project config
├── requirements-dev.txt                # Development dependencies
└── README.md                           # This file
```

## 🔄 CI/CD Pipeline

### Continuous Integration (CI)
**Triggers**: Pull requests, pushes to develop

**Jobs**:
1. **Unit Tests** - pytest on Python 3.9 & 3.11
2. **Linting** - black, flake8, pylint, isort
3. **Data Contracts** - Schema validation
4. **Security Scan** - Trivy vulnerability scanning
5. **Coverage** - Codecov reporting (80% minimum)

### Continuous Deployment (CD)
**Triggers**: Merge to main

**Jobs**:
1. **Terraform Apply** - Infrastructure updates
2. **Upload Spark Jobs** - Deploy to S3
3. **Deploy Airflow DAG** - Upload to MWAA
4. **Update Glue Jobs** - Configure job parameters
5. **Smoke Tests** - Verify deployment

**Rollback**: Automatic on deployment failure

## 📊 Performance Metrics

| Metric | Value |
|--------|-------|
| ETL Job Execution | 181 seconds |
| ML Training Execution | 165 seconds |
| Model AUC Score | 0.944 (94.4%) |
| Model F1 Score | 0.868 (86.8%) |
| Data Records Processed | 3,095 |
| Iceberg Files Created | 146+ |
| CI Pipeline Duration | 2-3 minutes |
| CD Pipeline Duration | 2-3 minutes |
| Test Coverage | 80%+ |
| Total Tests | 47+ |

## 📚 Documentation

- **[DELIVERABLES.md](DELIVERABLES.md)** - Complete deliverables checklist
- **[CI_CD_PIPELINE.md](CI_CD_PIPELINE.md)** - Detailed CI/CD documentation (600+ lines)
- **[DATA_PREPARATION_REPORT.md](DATA_PREPARATION_REPORT.md)** - Data preparation details
- **[INFRASTRUCTURE_VALIDATION_REPORT.md](INFRASTRUCTURE_VALIDATION_REPORT.md)** - Infrastructure validation
- **[MANUAL_TRIGGER_GUIDE.md](MANUAL_TRIGGER_GUIDE.md)** - Manual job trigger guide

## 🛠️ Troubleshooting

### Common Issues

**Issue**: Glue job fails with "Table doesn't exist"
```bash
# Solution: Check table creation in Glue Catalog
aws glue get-table --database-name contexq_dev --name corporate_registry
```

**Issue**: ML training fails with class imbalance
```bash
# Solution: Adjust profit threshold in ml_training_job.py (Line 143)
# Current: 180.0 (median)
when(col("profit") > lit(180.0), lit(1)).otherwise(lit(0))
```

**Issue**: MLflow metrics not visible
```bash
# Solution: Metrics are in CloudWatch logs, use viewer script
python scripts/view_ml_metrics.py
```

**Issue**: CI/CD pipeline fails
```bash
# Solution: Check GitHub Actions logs
# Ensure AWS credentials are configured in GitHub Secrets
```

## 🔐 Security

- **OIDC Authentication** - No long-lived AWS credentials
- **IAM Least Privilege** - Minimal permissions per service
- **S3 Encryption** - AES-256 server-side encryption
- **CloudTrail Logging** - Full audit trail
- **Secrets Management** - GitHub Secrets for sensitive data

## 🚧 Future Enhancements

- [ ] Add integration tests for end-to-end pipeline
- [ ] Set up persistent MLflow tracking server (EC2/ECS)
- [ ] Implement data lineage tracking (DataHub/Amundsen)
- [ ] Add real-time streaming pipeline (Kinesis/Kafka)
- [ ] Multi-region deployment support
- [ ] Advanced monitoring dashboards (Grafana)
- [ ] Model serving endpoint (SageMaker)

## 📄 License

This project is part of the ContextQ technical assessment.

## 👤 Author

**Prasad LVV**
- GitHub: [@kprasad7](https://github.com/kprasad7)
- Email: prasadlvv049@gmail.com

## 🙏 Acknowledgments

- OLIST Brazilian E-commerce Dataset (Kaggle)
- Apache Iceberg community
- AWS Glue team
- MLflow project

---

**Built with ❤️ using PySpark, Apache Iceberg, MLflow, Terraform, and AWS Glue**
