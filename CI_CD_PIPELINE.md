# CI/CD Pipeline Documentation

## Overview

This project implements a **complete end-to-end CI/CD pipeline** for the OLIST data processing system, following GitOps principles and production best practices.

```
┌────────────────────────┐
│      GitHub Repo       │
│  (ETL + ML + DAG Code) │
└────────────┬───────────┘
             │ push / PR
             ▼
┌────────────────────────────┐
│   GitHub Actions (CI)      │
│                            │
│  - pytest (unit tests)     │
│  - data contract checks    │
│  - linting                 │
└────────────┬───────────────┘
             │ merge to main
             ▼
┌────────────────────────────┐
│   GitHub Actions (CD)      │
│                            │
│  - Deploy Spark job        │
│  - Deploy Airflow DAG      │
│  - Update Terraform state  │
└────────────┬───────────────┘
             │
             ▼
┌────────────────────────────┐
│   AWS (Glue / EMR / MWAA)  │
└────────────────────────────┘
```

## Architecture

### 1. CI Pipeline (`.github/workflows/ci.yml`)

**Triggers**: Pull requests to `main`/`develop` and pushes to `develop`

#### Jobs:

##### 1.1 Unit Tests & Coverage
- **Purpose**: Validate code functionality
- **Matrix**: Python 3.9, 3.11
- **Tests**:
  - `tests/unit/test_etl_job.py` - ETL components (entity resolution, fuzzy matching, etc.)
  - `tests/unit/test_ml_job.py` - ML training components (feature engineering, model training)
  - Spark DataFrame transformations
  - Data type conversions
  - Null value handling
  - Performance benchmarks
- **Coverage**: Minimum 80% required
- **Reports**: JUnit XML + Codecov

**Key Test Cases**:
```python
# Entity resolution and deduplication
test_fuzzy_matching_detects_duplicates()
test_data_deduplication_logic()
test_md5_hash_deterministic()

# ML model training
test_label_distribution()
test_train_test_split_ratio()
test_feature_engineering()
test_model_metrics_calculation()

# Data quality
test_null_handling()
test_missing_values_handling()
```

##### 1.2 Linting & Code Quality
- **Tools**:
  - `black` - Code formatting (line length: 100)
  - `isort` - Import sorting
  - `flake8` - PEP8 compliance
  - `pylint` - Code analysis
- **Checks**:
  - Consistent formatting
  - Proper imports
  - No E9/F63/F7/F82 errors
  - Naming conventions
- **Config**: `pyproject.toml`, `.flake8`

##### 1.3 Data Contract Validation
- **Purpose**: Validate data schemas at pipeline boundaries
- **Contracts**:
  - `SourceDataContract` - Raw OLIST CSV schema
  - `PreparedDataContract` - Silver tier (cleaned data)
  - `MLFeatureContract` - ML training features
  - `MLPredictionContract` - Model predictions
- **Features**:
  - Pydantic validation models
  - Type hints and constraints
  - Business rule enforcement
- **Scripts**:
  - `tests/data_contracts/validate_schemas.py` - Schema validation
  - `tests/data_contracts/validate_contracts.py` - Transformation rules

**Data Quality Tiers**:
- **Bronze**: Raw data (quality score 50%)
- **Silver**: Cleaned data (null % < 5%, dupes < 1%)
- **Gold**: Production-ready (null % = 0%, dupes = 0%)

##### 1.4 Security Scanning
- **Tool**: Trivy vulnerability scanner
- **Scope**: Full filesystem scan
- **Output**: SARIF format → GitHub Security tab

##### 1.5 CI Summary
- **Purpose**: Aggregate all job results
- **Fails if**: Any job fails
- **Success**: All CI gates passed

### 2. CD Pipeline (`.github/workflows/cd.yml`)

**Triggers**: Push to `main` branch (auto-deploy after CI passes)

**Permissions**: Uses GitHub OIDC + AWS STS for secure credentials

#### Jobs:

##### 2.1 Deploy Job

**Steps**:

1. **Checkout & Setup**
   - Clone code with full history
   - Configure AWS credentials via OIDC
   - Setup Python 3.11

2. **Validation**
   - Python syntax check (py_compile)
   - Terraform format validation
   - Terraform plan review

3. **Upload Spark Jobs to S3**
   ```bash
   # Target bucket: contexq-dev-raw-data-119287772129
   aws s3 cp src/spark/etl_job.py s3://.../glue-scripts/
   aws s3 cp src/spark/ml_training_job.py s3://.../glue-scripts/
   aws s3 cp src/spark/data_preparation_job.py s3://.../glue-scripts/
   ```
   - **Result**: Scripts available for Glue jobs

4. **Upload Airflow DAG to MWAA**
   ```bash
   # Target bucket: contexq-dev-mwaa-dags-119287772129
   aws s3 cp src/airflow/dags/olist_data_pipeline.py s3://.../dags/
   ```
   - **Result**: DAG auto-synced by MWAA within 1 minute

5. **Terraform Infrastructure**
   - `terraform plan` - Show proposed changes
   - `terraform apply` - Deploy infrastructure
   - Export outputs for downstream use

6. **Update Glue Jobs**
   - Update job configurations with new script locations
   - Set resource requirements (G.2X workers, 2 instances)
   - Configure logging and error handling

7. **Trigger MWAA Refresh**
   - Verify DAG file exists in S3
   - Confirm DAG is accessible

8. **Smoke Tests**
   - Verify all Glue jobs exist and are accessible
   - Verify all S3 buckets exist
   - Verify Iceberg database and tables exist

9. **Deployment Summary**
   - Generate GitHub summary
   - List all deployed components
   - Provide next steps

##### 2.2 Automatic Rollback Job

**Triggers**: If deploy job fails

**Steps**:
1. Identify last working commit
2. Re-deploy previous Spark job versions
3. Alert on failure with logs

### 3. Test Files Structure

```
tests/
├── __init__.py
├── unit/
│   ├── __init__.py
│   ├── test_etl_job.py      # ETL job unit tests
│   └── test_ml_job.py       # ML job unit tests
├── integration/
│   └── __init__.py
└── data_contracts/
    ├── __init__.py
    ├── data_contracts.py    # Pydantic contract models
    ├── validate_schemas.py  # Schema validation script
    └── validate_contracts.py # Transformation rules
```

## Running Locally

### Prerequisites
```bash
pip install -r requirements-dev.txt
```

### Run Unit Tests
```bash
pytest tests/unit/ -v --cov=src --cov-report=html
```

### Run Linting
```bash
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/
pylint src/
```

### Run Data Contract Validation
```bash
python tests/data_contracts/validate_schemas.py
python tests/data_contracts/validate_contracts.py
```

### Format Code
```bash
black src/ tests/
isort src/ tests/
```

## Deployment Workflow

### 1. Feature Development
```bash
git checkout -b feature/new-feature
# Make changes
git push origin feature/new-feature
```

### 2. Create Pull Request
- GitHub Actions CI automatically runs
- Review test results and code quality
- Request review from team members
- CI must pass before merge

### 3. Merge to Main
```bash
# After PR approval, merge to main
git merge --squash feature/new-feature
git push origin main
```

### 4. CD Pipeline Runs
- GitHub Actions CD automatically deploys to AWS
- Spark jobs uploaded to S3
- Airflow DAG updated in MWAA
- Terraform resources updated
- Smoke tests verify deployment
- Automatic rollback if any step fails

### 5. Monitor Production
```bash
# View Glue job status
aws glue list-job-runs --job-name contexq-dev-etl

# Monitor Airflow DAG
# Access MWAA UI: https://airflow-mwaa.us-east-1.console.aws.amazon.com

# View CloudWatch logs
aws logs tail /aws/glue/contexq-dev-etl --follow
```

## Environment Configuration

### AWS Secrets Required

Add these to GitHub repository secrets:

```
AWS_ACCOUNT_ID=119287772129
AWS_ROLE_ARN=arn:aws:iam::119287772129:role/github-actions-role
```

### OIDC Provider Setup

AWS IAM role for GitHub Actions:
```hcl
trust_relationship = {
  federated_principal = "arn:aws:iam::119287772129:oidc-provider/token.actions.githubusercontent.com"
  condition = {
    "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
    "token.actions.githubusercontent.com:sub" = "repo:kprasad7/contexq-assesment:*"
  }
}
```

## Quality Gates

### CI Pipeline Gates

| Gate | Tool | Threshold | Fail If |
|------|------|-----------|---------|
| Unit Tests | pytest | - | Any test fails |
| Code Coverage | coverage | 80% | < 80% coverage |
| Code Format | black | - | Formatting violations |
| Import Sorting | isort | - | Imports unsorted |
| Linting | flake8 | - | E9, F63, F7, F82 errors |
| Data Contracts | pydantic | - | Schema validation fails |
| Security | trivy | - | High/critical vulnerabilities |

### CD Deployment Gates

| Gate | Check | Fail If |
|------|-------|---------|
| Syntax | py_compile | Invalid Python syntax |
| Terraform | terraform validate | HCL errors |
| Smoke Tests | AWS API | Any resource check fails |

## Performance Characteristics

### Test Execution Time
- **Unit Tests (Python 3.9)**: ~30-45 seconds
- **Unit Tests (Python 3.11)**: ~30-45 seconds
- **Linting**: ~10-15 seconds
- **Data Contracts**: ~5-10 seconds
- **Security Scan**: ~20-30 seconds
- **Total CI Time**: ~2-3 minutes

### Deployment Time
- **Spark Jobs Upload**: ~10 seconds
- **Airflow DAG Upload**: ~5 seconds
- **Terraform Plan**: ~20-30 seconds
- **Terraform Apply**: ~30-60 seconds
- **Smoke Tests**: ~10-15 seconds
- **Total CD Time**: ~2-3 minutes

## Troubleshooting

### CI Failures

**Test Failures**:
```bash
# Run failing test locally
pytest tests/unit/test_etl_job.py::test_name -v

# Check test output
# Review error message and stack trace
```

**Linting Failures**:
```bash
# Auto-format code
black src/ tests/
isort src/ tests/

# Review changes and commit
git add .
git commit -m "style: auto-format code"
```

**Data Contract Failures**:
```bash
# Run validation locally
python tests/data_contracts/validate_schemas.py

# Check error messages
# Update Pydantic models if schema changed
```

### CD Failures

**S3 Upload Fails**:
- Check AWS credentials
- Verify S3 bucket exists
- Check IAM permissions

**Terraform Fails**:
- Run `terraform validate` locally
- Check for syntax errors
- Verify AWS resources exist

**Glue Job Update Fails**:
- Verify job name matches
- Check IAM role permissions
- Review CloudWatch logs

### Rollback Procedure

If deployment fails and automatic rollback doesn't work:

```bash
# Identify last good commit
git log --oneline | head -5

# Get Spark jobs from previous version
git checkout <good-commit> -- src/spark/

# Re-upload to S3
aws s3 cp src/spark/etl_job.py s3://.../glue-scripts/etl_job.py

# Verify
aws glue get-job --name contexq-dev-etl
```

## Monitoring & Observability

### GitHub Actions Dashboard
- View workflow runs: `Actions` tab
- Check logs for each job
- Review step-by-step execution

### AWS CloudWatch
```bash
# View Glue job logs
aws logs tail /aws/glue/contexq-dev-etl --follow

# View specific run
aws logs tail /aws/glue/contexq-dev-etl \
  --since 1h \
  | grep ERROR
```

### MWAA/Airflow
- Dashboard: View task execution
- Logs: Review DAG run details
- Scheduler: Check trigger status

### MLflow
- Experiments: Track model training
- Models: View registered versions
- Artifacts: Access training artifacts

## Security Best Practices

1. **Credentials Management**
   - Use GitHub OIDC for AWS authentication
   - Never commit secrets
   - Rotate credentials regularly

2. **Code Review**
   - Require approvals before merge
   - Automated linting and testing
   - Manual security review

3. **Deployment Safety**
   - Automatic rollback on failure
   - Smoke tests verify deployment
   - CloudWatch monitoring

4. **Data Protection**
   - Encrypt data in transit (S3 SSL)
   - Encrypt data at rest (KMS)
   - Access control via IAM roles

## Future Enhancements

1. **Advanced Testing**
   - Integration tests against real AWS resources
   - Performance benchmarks
   - Chaos engineering tests

2. **Enhanced Monitoring**
   - Custom CloudWatch dashboards
   - PagerDuty alerts
   - Slack notifications

3. **Advanced CD**
   - Blue-green deployments
   - Canary releases
   - Feature flags

4. **Cost Optimization**
   - Auto-scaling based on data volume
   - Resource tagging and cost allocation
   - Usage reports and alerts

## References

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [AWS Glue Developer Guide](https://docs.aws.amazon.com/glue/)
- [Airflow Documentation](https://airflow.apache.org/)
- [Terraform Best Practices](https://www.terraform.io/docs/best-practices)
