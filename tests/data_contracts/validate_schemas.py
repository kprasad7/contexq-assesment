#!/usr/bin/env python3
"""Validate schemas for all pipeline data stages."""

import sys
sys.path.insert(0, '/workspaces/contexq-assesment/tests/data_contracts')

from data_contracts import (
    SourceDataContract,
    PreparedDataContract,
    MLFeatureContract,
    DataQualityTier,
    validate_data_quality_metrics
)
import pandas as pd

def validate_schemas():
    """Validate all data schemas."""
    
    print("=" * 60)
    print("DATA SCHEMA VALIDATION")
    print("=" * 60)
    
    # Test Source Data Schema
    print("\n[1] Source Data Schema")
    try:
        source = SourceDataContract(
            order_id="12345",
            customer_id="cust_001",
            seller_id="sell_001",
            payment_value=50000.00,
            order_status="delivered"
        )
        print(" Source data schema valid")
        print(f"   - Fields: {source.schema()['properties'].keys()}")
    except Exception as e:
        print(f" Source data schema invalid: {e}")
        return False
    
    # Test Prepared Data Schema
    print("\n[2] Prepared Data Schema (Silver Tier)")
    try:
        prepared = PreparedDataContract(
            corporate_id="CORP_001",
            revenue=1000000.00,
            profit=250000.00,
            profit_margin=0.25,
            market="retail",
            state="SP",
            activity_places=50,
            transaction_count=1000
        )
        print(" Prepared data schema valid")
        print(f"   - Required fields: {[f for f in prepared.schema()['required']]}")
    except Exception as e:
        print(f" Prepared data schema invalid: {e}")
        return False
    
    # Test ML Feature Schema
    print("\n[3] ML Feature Schema")
    try:
        features = MLFeatureContract(
            corporate_id="CORP_001",
            revenue=1000000.00,
            profit=250000.00,
            profit_margin=0.25,
            activity_places=50,
            label=1
        )
        print(" ML feature schema valid")
        print(f"   - Label values: [0, 1]")
    except Exception as e:
        print(f" ML feature schema invalid: {e}")
        return False
    
    # Test schema constraints
    print("\n[4] Schema Constraints")
    
    # Test negative payment (should fail)
    try:
        invalid = SourceDataContract(
            order_id="12345",
            customer_id="cust_001",
            seller_id="sell_001",
            payment_value=-1000.00,  # Invalid
            order_status="delivered"
        )
        print(" Negative payment should be rejected")
        return False
    except Exception:
        print(" Negative payment correctly rejected")
    
    # Test invalid state code
    try:
        invalid = PreparedDataContract(
            corporate_id="CORP_001",
            revenue=1000000.00,
            profit=250000.00,
            profit_margin=0.25,
            market="retail",
            state="INVALID",  # Too long
            activity_places=50,
            transaction_count=1000
        )
        print(" Invalid state should be rejected")
        return False
    except Exception:
        print(" Invalid state correctly rejected")
    
    # Test invalid label
    try:
        invalid = MLFeatureContract(
            corporate_id="CORP_001",
            revenue=1000000.00,
            profit=250000.00,
            profit_margin=0.25,
            activity_places=50,
            label=5  # Invalid
        )
        print(" Invalid label should be rejected")
        return False
    except Exception:
        print(" Invalid label correctly rejected")
    
    print("\n" + "=" * 60)
    print(" ALL SCHEMA VALIDATIONS PASSED")
    print("=" * 60)
    
    return True


def validate_quality_metrics():
    """Validate data quality metrics."""
    
    print("\n" + "=" * 60)
    print("DATA QUALITY METRICS")
    print("=" * 60)
    
    # Create sample datasets
    bronze_data = pd.DataFrame({
        'col1': [1, 2, None, 4, 5],
        'col2': ['a', 'b', 'c', 'd', 'e']
    })
    
    silver_data = pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': ['a', 'b', 'c', 'd', 'e']
    })
    
    gold_data = pd.DataFrame({
        'col1': [1, 2, 3, 4, 5],
        'col2': ['a', 'b', 'c', 'd', 'e']
    })
    
    # Test Bronze tier
    print("\n[1] Bronze Tier (Raw Data)")
    bronze_metrics = validate_data_quality_metrics(bronze_data, DataQualityTier.BRONZE)
    print(f"   - Records: {bronze_metrics['total_records']}")
    print(f"   - Quality Score: {bronze_metrics['quality_score']:.1%}")
    print(f"   - Passed: {bronze_metrics['passed']}")
    
    # Test Silver tier
    print("\n[2] Silver Tier (Cleaned Data)")
    silver_metrics = validate_data_quality_metrics(silver_data, DataQualityTier.SILVER)
    print(f"   - Records: {silver_metrics['total_records']}")
    print(f"   - Nulls: {silver_metrics['missing_values']}")
    print(f"   - Quality Score: {silver_metrics['quality_score']:.1%}")
    print(f"   - Passed: {silver_metrics['passed']}")
    
    # Test Gold tier
    print("\n[3] Gold Tier (Production-Ready)")
    gold_metrics = validate_data_quality_metrics(gold_data, DataQualityTier.GOLD)
    print(f"   - Records: {gold_metrics['total_records']}")
    print(f"   - Duplicates: {gold_metrics['duplicate_count']}")
    print(f"   - Quality Score: {gold_metrics['quality_score']:.1%}")
    print(f"   - Passed: {gold_metrics['passed']}")
    
    print("\n" + "=" * 60)
    print(" DATA QUALITY VALIDATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    success = validate_schemas()
    validate_quality_metrics()
    
    sys.exit(0 if success else 1)
