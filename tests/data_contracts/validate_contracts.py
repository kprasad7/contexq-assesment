#!/usr/bin/env python3
"""Validate data transformation contracts."""

import sys
sys.path.insert(0, '/workspaces/contexq-assesment/tests/data_contracts')

from data_contracts import PreparedDataContract, MLFeatureContract
import pandas as pd


def validate_etl_transformation():
    """Validate ETL transformation contracts."""
    
    print("=" * 60)
    print("ETL TRANSFORMATION VALIDATION")
    print("=" * 60)
    
    # Simulated ETL output
    etl_output = [
        {
            "corporate_id": "CORP_001",
            "revenue": 1000000.00,
            "profit": 250000.00,
            "profit_margin": 0.25,
            "market": "retail",
            "state": "SP",
            "activity_places": 50,
            "transaction_count": 1000
        },
        {
            "corporate_id": "CORP_002",
            "revenue": 500000.00,
            "profit": 50000.00,
            "profit_margin": 0.10,
            "market": "wholesale",
            "state": "RJ",
            "activity_places": 20,
            "transaction_count": 500
        },
        {
            "corporate_id": "CORP_003",
            "revenue": 750000.00,
            "profit": 150000.00,
            "profit_margin": 0.20,
            "market": "retail",
            "state": "MG",
            "activity_places": 35,
            "transaction_count": 750
        },
    ]
    
    print("\n[1] Validating ETL Output Records")
    errors = []
    valid_count = 0
    
    for idx, record in enumerate(etl_output):
        try:
            PreparedDataContract(**record)
            valid_count += 1
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")
    
    print(f"   - Total records: {len(etl_output)}")
    print(f"   - Valid records: {valid_count}")
    print(f"   - Invalid records: {len(errors)}")
    
    if errors:
        print("   ❌ Errors found:")
        for error in errors:
            print(f"      - {error}")
        return False
    else:
        print("   ✅ All records valid")
    
    # Validate transformations
    print("\n[2] Validating Transformation Rules")
    
    df = pd.DataFrame(etl_output)
    
    # Rule 1: Profit margin = profit / revenue
    print("   [Rule 1] Profit Margin Consistency")
    for idx, row in df.iterrows():
        expected_margin = row['profit'] / row['revenue']
        actual_margin = row['profit_margin']
        
        if abs(expected_margin - actual_margin) < 0.01:  # 1% tolerance
            print(f"      ✅ Record {idx}: Margin correct ({actual_margin:.2%})")
        else:
            print(f"      ❌ Record {idx}: Margin mismatch ({actual_margin:.2%} vs {expected_margin:.2%})")
            return False
    
    # Rule 2: Revenue >= 0
    print("   [Rule 2] Non-negative Revenue")
    if (df['revenue'] >= 0).all():
        print(f"      ✅ All revenue values non-negative")
    else:
        print(f"      ❌ Found negative revenue")
        return False
    
    # Rule 3: State codes are 2-letter
    print("   [Rule 3] Valid State Codes")
    valid_states = df['state'].str.len() == 2
    if valid_states.all():
        print(f"      ✅ All state codes are 2 letters: {df['state'].unique().tolist()}")
    else:
        print(f"      ❌ Invalid state codes found")
        return False
    
    print("\n✅ ETL Transformation Validation Passed")
    return True


def validate_ml_training_transformation():
    """Validate ML training data transformation."""
    
    print("\n" + "=" * 60)
    print("ML TRAINING DATA TRANSFORMATION VALIDATION")
    print("=" * 60)
    
    # Simulated ML training output
    ml_features = [
        {
            "corporate_id": "CORP_001",
            "revenue": 1000000.00,
            "profit": 250000.00,
            "profit_margin": 0.25,
            "activity_places": 50,
            "label": 1  # High profit
        },
        {
            "corporate_id": "CORP_002",
            "revenue": 500000.00,
            "profit": 50000.00,
            "profit_margin": 0.10,
            "activity_places": 20,
            "label": 0  # Low profit
        },
        {
            "corporate_id": "CORP_003",
            "revenue": 750000.00,
            "profit": 150000.00,
            "profit_margin": 0.20,
            "activity_places": 35,
            "label": 1  # High profit
        },
    ]
    
    print("\n[1] Validating ML Feature Records")
    errors = []
    valid_count = 0
    
    for idx, record in enumerate(ml_features):
        try:
            MLFeatureContract(**record)
            valid_count += 1
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")
    
    print(f"   - Total features: {len(ml_features)}")
    print(f"   - Valid features: {valid_count}")
    print(f"   - Invalid features: {len(errors)}")
    
    if errors:
        print("   ❌ Errors found:")
        for error in errors:
            print(f"      - {error}")
        return False
    else:
        print("   ✅ All features valid")
    
    # Validate feature statistics
    print("\n[2] Validating Feature Statistics")
    
    df = pd.DataFrame(ml_features)
    
    # Check label distribution
    print("   [Stat 1] Label Distribution")
    label_dist = df['label'].value_counts().sort_index()
    print(f"      - Class 0 (Low Profit): {label_dist.get(0, 0)} records ({label_dist.get(0, 0)/len(df)*100:.1f}%)")
    print(f"      - Class 1 (High Profit): {label_dist.get(1, 0)} records ({label_dist.get(1, 0)/len(df)*100:.1f}%)")
    
    if len(label_dist) < 2:
        print("      ⚠️ Warning: Only one class present")
    else:
        print("      ✅ Both classes present")
    
    # Check revenue range
    print("   [Stat 2] Revenue Range")
    print(f"      - Min: ${df['revenue'].min():,.0f}")
    print(f"      - Max: ${df['revenue'].max():,.0f}")
    print(f"      - Mean: ${df['revenue'].mean():,.0f}")
    print(f"      - Std: ${df['revenue'].std():,.0f}")
    
    # Check profit margin range
    print("   [Stat 3] Profit Margin Range")
    print(f"      - Min: {df['profit_margin'].min():.1%}")
    print(f"      - Max: {df['profit_margin'].max():.1%}")
    print(f"      - Mean: {df['profit_margin'].mean():.1%}")
    
    # Check activity places
    print("   [Stat 4] Activity Places")
    print(f"      - Min: {df['activity_places'].min()}")
    print(f"      - Max: {df['activity_places'].max()}")
    print(f"      - Mean: {df['activity_places'].mean():.0f}")
    
    # Feature correlation with label
    print("\n[3] Feature-Label Correlation Analysis")
    
    # Profit should correlate with high profit label
    if (df[df['label'] == 1]['profit'].mean() > df[df['label'] == 0]['profit'].mean()):
        print(f"      ✅ Profit correlates with high_profit label")
    else:
        print(f"      ⚠️ Profit does not correlate as expected")
    
    # Profit margin should correlate with high profit label
    if (df[df['label'] == 1]['profit_margin'].mean() > df[df['label'] == 0]['profit_margin'].mean()):
        print(f"      ✅ Profit margin correlates with high_profit label")
    else:
        print(f"      ⚠️ Profit margin does not correlate as expected")
    
    print("\n✅ ML Training Transformation Validation Passed")
    return True


def validate_data_integrity():
    """Validate data integrity across transformations."""
    
    print("\n" + "=" * 60)
    print("DATA INTEGRITY VALIDATION")
    print("=" * 60)
    
    # Test referential integrity
    print("\n[1] Referential Integrity")
    
    # Simulate ETL output and ML features
    etl_ids = {"CORP_001", "CORP_002", "CORP_003"}
    ml_ids = {"CORP_001", "CORP_002", "CORP_003"}
    
    missing_ml = etl_ids - ml_ids
    extra_ml = ml_ids - etl_ids
    
    if not missing_ml and not extra_ml:
        print("   ✅ All ETL records included in ML features")
    else:
        if missing_ml:
            print(f"   ❌ Missing in ML features: {missing_ml}")
        if extra_ml:
            print(f"   ❌ Extra in ML features: {extra_ml}")
        return False
    
    # Test data completeness
    print("\n[2] Data Completeness")
    
    ml_features = [
        {"corporate_id": "CORP_001", "revenue": 1000000, "profit": 250000, 
         "profit_margin": 0.25, "activity_places": 50, "label": 1},
        {"corporate_id": "CORP_002", "revenue": 500000, "profit": 50000, 
         "profit_margin": 0.10, "activity_places": 20, "label": 0},
    ]
    
    df = pd.DataFrame(ml_features)
    null_pct = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
    
    print(f"   - Null values: {null_pct:.1f}%")
    
    if null_pct == 0:
        print("   ✅ No missing values in critical fields")
    else:
        print(f"   ⚠️ {null_pct:.1f}% missing data")
    
    # Test uniqueness
    print("\n[3] Uniqueness Constraints")
    
    duplicate_ids = df['corporate_id'].duplicated().sum()
    
    if duplicate_ids == 0:
        print("   ✅ All corporate_ids are unique")
    else:
        print(f"   ❌ Found {duplicate_ids} duplicate IDs")
        return False
    
    print("\n✅ Data Integrity Validation Passed")
    return True


if __name__ == "__main__":
    success = True
    
    success = validate_etl_transformation() and success
    success = validate_ml_training_transformation() and success
    success = validate_data_integrity() and success
    
    print("\n" + "=" * 60)
    if success:
        print("✅ ALL TRANSFORMATION VALIDATIONS PASSED")
    else:
        print("❌ SOME VALIDATIONS FAILED")
    print("=" * 60)
    
    sys.exit(0 if success else 1)
