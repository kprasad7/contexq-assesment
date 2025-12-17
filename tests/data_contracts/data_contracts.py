"""Data contract validation for OLIST pipeline."""

from enum import Enum
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, Field, validator


class DataQualityTier(str, Enum):
    """Data quality tiers."""

    BRONZE = "bronze"  # Raw data
    SILVER = "silver"  # Cleaned and validated
    GOLD = "gold"  # Production-ready


class SourceDataContract(BaseModel):
    """Contract for raw source data."""

    order_id: str = Field(..., description="Unique order identifier")
    customer_id: str = Field(..., description="Customer identifier")
    seller_id: str = Field(..., description="Seller/Supplier identifier")
    payment_value: float = Field(..., gt=0, description="Payment amount in BRL")
    order_status: str = Field(..., description="Order status")

    @validator("payment_value")
    def validate_payment_range(cls, v):
        """Ensure payment value is reasonable."""
        if v > 1_000_000:  # Max 1M BRL per transaction
            raise ValueError(f"Payment value too high: {v}")
        return v

    class Config:
        schema_extra = {
            "example": {
                "order_id": "123e4567-e89b-12d3-a456-426614174000",
                "customer_id": "cust_001",
                "seller_id": "sell_001",
                "payment_value": 50000.00,
                "order_status": "delivered",
            }
        }


class PreparedDataContract(BaseModel):
    """Contract for prepared (silver tier) data."""

    corporate_id: str = Field(..., description="Standardized corporate identifier")
    revenue: float = Field(..., ge=0, description="Total revenue")
    profit: float = Field(..., description="Total profit (can be negative)")
    profit_margin: float = Field(..., ge=-1, le=1, description="Profit margin ratio")
    market: Optional[str] = Field(None, description="Market segment")
    state: str = Field(..., min_length=2, max_length=2, description="Brazilian state code")
    activity_places: int = Field(..., ge=0, description="Number of activity locations")
    transaction_count: int = Field(..., ge=0, description="Number of transactions")

    @validator("profit_margin")
    def validate_margin(cls, v, values):
        """Ensure profit margin is consistent with revenue/profit."""
        if "revenue" in values and values["revenue"] > 0:
            if "profit" in values:
                expected_margin = values["profit"] / values["revenue"]
                if abs(v - expected_margin) > 0.01:  # Allow 1% tolerance
                    raise ValueError(f"Profit margin inconsistent: {v} vs {expected_margin}")
        return v

    class Config:
        schema_extra = {
            "example": {
                "corporate_id": "CORP_001",
                "revenue": 1000000.00,
                "profit": 250000.00,
                "profit_margin": 0.25,
                "market": "retail",
                "state": "SP",
                "activity_places": 50,
                "transaction_count": 1000,
            }
        }


class MLFeatureContract(BaseModel):
    """Contract for ML training features."""

    corporate_id: str
    revenue: float = Field(..., gt=0)
    profit: float
    profit_margin: float = Field(..., ge=-1, le=1)
    activity_places: int = Field(..., ge=0)
    label: int = Field(..., ge=0, le=1, description="Binary label: 0=low profit, 1=high profit")

    @validator("label")
    def validate_label(cls, v):
        """Ensure label is binary."""
        if v not in [0, 1]:
            raise ValueError(f"Label must be 0 or 1, got {v}")
        return v


class MLPredictionContract(BaseModel):
    """Contract for ML model predictions."""

    corporate_id: str
    predicted_class: int = Field(..., ge=0, le=1, description="Predicted class")
    prediction_probability: float = Field(..., ge=0, le=1, description="Prediction probability")
    confidence: float = Field(..., ge=0, le=1, description="Model confidence score")
    prediction_timestamp: str = Field(..., description="ISO 8601 timestamp")

    @validator("prediction_probability")
    def validate_probability(cls, v):
        """Ensure valid probability."""
        if not (0 <= v <= 1):
            raise ValueError(f"Probability must be between 0 and 1, got {v}")
        return v


def validate_source_data(data: List[dict]) -> tuple[bool, List[str]]:
    """
    Validate source data against contract.

    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []

    for idx, record in enumerate(data):
        try:
            SourceDataContract(**record)
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    return len(errors) == 0, errors


def validate_prepared_data(data: List[dict]) -> tuple[bool, List[str]]:
    """Validate prepared data against contract."""
    errors = []

    for idx, record in enumerate(data):
        try:
            PreparedDataContract(**record)
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    return len(errors) == 0, errors


def validate_ml_features(data: List[dict]) -> tuple[bool, List[str]]:
    """Validate ML training features."""
    errors = []

    for idx, record in enumerate(data):
        try:
            MLFeatureContract(**record)
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    return len(errors) == 0, errors


def validate_ml_predictions(data: List[dict]) -> tuple[bool, List[str]]:
    """Validate ML predictions."""
    errors = []

    for idx, record in enumerate(data):
        try:
            MLPredictionContract(**record)
        except Exception as e:
            errors.append(f"Row {idx}: {str(e)}")

    return len(errors) == 0, errors


def validate_data_quality_metrics(df: pd.DataFrame, tier: DataQualityTier) -> dict:
    """
    Validate data quality metrics.

    Args:
        df: Pandas DataFrame to validate
        tier: Data quality tier (bronze, silver, gold)

    Returns:
        Dictionary with quality metrics
    """
    metrics = {
        "total_records": len(df),
        "missing_values": df.isnull().sum().to_dict(),
        "null_percentage": (df.isnull().sum() / len(df) * 100).to_dict(),
        "duplicate_count": df.duplicated().sum(),
        "column_count": len(df.columns),
        "data_types": df.dtypes.to_dict(),
    }

    # Tier-specific validation
    if tier == DataQualityTier.BRONZE:
        # Raw data - just check basic structure
        metrics["quality_score"] = 0.5
        metrics["passed"] = len(df) > 0

    elif tier == DataQualityTier.SILVER:
        # Cleaned data - should have minimal nulls
        null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        duplicate_percentage = df.duplicated().sum() / len(df) * 100

        metrics["quality_score"] = max(0, 100 - null_percentage - duplicate_percentage) / 100
        metrics["passed"] = (null_percentage < 5) and (duplicate_percentage < 1)

    elif tier == DataQualityTier.GOLD:
        # Production-ready - strict requirements
        null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100
        duplicate_percentage = df.duplicated().sum() / len(df) * 100

        metrics["quality_score"] = max(0, 100 - null_percentage - duplicate_percentage) / 100
        metrics["passed"] = (null_percentage == 0) and (duplicate_percentage == 0)

    return metrics


def test_contracts():
    """Test data contracts."""

    # Test source data contract
    valid_source = {
        "order_id": "123",
        "customer_id": "cust_001",
        "seller_id": "sell_001",
        "payment_value": 50000.00,
        "order_status": "delivered",
    }

    try:
        SourceDataContract(**valid_source)
        print(" Source data contract validation passed")
    except Exception as e:
        print(f" Source data contract validation failed: {e}")

    # Test prepared data contract
    valid_prepared = {
        "corporate_id": "CORP_001",
        "revenue": 1000000.00,
        "profit": 250000.00,
        "profit_margin": 0.25,
        "market": "retail",
        "state": "SP",
        "activity_places": 50,
        "transaction_count": 1000,
    }

    try:
        PreparedDataContract(**valid_prepared)
        print(" Prepared data contract validation passed")
    except Exception as e:
        print(f" Prepared data contract validation failed: {e}")

    # Test ML feature contract
    valid_ml_features = {
        "corporate_id": "CORP_001",
        "revenue": 1000000.00,
        "profit": 250000.00,
        "profit_margin": 0.25,
        "activity_places": 50,
        "label": 1,
    }

    try:
        MLFeatureContract(**valid_ml_features)
        print(" ML feature contract validation passed")
    except Exception as e:
        print(f" ML feature contract validation failed: {e}")

    # Test invalid data
    invalid_prepared = {
        "corporate_id": "CORP_001",
        "revenue": 1000000.00,
        "profit": 250000.00,
        "profit_margin": 0.50,  # Wrong - should be ~0.25
        "market": "retail",
        "state": "SP",
        "activity_places": 50,
        "transaction_count": 1000,
    }

    try:
        PreparedDataContract(**invalid_prepared)
        print(" Invalid data should have been rejected")
    except Exception as e:
        print(f" Invalid data correctly rejected: {e}")


if __name__ == "__main__":
    test_contracts()
