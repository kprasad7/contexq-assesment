"""Unit tests for ML training job."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src/spark'))


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for tests."""
    spark = SparkSession.builder \
        .appName("test-ml") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_training_data(spark):
    """Create sample training data for ML model."""
    schema = StructType([
        StructField("corporate_id", StringType()),
        StructField("revenue", DoubleType()),
        StructField("profit", DoubleType()),
        StructField("profit_margin", DoubleType()),
        StructField("activity_places", IntegerType()),
        StructField("label", IntegerType()),  # 1 = high_profit, 0 = low_profit
    ])
    
    data = [
        ("C001", 1000000.0, 200000.0, 0.20, 50, 1),
        ("C002", 500000.0, 75000.0, 0.15, 20, 1),
        ("C003", 200000.0, 20000.0, 0.10, 5, 0),
        ("C004", 800000.0, 80000.0, 0.10, 30, 0),
        ("C005", 1500000.0, 375000.0, 0.25, 100, 1),
        ("C006", 300000.0, 30000.0, 0.10, 10, 0),
        ("C007", 900000.0, 225000.0, 0.25, 60, 1),
        ("C008", 400000.0, 40000.0, 0.10, 15, 0),
    ]
    
    return spark.createDataFrame(data, schema)


def test_data_loads_successfully(sample_training_data):
    """Test that training data loads correctly."""
    assert sample_training_data.count() == 8
    assert len(sample_training_data.columns) == 6


def test_label_distribution(sample_training_data):
    """Test balanced label distribution."""
    from pyspark.sql.functions import col, count
    
    label_counts = sample_training_data.groupBy("label").count().collect()
    
    # Should have both classes
    assert len(label_counts) == 2
    
    # Extract counts
    counts = {row.label: row["count"] for row in label_counts}
    assert 0 in counts and 1 in counts


def test_feature_scaling():
    """Test feature scaling for ML pipeline."""
    from sklearn.preprocessing import StandardScaler
    import numpy as np
    
    # Sample features
    features = np.array([
        [1000000.0, 0.20],
        [500000.0, 0.15],
        [200000.0, 0.10],
    ]).reshape(-1, 2)
    
    scaler = StandardScaler()
    scaled = scaler.fit_transform(features)
    
    # Check scaling worked
    assert scaled.shape == features.shape
    # Mean should be ~0, std should be ~1
    assert abs(scaled.mean()) < 0.1


def test_train_test_split_ratio(sample_training_data):
    """Test train/test split maintains ratio."""
    train, test = sample_training_data.randomSplit([0.8, 0.2], seed=42)
    
    total = sample_training_data.count()
    train_count = train.count()
    test_count = test.count()
    
    assert train_count + test_count == total
    # Allow 5% tolerance in split ratio
    assert 0.75 <= train_count / total <= 0.85


def test_feature_engineering(spark):
    """Test feature engineering calculations."""
    from pyspark.sql.functions import col, when
    
    schema = StructType([
        StructField("revenue", DoubleType()),
        StructField("profit", DoubleType()),
        StructField("activity_places", IntegerType()),
    ])
    
    data = [
        (1000000.0, 200000.0, 50),
        (500000.0, 50000.0, 25),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Calculate derived features
    df_features = df.withColumn(
        "profit_margin",
        col("profit") / col("revenue")
    ).withColumn(
        "avg_profit_per_location",
        col("profit") / col("activity_places")
    )
    
    assert "profit_margin" in df_features.columns
    assert "avg_profit_per_location" in df_features.columns
    
    # Verify calculations
    row = df_features.first()
    assert abs(row.profit_margin - 0.2) < 0.01


def test_model_prediction_probability():
    """Test that model produces valid probabilities."""
    from sklearn.linear_model import LogisticRegression
    import numpy as np
    
    # Train simple model
    X_train = np.array([[0.5, 0.2], [0.8, 0.3], [0.3, 0.1], [0.6, 0.25]])
    y_train = np.array([1, 1, 0, 1])
    
    model = LogisticRegression(random_state=42)
    model.fit(X_train, y_train)
    
    # Get predictions
    predictions = model.predict_proba(X_train)
    
    # Check probabilities sum to 1
    assert np.allclose(predictions.sum(axis=1), 1.0)
    # Check range [0, 1]
    assert np.all(predictions >= 0) and np.all(predictions <= 1)


def test_model_metrics_calculation():
    """Test metrics calculation (AUC, F1)."""
    from sklearn.metrics import roc_auc_score, f1_score
    import numpy as np
    
    y_true = np.array([0, 1, 1, 0, 1])
    y_pred = np.array([0.1, 0.9, 0.8, 0.2, 0.7])
    y_pred_binary = (y_pred > 0.5).astype(int)
    
    auc = roc_auc_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred_binary)
    
    # Metrics should be in valid range
    assert 0 <= auc <= 1
    assert 0 <= f1 <= 1


def test_class_imbalance_handling(spark):
    """Test handling of class imbalance."""
    schema = StructType([
        StructField("features", DoubleType()),
        StructField("label", IntegerType()),
    ])
    
    # Imbalanced data: 90% class 0, 10% class 1
    data = [(0.5, 0)] * 90 + [(0.6, 1)] * 10
    df = spark.createDataFrame(data, schema)
    
    # Count distribution
    counts = df.groupBy("label").count().collect()
    assert len(counts) == 2


def test_missing_values_handling(spark):
    """Test handling of missing values."""
    schema = StructType([
        StructField("feature", DoubleType()),
        StructField("label", IntegerType()),
    ])
    
    data = [
        (1.0, 1),
        (None, 0),  # Missing value
        (2.0, 1),
        (None, 0),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Filter nulls
    clean = df.filter(df.feature.isNotNull())
    
    assert clean.count() == 2


def test_model_serialization():
    """Test that models can be serialized."""
    from sklearn.linear_model import LogisticRegression
    import pickle
    import tempfile
    
    model = LogisticRegression(random_state=42)
    X = [[0, 0], [1, 1]]
    y = [0, 1]
    model.fit(X, y)
    
    # Serialize
    with tempfile.NamedTemporaryFile(delete=False) as f:
        pickle.dump(model, f)
        model_path = f.name
    
    # Deserialize
    with open(model_path, 'rb') as f:
        loaded_model = pickle.load(f)
    
    # Verify
    pred_original = model.predict(X)
    pred_loaded = loaded_model.predict(X)
    
    assert (pred_original == pred_loaded).all()


def test_feature_importance():
    """Test feature importance extraction."""
    from sklearn.linear_model import LogisticRegression
    import numpy as np
    
    X = np.array([
        [1.0, 2.0, 3.0],
        [2.0, 3.0, 4.0],
        [3.0, 4.0, 5.0],
        [4.0, 5.0, 6.0],
    ])
    y = np.array([0, 0, 1, 1])
    
    model = LogisticRegression(random_state=42)
    model.fit(X, y)
    
    # Get coefficients (importance)
    importance = np.abs(model.coef_[0])
    
    assert len(importance) == 3
    assert np.all(importance >= 0)


def test_hyperparameter_validation():
    """Test model hyperparameter ranges."""
    from sklearn.linear_model import LogisticRegression
    
    # Valid parameters
    model = LogisticRegression(
        C=1.0,  # Regularization strength
        max_iter=100,
        solver='lbfgs',
        random_state=42
    )
    
    assert model.C == 1.0
    assert model.max_iter == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
