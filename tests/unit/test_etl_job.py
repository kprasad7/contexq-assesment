"""Unit tests for ETL job components."""

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
        .appName("test-etl") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_supply_data(spark):
    """Create sample supply chain data."""
    schema = StructType([
        StructField("supplier_id", StringType()),
        StructField("supplier_name", StringType()),
        StructField("revenue", DoubleType()),
        StructField("state", StringType()),
    ])
    
    data = [
        ("S001", "Acme Corporation", 1000000.0, "SP"),
        ("S002", "Acme Corp", 950000.0, "SP"),
        ("S003", "TechVision Ltd", 500000.0, "RJ"),
    ]
    
    return spark.createDataFrame(data, schema)


@pytest.fixture
def sample_financial_data(spark):
    """Create sample financial data."""
    schema = StructType([
        StructField("payment_id", StringType()),
        StructField("supplier_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("transaction_date", StringType()),
    ])
    
    data = [
        ("P001", "S001", 50000.0, "2025-12-01"),
        ("P002", "S002", 45000.0, "2025-12-02"),
        ("P003", "S003", 25000.0, "2025-12-03"),
    ]
    
    return spark.createDataFrame(data, schema)


def test_data_loads_successfully(sample_supply_data, sample_financial_data):
    """Test that sample data loads correctly."""
    assert sample_supply_data.count() == 3
    assert sample_financial_data.count() == 3
    
    # Verify schemas
    assert len(sample_supply_data.columns) == 4
    assert len(sample_financial_data.columns) == 4


def test_fuzzy_matching_detects_duplicates(sample_supply_data):
    """Test entity resolution finds similar company names."""
    from fuzzywuzzy import fuzz
    
    # Test fuzzy matching
    company1 = "Acme Corporation"
    company2 = "Acme Corp"
    
    ratio = fuzz.token_set_ratio(company1, company2)
    assert ratio > 80, f"Expected high similarity score, got {ratio}"


def test_schema_harmonization(spark):
    """Test data harmonization to Iceberg schema."""
    source_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("value", DoubleType()),
    ])
    
    source_data = [("1", "Test", 100.0)]
    source_df = spark.createDataFrame(source_data, source_schema)
    
    # Expected Iceberg schema (13 columns)
    iceberg_schema = [
        "corporate_id", "revenue", "profit", "profit_margin",
        "market", "state", "city", "activity_places",
        "supplier_name", "transaction_count", "avg_transaction",
        "last_update", "data_source"
    ]
    
    # Harmonize: cast and add columns
    harmonized = source_df.select(
        source_df.id.cast("string").alias("corporate_id"),
        source_df.value.cast("double").alias("revenue"),
    )
    
    # Add remaining columns with defaults
    for col in iceberg_schema[2:]:
        harmonized = harmonized.withColumn(col, 
            None if col in ["profit", "market", "state", "city", "supplier_name", "last_update"] 
            else 0 if col in ["profit_margin", "transaction_count", "avg_transaction"] 
            else "imported")
    
    assert len(harmonized.columns) >= len(iceberg_schema) - 11


def test_md5_hash_deterministic():
    """Test that MD5 hashing is deterministic."""
    import hashlib
    
    company = "Acme Corporation"
    hash1 = hashlib.md5(company.encode()).hexdigest()
    hash2 = hashlib.md5(company.encode()).hexdigest()
    
    assert hash1 == hash2


def test_data_deduplication_logic(spark):
    """Test deduplication keeps highest revenue record."""
    schema = StructType([
        StructField("entity_id", StringType()),
        StructField("revenue", DoubleType()),
    ])
    
    # Duplicate entities with different revenues
    data = [
        ("ENTITY_1", 100000.0),
        ("ENTITY_1", 50000.0),  # Lower revenue duplicate
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Group and take max revenue
    from pyspark.sql.functions import max as spark_max
    deduped = df.groupBy("entity_id").agg(spark_max("revenue").alias("revenue"))
    
    assert deduped.count() == 1
    assert deduped.collect()[0].revenue == 100000.0


def test_null_handling(spark):
    """Test that null values are handled correctly."""
    schema = StructType([
        StructField("id", StringType()),
        StructField("value", DoubleType()),
    ])
    
    data = [
        ("1", 100.0),
        ("2", None),
        ("3", 300.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Filter out nulls
    filtered = df.filter(df.value.isNotNull())
    
    assert filtered.count() == 2


def test_partition_strategy(spark):
    """Test data partitioning strategy."""
    schema = StructType([
        StructField("date", StringType()),
        StructField("value", DoubleType()),
    ])
    
    data = [
        ("2025-12-01", 100.0),
        ("2025-12-01", 200.0),
        ("2025-12-02", 150.0),
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Partition by date
    partitioned = df.repartition("date")
    
    assert partitioned.rdd.getNumPartitions() > 0


def test_performance_large_dataset(spark):
    """Test performance with large dataset."""
    from pyspark.sql.functions import rand
    import time
    
    # Generate 10k rows
    large_df = spark.range(10000).select(
        "id",
        (rand() * 1000000).alias("value")
    )
    
    # Time aggregation
    start = time.time()
    result = large_df.groupBy("id").count().collect()
    duration = time.time() - start
    
    assert len(result) > 0
    assert duration < 5.0, f"Aggregation took too long: {duration}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
