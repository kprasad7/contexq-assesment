#!/usr/bin/env python3
"""
Local data preparation validation script.
Tests data transformation logic before Glue deployment.
"""

import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, when, lit, trim, lower, coalesce, 
        count, isnan, isnull, collect_list, concat, 
        row_number, sum as spark_sum, current_timestamp
    )
    from pyspark.sql.window import Window
except ImportError:
    logger.error("PySpark not found. Install with: pip install pyspark")
    sys.exit(1)

def initialize_spark():
    """Initialize Spark session."""
    logger.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("data-preparation-validation") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("✓ Spark session initialized")
    return spark

def read_csv_data(spark, bucket_name):
    """Read CSV files from S3."""
    logger.info(f"Reading datasets from S3 bucket: {bucket_name}")
    
    try:
        order_items_path = f"s3://{bucket_name}/source_supply/olist_order_items_dataset.csv"
        order_items_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(order_items_path)
        logger.info(f"✓ Order Items: {order_items_df.count():,} records")
        
        payments_path = f"s3://{bucket_name}/source_financial/olist_order_payments_dataset.csv"
        payments_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(payments_path)
        logger.info(f"✓ Payments: {payments_df.count():,} records")
        
        sellers_path = f"s3://{bucket_name}/source_supply/olist_sellers_dataset.csv"
        sellers_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(sellers_path)
        logger.info(f"✓ Sellers: {sellers_df.count():,} records")
        
        return order_items_df, payments_df, sellers_df
        
    except Exception as e:
        logger.error(f"Failed to read CSV data: {str(e)}")
        raise

def validate_data_quality(spark, order_items_df, payments_df, sellers_df):
    """Perform data quality checks."""
    logger.info("\n=== DATA QUALITY ASSESSMENT ===")
    
    order_items_df.createOrReplaceTempView("order_items")
    payments_df.createOrReplaceTempView("payments")
    sellers_df.createOrReplaceTempView("sellers")
    
    quality_check = spark.sql("""
    SELECT 
        'order_items' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_ids,
        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_prices,
        SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_products
    FROM order_items
    UNION ALL
    SELECT 
        'payments' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
        SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) as null_payments,
        0 as null_products
    FROM payments
    UNION ALL
    SELECT 
        'sellers' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_ids,
        SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) as null_cities,
        0 as null_products
    FROM sellers
    """)
    
    quality_check.show(truncate=False)
    logger.info("✓ Data quality assessment complete")

def prepare_source1_supply_chain(spark):
    """Prepare Source 1: Supply Chain Data."""
    logger.info("\n=== PREPARING SOURCE 1: SUPPLY CHAIN ===")
    
    source1_df = spark.sql("""
    SELECT 
        oi.seller_id as corporate_id,
        s.seller_city as corporate_name,
        CONCAT(s.seller_city, ', ', s.seller_state) as address,
        s.seller_city as city,
        s.seller_state as state,
        1 as activity_places,
        COLLECT_LIST(DISTINCT oi.product_id) as top_suppliers,
        CAST(NULL AS STRING) as main_customers,
        CAST(ROUND(SUM(oi.price), 2) AS DECIMAL(18,2)) as revenue,
        CAST(ROUND(SUM(oi.price) * 0.2, 2) AS DECIMAL(18,2)) as profit,
        'olist_supply_chain' as source_system,
        CURRENT_TIMESTAMP() as load_date
    FROM order_items oi
    LEFT JOIN sellers s ON oi.seller_id = s.seller_id
    GROUP BY oi.seller_id, s.seller_city, s.seller_state
    """)
    
    record_count = source1_df.count()
    logger.info(f"✓ Source 1 prepared: {record_count:,} records")
    
    source1_df.show(5, truncate=True)
    logger.info(f"Schema: {source1_df.schema}")
    
    return source1_df

def prepare_source2_financial(spark):
    """Prepare Source 2: Financial Data."""
    logger.info("\n=== PREPARING SOURCE 2: FINANCIAL ===")
    
    source2_df = spark.sql("""
    SELECT 
        ROW_NUMBER() OVER (ORDER BY order_id) as corporate_id,
        CONCAT('Order_', order_id) as corporate_name,
        CAST(NULL AS STRING) as address,
        CAST(NULL AS STRING) as city,
        CAST(NULL AS STRING) as state,
        1 as activity_places,
        CAST(NULL AS STRING) as top_suppliers,
        payment_type as main_customers,
        CAST(ROUND(SUM(payment_value), 2) AS DECIMAL(18,2)) as revenue,
        CAST(ROUND(SUM(payment_value) * 0.15, 2) AS DECIMAL(18,2)) as profit,
        'olist_financial' as source_system,
        CURRENT_TIMESTAMP() as load_date
    FROM payments
    GROUP BY order_id, payment_type
    """)
    
    record_count = source2_df.count()
    logger.info(f"✓ Source 2 prepared: {record_count:,} records")
    
    source2_df.show(5, truncate=True)
    logger.info(f"Schema: {source2_df.schema}")
    
    return source2_df

def write_prepared_sources(source1_df, source2_df, target_bucket):
    """Write prepared data sources to S3."""
    logger.info(f"\n=== WRITING PREPARED SOURCES TO S3 ===")
    
    try:
        source1_output = f"s3://{target_bucket}/prepared_sources/source1_supply/"
        logger.info(f"Writing Source 1 to {source1_output}...")
        source1_df.write.mode("overwrite").parquet(source1_output)
        logger.info(f"✓ Source 1 written successfully")
        
        source2_output = f"s3://{target_bucket}/prepared_sources/source2_financial/"
        logger.info(f"Writing Source 2 to {source2_output}...")
        source2_df.write.mode("overwrite").parquet(source2_output)
        logger.info(f"✓ Source 2 written successfully")
        
    except Exception as e:
        logger.error(f"Failed to write to S3: {str(e)}")
        raise

def main():
    """Main validation script."""
    logger.info("╔════════════════════════════════════════════════════════════╗")
    logger.info("║  OLIST Data Preparation Validation Script                 ║")
    logger.info("║  Validates ETL transformations before Glue deployment      ║")
    logger.info("╚════════════════════════════════════════════════════════════╝\n")
    
    # Configuration
    SOURCE_BUCKET = "contexq-dev-raw-data-119287772129"
    TARGET_BUCKET = "contexq-dev-processed-data-119287772129"
    
    try:
        # Initialize Spark
        spark = initialize_spark()
        
        # Read CSV data
        order_items_df, payments_df, sellers_df = read_csv_data(spark, SOURCE_BUCKET)
        
        # Validate data quality
        validate_data_quality(spark, order_items_df, payments_df, sellers_df)
        
        # Prepare sources
        source1_df = prepare_source1_supply_chain(spark)
        source2_df = prepare_source2_financial(spark)
        
        # Write prepared sources
        write_prepared_sources(source1_df, source2_df, TARGET_BUCKET)
        
        logger.info("\n╔════════════════════════════════════════════════════════════╗")
        logger.info("║  ✓ DATA PREPARATION VALIDATION COMPLETE                   ║")
        logger.info("║  Ready for ETL pipeline deployment                        ║")
        logger.info("╚════════════════════════════════════════════════════════════╝\n")
        
        spark.stop()
        return 0
        
    except Exception as e:
        logger.error(f"\n✗ Validation failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
