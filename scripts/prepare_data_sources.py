"""
Data preparation script to validate and prepare OLIST datasets for ETL pipeline.
Reads CSV files from S3, validates schema, and prepares for entity resolution.
"""

import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, trim, lower, coalesce
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_and_prepare():
    """Validate and prepare data sources."""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("data-preparation") \
        .getOrCreate()
    
    try:
        # Define expected schemas
        order_items_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
        ])
        
        payments_schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("payment_sequential", IntegerType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("payment_value", DoubleType(), True),
        ])
        
        sellers_schema = StructType([
            StructField("seller_id", StringType(), True),
            StructField("seller_zip_code_prefix", StringType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
        ])
        
        # Read data from S3
        logger.info("Reading order items dataset...")
        order_items = spark.read \
            .option("header", "true") \
            .schema(order_items_schema) \
            .csv("s3://contexq-dev-raw-data-119287772129/source_supply/olist_order_items_dataset.csv")
        
        logger.info("Reading order payments dataset...")
        payments = spark.read \
            .option("header", "true") \
            .schema(payments_schema) \
            .csv("s3://contexq-dev-raw-data-119287772129/source_financial/olist_order_payments_dataset.csv")
        
        logger.info("Reading sellers dataset...")
        sellers = spark.read \
            .option("header", "true") \
            .schema(sellers_schema) \
            .csv("s3://contexq-dev-raw-data-119287772129/source_supply/olist_sellers_dataset.csv")
        
        # Data profiling
        logger.info(f"Order Items: {order_items.count()} records")
        logger.info(f"Payments: {payments.count()} records")
        logger.info(f"Sellers: {sellers.count()} records")
        
        # Data quality checks
        logger.info("Running data quality checks...")
        
        # Check for nulls
        null_order_items = order_items.filter(col("seller_id").isNull()).count()
        null_payments = payments.filter(col("order_id").isNull()).count()
        null_sellers = sellers.filter(col("seller_id").isNull()).count()
        
        logger.info(f"Null seller_ids in order_items: {null_order_items}")
        logger.info(f"Null order_ids in payments: {null_payments}")
        logger.info(f"Null seller_ids in sellers: {null_sellers}")
        
        # Prepare source1 (supply chain): Order items + Sellers
        logger.info("Preparing Source 1 (Supply Chain)...")
        source1 = order_items.join(
            sellers,
            on="seller_id",
            how="left"
        ).select(
            col("seller_id").alias("corporate_id"),
            col("seller_city").alias("corporate_name"),
            col("seller_city").alias("address"),
            col("seller_state").alias("state"),
            lit(1).alias("activity_places"),
            col("product_id").alias("top_suppliers"),
            lit(None).alias("main_customers"),
            col("price").alias("revenue"),
            lit(0).alias("profit"),
            lit("supply_chain").alias("source_system")
        )
        
        # Prepare source2 (financial): Order payments aggregated
        logger.info("Preparing Source 2 (Financial)...")
        source2 = payments.groupBy("order_id").agg(
            {"payment_value": "sum"}
        ).withColumnRenamed("sum(payment_value)", "total_revenue").select(
            col("order_id").alias("corporate_id"),
            lit("Financial Entity").alias("corporate_name"),
            lit(None).alias("address"),
            lit(None).alias("state"),
            lit(None).alias("activity_places"),
            lit(None).alias("top_suppliers"),
            lit("unknown").alias("main_customers"),
            col("total_revenue").alias("revenue"),
            col("total_revenue").alias("profit"),
            lit("financial").alias("source_system")
        )
        
        # Save prepared sources to S3
        logger.info("Writing prepared sources to S3...")
        
        source1.write.mode("overwrite").parquet(
            "s3://contexq-dev-raw-data-119287772129/prepared_sources/source1_supply"
        )
        logger.info("✓ Source 1 (Supply) written")
        
        source2.write.mode("overwrite").parquet(
            "s3://contexq-dev-raw-data-119287772129/prepared_sources/source2_financial"
        )
        logger.info("✓ Source 2 (Financial) written")
        
        # Summary
        logger.info(f"✓ Data preparation complete!")
        logger.info(f"  Source 1 records: {source1.count()}")
        logger.info(f"  Source 2 records: {source2.count()}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error: {str(e)}", exc_info=True)
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(validate_and_prepare())
