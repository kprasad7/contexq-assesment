"""
AWS Glue data preparation job (serverless PySpark).
Validates and prepares OLIST datasets for entity resolution ETL pipeline.
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql.functions import col, when, lit, trim, lower, coalesce, count, isnan, isnull

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'target_bucket'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SOURCE_BUCKET = args.get('source_bucket', 'contexq-dev-raw-data-119287772129')
TARGET_BUCKET = args.get('target_bucket', 'contexq-dev-processed-data-119287772129')

try:
    logger.info(f"Starting data preparation job...")
    logger.info(f"Source bucket: {SOURCE_BUCKET}")
    logger.info(f"Target bucket: {TARGET_BUCKET}")
    
    # Read CSV files from S3
    logger.info("Reading order items dataset...")
    order_items_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_order_items_dataset.csv"
    order_items_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(order_items_path)
    
    logger.info("Reading order payments dataset...")
    payments_path = f"s3://{SOURCE_BUCKET}/source_financial/olist_order_payments_dataset.csv"
    payments_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(payments_path)
    
    logger.info("Reading sellers dataset...")
    sellers_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_sellers_dataset.csv"
    sellers_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(sellers_path)
    
    # Data profiling
    logger.info(f"Order Items records: {order_items_df.count()}")
    logger.info(f"Payments records: {payments_df.count()}")
    logger.info(f"Sellers records: {sellers_df.count()}")
    
    # Data quality assessment
    logger.info("Performing data quality checks...")
    
    order_items_df.createOrReplaceTempView("order_items")
    payments_df.createOrReplaceTempView("payments")
    sellers_df.createOrReplaceTempView("sellers")
    
    # Check data quality
    quality_check = spark.sql("""
    SELECT 
        'order_items' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_ids,
        SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_prices
    FROM order_items
    UNION ALL
    SELECT 
        'payments' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
        SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) as null_payments
    FROM payments
    UNION ALL
    SELECT 
        'sellers' as dataset,
        COUNT(*) as total_records,
        SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_seller_ids,
        SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) as null_cities
    FROM sellers
    """)
    
    quality_check.show()
    logger.info("Data quality check complete")
    
    # Prepare Source 1: Supply Chain Data (Order Items + Sellers)
    logger.info("Preparing Source 1: Supply Chain...")
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
        CAST(SUM(oi.price) AS DECIMAL(18,2)) as revenue,
        CAST(SUM(oi.price) * 0.2 AS DECIMAL(18,2)) as profit,
        'olist_supply_chain' as source_system,
        CURRENT_TIMESTAMP() as load_date
    FROM order_items oi
    LEFT JOIN sellers s ON oi.seller_id = s.seller_id
    GROUP BY oi.seller_id, s.seller_city, s.seller_state
    """)
    
    logger.info(f"Source 1 prepared: {source1_df.count()} records")
    
    # Prepare Source 2: Financial Data (Order Payments aggregated)
    logger.info("Preparing Source 2: Financial...")
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
        CAST(SUM(payment_value) AS DECIMAL(18,2)) as revenue,
        CAST(SUM(payment_value) * 0.15 AS DECIMAL(18,2)) as profit,
        'olist_financial' as source_system,
        CURRENT_TIMESTAMP() as load_date
    FROM payments
    GROUP BY order_id, payment_type
    """)
    
    logger.info(f"Source 2 prepared: {source2_df.count()} records")
    
    # Write prepared sources to S3 (Parquet for better performance)
    logger.info("Writing prepared data sources to S3...")
    
    source1_output_path = f"s3://{TARGET_BUCKET}/prepared_sources/source1_supply/"
    source1_df.write.mode("overwrite").parquet(source1_output_path)
    logger.info(f"✓ Source 1 written to {source1_output_path}")
    
    source2_output_path = f"s3://{TARGET_BUCKET}/prepared_sources/source2_financial/"
    source2_df.write.mode("overwrite").parquet(source2_output_path)
    logger.info(f"✓ Source 2 written to {source2_output_path}")
    
    # Create summary report
    summary_df = spark.createDataFrame([
        ("source_1_supply", source1_df.count(), "olist_supply_chain"),
        ("source_2_financial", source2_df.count(), "olist_financial"),
    ], ["source_name", "record_count", "source_system"])
    
    summary_path = f"s3://{TARGET_BUCKET}/data_preparation_report/"
    summary_df.write.mode("overwrite").parquet(summary_path)
    logger.info(f"✓ Summary report written to {summary_path}")
    
    logger.info("✓ Data preparation job completed successfully!")
    job.commit()
    
except Exception as e:
    logger.error(f"Job failed: {str(e)}", exc_info=True)
    job.commit()
    raise
