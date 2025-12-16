"""
AWS Glue ETL Job - Simple & Robust
Reads sellers + order items from CSV, aggregates by seller, writes to Iceberg
"""

import sys
import logging

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, sum as spark_sum, count as spark_count, lit, current_timestamp

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("=" * 80)
logger.info("ETL JOB STARTING")
logger.info("=" * 80)

# Get parameters
logger.info("\n[1/8] Getting job parameters...")
try:
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket', 'database', 'table'])
    SOURCE_BUCKET = args['source_bucket']
    TARGET_BUCKET = args['target_bucket']
    DB_NAME = args['database']
    TABLE_NAME = args['table']
    logger.info(f"  ✓ SOURCE: {SOURCE_BUCKET}")
    logger.info(f"  ✓ TARGET: {TARGET_BUCKET}")
    logger.info(f"  ✓ DB: {DB_NAME}")
    logger.info(f"  ✓ TABLE: {TABLE_NAME}")
except Exception as e:
    logger.error(f"Failed to get parameters: {e}", exc_info=True)
    sys.exit(1)

# Initialize contexts
logger.info("\n[2/8] Initializing Spark context...")
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger.info("  ✓ Spark, Glue, and Job contexts initialized")
except Exception as e:
    logger.error(f"Failed to initialize contexts: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 1: READ DATA
# ============================================================================

logger.info("\n[3/8] Reading CSV files...")
try:
    sellers_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_sellers_dataset.csv"
    sellers_df = spark.read.option("header", "true").option("inferSchema", "true").csv(sellers_path)
    seller_count = sellers_df.count()
    logger.info(f"  ✓ Sellers: {seller_count:,} records")
    
    orders_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_order_items_dataset.csv"
    orders_df = spark.read.option("header", "true").option("inferSchema", "true").csv(orders_path)
    order_count = orders_df.count()
    logger.info(f"  ✓ Order Items: {order_count:,} records")
except Exception as e:
    logger.error(f"Failed to read CSV files: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 2: AGGREGATE SALES BY SELLER
# ============================================================================

logger.info("\n[4/8] Aggregating sales by seller...")
try:
    seller_sales = orders_df.groupBy("seller_id").agg(
        spark_count("order_id").alias("num_orders"),
        spark_sum(col("price")).alias("total_sales_value"),
        spark_sum(col("freight_value")).alias("total_freight_value")
    )
    logger.info(f"  ✓ Aggregated {seller_sales.count():,} sellers")
except Exception as e:
    logger.error(f"Failed to aggregate sales: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 3: CREATE CORPORATE ENTITIES
# ============================================================================

logger.info("\n[5/8] Creating corporate entities...")
try:
    # Drop seller_id from seller_sales to avoid ambiguity in join
    seller_sales_clean = seller_sales.drop("seller_id")
    
    corporate_df = sellers_df.join(
        seller_sales_clean,
        on=sellers_df.seller_id == seller_sales_clean.seller_id,
        how="left"
    ).select(
        sellers_df.seller_id.alias("corporate_id"),
        sellers_df.seller_city.alias("corporate_name"),
        sellers_df.seller_city.alias("city"),
        sellers_df.seller_state.alias("state"),
        col("total_sales_value"),
        col("total_freight_value"),
        col("num_orders"),
        lit("SUPPLY_CHAIN").alias("source_system"),
        current_timestamp().alias("load_date")
    )
    
    entity_count = corporate_df.count()
    logger.info(f"  ✓ Created {entity_count:,} corporate entities")
    logger.info("  Sample data:")
    corporate_df.limit(3).show(truncate=False)
except Exception as e:
    logger.error(f"Failed to create corporate entities: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 4: CREATE TABLE IF NEEDED
# ============================================================================

logger.info(f"\n[6/8] Checking/creating Iceberg table {DB_NAME}.{TABLE_NAME}...")
try:
    table_exists = False
    try:
        spark.sql(f"DESCRIBE TABLE {DB_NAME}.{TABLE_NAME}")
        table_exists = True
    except:
        pass
    
    if not table_exists:
        logger.info(f"  Creating table {DB_NAME}.{TABLE_NAME}...")
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DB_NAME}.{TABLE_NAME}
        USING ICEBERG
        LOCATION 's3://{TARGET_BUCKET}/warehouse/{TABLE_NAME}/'
        AS SELECT 
            cast(null as string) as corporate_id,
            cast(null as string) as corporate_name,
            cast(null as string) as city,
            cast(null as string) as state,
            cast(null as double) as total_sales_value,
            cast(null as double) as total_freight_value,
            cast(null as int) as num_orders,
            cast(null as string) as source_system,
            cast(null as timestamp) as load_date
        WHERE FALSE
        """)
    
    logger.info(f"  ✓ Table ready: {DB_NAME}.{TABLE_NAME}")
except Exception as e:
    logger.error(f"Failed to create table: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 5: MERGE INTO ICEBERG
# ============================================================================

logger.info(f"\n[7/8] Merging data into Iceberg table...")
try:
    corporate_df.createOrReplaceTempView("source_entities")
    
    spark.sql(f"""
    MERGE INTO {DB_NAME}.{TABLE_NAME} t
    USING source_entities s
    ON t.corporate_id = s.corporate_id AND t.source_system = s.source_system
    WHEN MATCHED THEN UPDATE SET
        corporate_name = s.corporate_name,
        city = s.city,
        state = s.state,
        total_sales_value = s.total_sales_value,
        total_freight_value = s.total_freight_value,
        num_orders = s.num_orders,
        load_date = s.load_date
    WHEN NOT MATCHED THEN INSERT (
        corporate_id, corporate_name, city, state,
        total_sales_value, total_freight_value, num_orders,
        source_system, load_date
    ) VALUES (
        s.corporate_id, s.corporate_name, s.city, s.state,
        s.total_sales_value, s.total_freight_value, s.num_orders,
        s.source_system, s.load_date
    )
    """)
    logger.info(f"  ✓ MERGE INTO completed")
except Exception as e:
    logger.error(f"Failed to merge into Iceberg: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# PHASE 6: VERIFY RESULTS
# ============================================================================

logger.info(f"\n[8/8] Verifying results...")
try:
    final_df = spark.sql(f"SELECT * FROM {DB_NAME}.{TABLE_NAME}")
    final_count = final_df.count()
    logger.info(f"  ✓ Final row count: {final_count:,}")
    logger.info("  Final table contents (first 10 rows):")
    final_df.limit(10).show(truncate=False)
except Exception as e:
    logger.error(f"Failed to verify results: {e}", exc_info=True)
    sys.exit(1)

# ============================================================================
# COMPLETE
# ============================================================================

try:
    job.commit()
    logger.info("\n" + "=" * 80)
    logger.info("ETL JOB COMPLETED SUCCESSFULLY ✓")
    logger.info("=" * 80)
    sys.exit(0)
except Exception as e:
    logger.error(f"Failed to commit job: {e}", exc_info=True)
    sys.exit(1)
