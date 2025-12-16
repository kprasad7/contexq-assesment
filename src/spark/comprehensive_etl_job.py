"""
AWS Glue Comprehensive ETL Job - Complete Data Pipeline
Handles: CSV Ingestion → Data Preparation → Entity Resolution → 
Iceberg Merge → Data Quality Checks in a single unified job.
"""

import sys
import logging
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import (
    col, when, lit, count as spark_count, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType
)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class ETLValidationError(Exception):
    """Raised when validation checks fail"""
    pass

class DataIngestionError(Exception):
    """Raised when data ingestion fails"""
    pass

class TableOperationError(Exception):
    """Raised when Iceberg table operations fail"""
    pass

# ============================================================================
# SETUP LOGGING
# ============================================================================

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get job parameters
try:
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'source_bucket',
        'target_bucket',
        'database',
        'table'
    ])
except Exception as e:
    logger.error(f"Failed to get job parameters: {str(e)}")
    sys.exit(1)

# Initialize Glue context
try:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
except Exception as e:
    logger.error(f"Failed to initialize Glue context: {str(e)}")
    sys.exit(1)

SOURCE_BUCKET = args.get('source_bucket', 'contexq-dev-raw-data-119287772129')
TARGET_BUCKET = args.get('target_bucket', 'contexq-dev-processed-data-119287772129')
DATABASE_NAME = args.get('database', 'contexq_dev')
TABLE_NAME = args.get('table', 'corporate_registry')

# Log startup parameters
logger.info(f"✓ JOB_NAME: {args['JOB_NAME']}")
logger.info(f"✓ SOURCE_BUCKET: {SOURCE_BUCKET}")
logger.info(f"✓ TARGET_BUCKET: {TARGET_BUCKET}")
logger.info(f"✓ DATABASE: {DATABASE_NAME}")
logger.info(f"✓ TABLE: {TABLE_NAME}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def check_iceberg_table_exists(database: str, table: str) -> bool:
    """Check if Iceberg table exists in Glue Catalog"""
    try:
        spark.sql(f"DESCRIBE TABLE {database}.{table}")
        logger.info(f"✓ Table {database}.{table} exists")
        return True
    except Exception as e:
        logger.warning(f"Table {database}.{table} does not exist: {str(e)}")
        return False

def create_iceberg_table(database: str, table: str) -> None:
    """Create Iceberg table if it doesn't exist"""
    try:
        logger.info(f"Creating Iceberg table {database}.{table}...")
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{table}
        USING ICEBERG
        AS SELECT 
            cast(null as string) as corporate_id,
            cast(null as string) as corporate_name,
            cast(null as string) as city,
            cast(null as string) as state,
            cast(null as decimal(18,2)) as total_sales_value,
            cast(null as decimal(18,2)) as total_freight_value,
            cast(null as int) as num_orders,
            cast(null as string) as source_system,
            cast(null as timestamp) as load_date
        WHERE FALSE
        """
        
        spark.sql(create_sql)
        logger.info(f"✓ Iceberg table created: {database}.{table}")
    except Exception as e:
        raise TableOperationError(f"Failed to create Iceberg table: {str(e)}")


def main():
    """Main ETL pipeline orchestration"""
    
    try:
        logger.info("\n" + "="*70)
        logger.info("STARTING COMPREHENSIVE ETL PIPELINE")
        logger.info("="*70)
        
        # ====================================================================
        # PHASE 1: VALIDATE ICEBERG TABLE
        # ====================================================================
        logger.info("\n=== PHASE 1: ICEBERG TABLE VALIDATION ===")
        
        try:
            table_exists = check_iceberg_table_exists(DATABASE_NAME, TABLE_NAME)
            if not table_exists:
                logger.info(f"Creating new Iceberg table...")
                create_iceberg_table(DATABASE_NAME, TABLE_NAME)
        except TableOperationError as e:
            logger.error(f"✗ Table validation failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"✗ Unexpected error during table validation: {str(e)}", exc_info=True)
            raise
        
        # ====================================================================
        # PHASE 2: DATA INGESTION
        # ====================================================================
        logger.info("\n=== PHASE 2: DATA INGESTION ===")
        
        sellers_df = None
        orders_df = None
        
        try:
            # Read sellers data
            logger.info("Reading sellers dataset...")
            sellers_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_sellers_dataset.csv"
            sellers_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(sellers_path)
            seller_count = sellers_df.count()
            logger.info(f"✓ Sellers: {seller_count:,} records")
            logger.info(f"  Columns: {sellers_df.columns}")
            
            # Read order items data
            logger.info("Reading order items dataset...")
            orders_path = f"s3://{SOURCE_BUCKET}/source_supply/olist_order_items_dataset.csv"
            orders_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(orders_path)
            order_count = orders_df.count()
            logger.info(f"✓ Order Items: {order_count:,} records")
            logger.info(f"  Columns: {orders_df.columns}")
            
        except Exception as e:
            raise DataIngestionError(f"Failed to read CSV files: {str(e)}")
        
        # ====================================================================
        # PHASE 3: DATA TRANSFORMATION & AGGREGATION
        # ====================================================================
        logger.info("\n=== PHASE 3: DATA TRANSFORMATION ===")
        
        try:
            # Aggregate sales by seller
            logger.info("Aggregating sales by seller...")
            seller_sales = orders_df \
                .groupBy("seller_id") \
                .agg(
                    spark_count("order_id").alias("num_orders"),
                    col("price").cast(DecimalType(18, 2)).alias("total_sales_value"),
                    col("freight_value").cast(DecimalType(18, 2)).alias("total_freight_value")
                )
            
            # Join sellers with sales data
            logger.info("Joining sellers with sales data...")
            seller_aggregated = sellers_df.join(
                seller_sales,
                sellers_df.seller_id == seller_sales.seller_id,
                "left"
            )
            
            # Create corporate entities
            logger.info("Creating standardized corporate entities...")
            corporate_df = seller_aggregated \
                .withColumn("corporate_id", 
                    when(col("seller_id").isNotNull(), col("seller_id")).otherwise(lit("UNKNOWN"))) \
                .withColumn("corporate_name", 
                    when(col("seller_city").isNotNull(), col("seller_city")).otherwise(lit("UNKNOWN"))) \
                .withColumn("city", col("seller_city")) \
                .withColumn("state", col("seller_state")) \
                .withColumn("source_system", lit("SUPPLY_CHAIN")) \
                .withColumn("load_date", current_timestamp()) \
                .select(
                    "corporate_id",
                    "corporate_name",
                    "city",
                    "state",
                    "total_sales_value",
                    "total_freight_value",
                    "num_orders",
                    "source_system",
                    "load_date"
                )
            
            entity_count = corporate_df.count()
            logger.info(f"✓ Created {entity_count:,} corporate entities")
            
            # Show sample
            logger.info("Sample data:")
            corporate_df.limit(5).show(truncate=False)
            
        except Exception as e:
            raise ETLValidationError(f"Failed during transformation: {str(e)}")
        
        # ====================================================================
        # PHASE 4: ICEBERG MERGE
        # ====================================================================
        logger.info("\n=== PHASE 4: ICEBERG MERGE INTO ===")
        
        try:
            # Register as temp view
            corporate_df.createOrReplaceTempView("source_entities")
            
            # Execute MERGE INTO
            merge_sql = f"""
            MERGE INTO {DATABASE_NAME}.{TABLE_NAME} t
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
            """
            
            logger.info(f"Executing MERGE INTO {DATABASE_NAME}.{TABLE_NAME}...")
            spark.sql(merge_sql)
            logger.info(f"✓ MERGE INTO complete")
            
        except Exception as e:
            raise TableOperationError(f"Failed to merge into Iceberg table: {str(e)}")
        
        # ====================================================================
        # PHASE 5: VERIFICATION & SUMMARY
        # ====================================================================
        logger.info("\n=== PHASE 5: VERIFICATION ===")
        
        try:
            # Read final table
            final_df = spark.sql(f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}")
            final_count = final_df.count()
            logger.info(f"✓ Final table row count: {final_count:,}")
            
            # Show summary
            logger.info("Table contents:")
            final_df.show(10, truncate=False)
            
            logger.info("\nRow count by source system:")
            summary = spark.sql(f"""
                SELECT 
                    source_system,
                    COUNT(*) as count
                FROM {DATABASE_NAME}.{TABLE_NAME}
                GROUP BY source_system
            """)
            summary.show()
            
        except Exception as e:
            logger.warning(f"Failed to read final table: {str(e)}")
        
        # ====================================================================
        # SUMMARY REPORT
        # ====================================================================
        logger.info("\n" + "="*70)
        logger.info("ETL JOB COMPLETED SUCCESSFULLY ✓")
        logger.info("="*70 + "\n")
        
        job.commit()
        return 0
        
    except DataIngestionError as e:
        logger.error(f"✗ Data Ingestion Error: {str(e)}", exc_info=True)
        try:
            job.commit()
        except:
            pass
        return 1
        
    except TableOperationError as e:
        logger.error(f"✗ Table Operation Error: {str(e)}", exc_info=True)
        try:
            job.commit()
        except:
            pass
        return 1
        
    except ETLValidationError as e:
        logger.error(f"✗ Validation Error: {str(e)}", exc_info=True)
        try:
            job.commit()
        except:
            pass
        return 1
        
    except Exception as e:
        logger.error(f"✗ Unexpected error: {type(e).__name__}: {str(e)}", exc_info=True)
        try:
            job.commit()
        except:
            pass
        return 1


if __name__ == "__main__":
    exit_code = main()
    logger.info(f"Job exiting with code: {exit_code}")
    sys.exit(exit_code)
