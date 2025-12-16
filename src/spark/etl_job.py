"""
AWS Glue ETL job for entity resolution, deduplication, and harmonization.
Processes prepared sources and creates corporate_registry Iceberg table.
"""

import sys
import logging
from typing import Dict, List, Tuple
from datetime import datetime
from hashlib import md5

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import Window
from pyspark.sql.functions import (
    col, when, lit, trim, lower, concat_ws, coalesce,
    md5 as spark_md5, row_number, count as spark_count,
    collect_list, struct, array_contains, concat, isnan, isnull,
    levenshtein, current_timestamp, split, array_join
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, ArrayType, DoubleType
)

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

# Iceberg table schema
ICEBERG_SCHEMA = StructType([
    StructField("corporate_id", StringType(), False),
    StructField("corporate_name", StringType(), False),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("activity_places", IntegerType(), True),
    StructField("top_suppliers", ArrayType(StringType()), True),
    StructField("main_customers", StringType(), True),
    StructField("revenue", DecimalType(18, 2), True),
    StructField("profit", DecimalType(18, 2), True),
    StructField("source_system", StringType(), False),
    StructField("load_date", TimestampType(), False),
    StructField("entity_hash", StringType(), False),
])


class EntityResolutionEngine:
    """Handles entity matching and deduplication."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("EntityResolutionEngine initialized")
    
    def normalize_text(self, text: str) -> str:
        """Normalize text for matching."""
        return text.lower().strip() if text else ""
    
    def create_entity_hash(self, row_dict: Dict) -> str:
        """Create deterministic hash for entity."""
        hash_input = f"{row_dict.get('corporate_name', '')}|{row_dict.get('city', '')}"
        return md5(hash_input.encode()).hexdigest()
    
    def resolve_duplicates(self, df):
        """Identify and resolve duplicate entities using fuzzy matching."""
        logger.info("Starting entity resolution...")
        
        # Create normalized columns for matching
        df_norm = df.withColumn(
            "norm_name", 
            trim(lower(col("corporate_name")))
        )
        
        # Add entity hash
        df_with_hash = df_norm.withColumn(
            "entity_hash",
            spark_md5(concat_ws("|", col("corporate_name"), col("city")))
        )
        
        # Window function to find duplicates
        window_spec = Window.partitionBy("norm_name").orderBy(col("revenue").desc())
        df_ranked = df_with_hash.withColumn("rank", row_number().over(window_spec))
        
        # Keep only best match (highest revenue)
        df_deduped = df_ranked.filter(col("rank") == 1).drop("rank", "norm_name")
        
        logger.info(f"✓ Deduplication complete: {df_deduped.count()} unique entities")
        return df_deduped


class DataHarmonizer:
    """Harmonizes data schema and formats."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("DataHarmonizer initialized")
    
    def harmonize_schema(self, df):
        """Harmonize data to Iceberg schema."""
        logger.info("Harmonizing schema...")
        
        # Select and cast columns
        df_harmonized = df.select(
            col("corporate_id").cast(StringType()),
            col("corporate_name").cast(StringType()),
            col("address").cast(StringType()),
            col("city").cast(StringType()),
            col("state").cast(StringType()),
            col("activity_places").cast(IntegerType()),
            col("top_suppliers").cast(ArrayType(StringType())),
            col("main_customers").cast(StringType()),
            col("revenue").cast(DecimalType(18, 2)),
            col("profit").cast(DecimalType(18, 2)),
            col("source_system").cast(StringType()),
            col("load_date").cast(TimestampType()),
            col("entity_hash").cast(StringType()),
        )
        
        logger.info("✓ Schema harmonization complete")
        return df_harmonized


class IcebergMerger:
    """Handles Iceberg MERGE INTO operations."""
    
    def __init__(self, spark):
        self.spark = spark
        logger.info("IcebergMerger initialized")
    
    def merge_into_iceberg(self, df, table_name: str, database: str = "contexq_dev"):
        """Perform ACID MERGE INTO Iceberg table."""
        logger.info(f"Starting MERGE INTO {database}.{table_name}...")
        
        # Register source as temp view
        df.createOrReplaceTempView("source_entities")
        
        # Execute MERGE INTO (UPSERT)
        merge_sql = f"""
        MERGE INTO {database}.{table_name} t
        USING source_entities s
        ON t.corporate_id = s.corporate_id AND t.source_system = s.source_system
        WHEN MATCHED THEN UPDATE SET
            corporate_name = s.corporate_name,
            address = s.address,
            city = s.city,
            state = s.state,
            activity_places = s.activity_places,
            top_suppliers = s.top_suppliers,
            main_customers = s.main_customers,
            revenue = s.revenue,
            profit = s.profit,
            load_date = s.load_date,
            entity_hash = s.entity_hash
        WHEN NOT MATCHED THEN INSERT (
            corporate_id, corporate_name, address, city, state,
            activity_places, top_suppliers, main_customers, revenue, profit,
            source_system, load_date, entity_hash
        ) VALUES (
            s.corporate_id, s.corporate_name, s.address, s.city, s.state,
            s.activity_places, s.top_suppliers, s.main_customers, s.revenue, s.profit,
            s.source_system, s.load_date, s.entity_hash
        )
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.info("✓ MERGE INTO completed successfully")
            
            # Get record counts
            count_result = self.spark.sql(f"SELECT COUNT(*) as count FROM {database}.{table_name}")
            total_count = count_result.collect()[0]['count']
            logger.info(f"✓ Total records in {table_name}: {total_count:,}")
            
            return True
        except Exception as e:
            logger.error(f"✗ MERGE INTO failed: {str(e)}")
            raise


def main():
    """Main ETL orchestration."""
    logger.info("Starting ETL job: Entity Resolution and Iceberg Merge")
    logger.info("Processing OLIST Supply Chain and Financial Data")
    
    try:
        # Read prepared sources
        logger.info("Reading prepared sources from S3...")
        
        source1_path = f"s3://{TARGET_BUCKET}/prepared_sources/source1_supply/"
        source1_df = spark.read.parquet(source1_path)
        logger.info(f"✓ Source 1 (supply chain): {source1_df.count():,} records")
        
        source2_path = f"s3://{TARGET_BUCKET}/prepared_sources/source2_financial/"
        source2_df = spark.read.parquet(source2_path)
        logger.info(f"✓ Source 2 (financial): {source2_df.count():,} records")
        
        # Entity Resolution
        logger.info("\n=== ENTITY RESOLUTION ===")
        entity_resolver = EntityResolutionEngine(spark)
        
        source1_resolved = entity_resolver.resolve_duplicates(source1_df)
        source2_resolved = entity_resolver.resolve_duplicates(source2_df)
        
        # Combine both sources
        logger.info("\nCombining resolved sources...")
        combined_df = source1_resolved.union(source2_resolved)
        logger.info(f"✓ Combined entities: {combined_df.count():,}")
        
        # Data Harmonization
        logger.info("\n=== DATA HARMONIZATION ===")
        harmonizer = DataHarmonizer(spark)
        harmonized_df = harmonizer.harmonize_schema(combined_df)
        
        # Iceberg MERGE
        logger.info("\n=== ICEBERG MERGE INTO ===")
        merger = IcebergMerger(spark)
        merger.merge_into_iceberg(harmonized_df, "corporate_registry")
        
        # Generate summary report
        logger.info("\n=== SUMMARY REPORT ===")
        summary_sql = """
        SELECT 
            source_system,
            COUNT(*) as entity_count,
            SUM(CAST(revenue AS DECIMAL(20,2))) as total_revenue,
            SUM(CAST(profit AS DECIMAL(20,2))) as total_profit,
            COUNT(DISTINCT city) as cities,
            COUNT(DISTINCT state) as states
        FROM contexq_dev.corporate_registry
        GROUP BY source_system
        """
        
        summary_df = spark.sql(summary_sql)
        logger.info("Source System Summary:")
        summary_df.show(truncate=False)
        
        # Write summary to S3
        summary_path = f"s3://{TARGET_BUCKET}/etl_reports/summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}/"
        summary_df.write.mode("overwrite").parquet(summary_path)
        logger.info(f"✓ Summary written to {summary_path}")
        
        logger.info("ETL job completed successfully")
        logger.info("Corporate registry table ready for ML pipeline")
        
        job.commit()
        return 0
        
    except Exception as e:
        logger.error(f"✗ ETL job failed: {str(e)}", exc_info=True)
        job.commit()
        return 1


if __name__ == "__main__":
    sys.exit(main())
