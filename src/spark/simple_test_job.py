"""
Simple test job to verify Glue basics work
"""
import sys
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("✓ Simple test job started")

try:
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    
    logger.info("✓ Imports successful")
    
    # Get parameters
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_bucket', 'target_bucket'])
    logger.info(f"✓ Parameters retrieved: source={args['source_bucket']}, target={args['target_bucket']}")
    
    # Initialize Spark
    sc = SparkContext()
    logger.info("✓ SparkContext initialized")
    
    glueContext = GlueContext(sc)
    logger.info("✓ GlueContext initialized")
    
    spark = glueContext.spark_session
    logger.info("✓ Spark session ready")
    
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger.info(f"✓ Job initialized: {args['JOB_NAME']}")
    
    # Try to list buckets
    logger.info("✓ Testing S3 connectivity...")
    source_bucket = args['source_bucket']
    
    # Read CSV file
    csv_path = f"s3://{source_bucket}/source_supply/olist_sellers_dataset.csv"
    logger.info(f"Attempting to read: {csv_path}")
    
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    logger.info(f"✓ CSV read successful: {df.count():,} records")
    logger.info(f"✓ Columns: {df.columns}")
    
    job.commit()
    logger.info("✓ Job committed successfully")
    sys.exit(0)
    
except Exception as e:
    logger.error(f"✗ Error: {type(e).__name__}: {str(e)}", exc_info=True)
    sys.exit(1)
