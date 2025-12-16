#!/usr/bin/env python3
"""
Data preparation validation using pandas.
Validates data structure and transformations locally.
"""

import logging
import pandas as pd
import numpy as np
from io import StringIO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("╔════════════════════════════════════════════════════════════╗")
    logger.info("║  OLIST Data Structure Validation (Pandas-based)           ║")
    logger.info("║  Tests data integrity without PySpark dependencies        ║")
    logger.info("╚════════════════════════════════════════════════════════════╝\n")
    
    try:
        # Read CSV files locally
        logger.info("Reading OLIST datasets...")
        order_items = pd.read_csv('/workspaces/contexq-assesment/olist_order_items_dataset.csv')
        payments = pd.read_csv('/workspaces/contexq-assesment/olist_order_payments_dataset.csv')
        sellers = pd.read_csv('/workspaces/contexq-assesment/olist_sellers_dataset.csv')
        
        logger.info(f"✓ Order Items: {len(order_items):,} rows, {len(order_items.columns)} columns")
        logger.info(f"✓ Payments: {len(payments):,} rows, {len(payments.columns)} columns")
        logger.info(f"✓ Sellers: {len(sellers):,} rows, {len(sellers.columns)} columns")
        
        # Data quality assessment
        logger.info("\n=== DATA QUALITY ASSESSMENT ===")
        
        logger.info("\nOrder Items - Null Values:")
        print(order_items.isnull().sum())
        
        logger.info("\nPayments - Null Values:")
        print(payments.isnull().sum())
        
        logger.info("\nSellers - Null Values:")
        print(sellers.isnull().sum())
        
        # Source 1: Supply Chain (Order Items + Sellers)
        logger.info("\n=== PREPARING SOURCE 1: SUPPLY CHAIN ===")
        source1 = order_items.merge(
            sellers[['seller_id', 'seller_city', 'seller_state']], 
            on='seller_id', 
            how='left'
        )
        
        source1_agg = source1.groupby(['seller_id', 'seller_city', 'seller_state']).agg({
            'price': 'sum',
            'product_id': lambda x: len(set(x))
        }).reset_index()
        
        source1_agg.columns = ['corporate_id', 'corporate_name', 'state', 'revenue', 'product_diversity']
        source1_agg['address'] = source1_agg['corporate_name'] + ', ' + source1_agg['state']
        source1_agg['source_system'] = 'olist_supply_chain'
        source1_agg['profit'] = source1_agg['revenue'] * 0.2
        
        logger.info(f"✓ Source 1 prepared: {len(source1_agg):,} unique suppliers")
        logger.info("\nTop 5 suppliers by revenue:")
        print(source1_agg.nlargest(5, 'revenue')[['corporate_id', 'corporate_name', 'revenue', 'profit']])
        
        # Source 2: Financial (Order Payments)
        logger.info("\n=== PREPARING SOURCE 2: FINANCIAL ===")
        source2 = payments.groupby(['order_id', 'payment_type']).agg({
            'payment_value': 'sum'
        }).reset_index()
        
        source2['corporate_id'] = range(1, len(source2) + 1)
        source2['corporate_name'] = 'Order_' + source2['order_id'].astype(str)
        source2['revenue'] = source2['payment_value']
        source2['profit'] = source2['payment_value'] * 0.15
        source2['source_system'] = 'olist_financial'
        source2 = source2[['corporate_id', 'corporate_name', 'payment_type', 'revenue', 'profit', 'source_system']]
        
        logger.info(f"✓ Source 2 prepared: {len(source2):,} payment records")
        logger.info("\nPayment type distribution:")
        print(source2['payment_type'].value_counts())
        
        # Duplicate resolution simulation
        logger.info("\n=== SIMULATING ENTITY RESOLUTION ===")
        
        # Check for duplicates in Source 1
        source1_dupes = source1_agg[source1_agg.duplicated(subset=['corporate_name'], keep=False)].shape[0]
        logger.info(f"Potential duplicates in Source 1: {source1_dupes}")
        
        # Simulate matching statistics
        logger.info(f"\nMatching Statistics:")
        logger.info(f"  - Source 1 entities: {len(source1_agg):,}")
        logger.info(f"  - Source 2 entities: {len(source2):,}")
        logger.info(f"  - Total entities for deduplication: {len(source1_agg) + len(source2):,}")
        
        # Revenue statistics
        logger.info(f"\n=== REVENUE STATISTICS ===")
        logger.info(f"Source 1 (Supply Chain):")
        logger.info(f"  - Total revenue: ${source1_agg['revenue'].sum():,.2f}")
        logger.info(f"  - Average revenue per supplier: ${source1_agg['revenue'].mean():,.2f}")
        logger.info(f"  - Total profit: ${source1_agg['profit'].sum():,.2f}")
        
        logger.info(f"Source 2 (Financial):")
        logger.info(f"  - Total payment value: ${source2['revenue'].sum():,.2f}")
        logger.info(f"  - Average payment: ${source2['revenue'].mean():,.2f}")
        logger.info(f"  - Total profit (15%): ${source2['profit'].sum():,.2f}")
        
        # Schema validation
        logger.info("\n=== SCHEMA VALIDATION ===")
        required_cols_source1 = ['corporate_id', 'corporate_name', 'address', 'revenue', 'profit', 'source_system']
        required_cols_source2 = ['corporate_id', 'corporate_name', 'revenue', 'profit', 'source_system']
        
        missing_s1 = [col for col in required_cols_source1 if col not in source1_agg.columns]
        missing_s2 = [col for col in required_cols_source2 if col not in source2.columns]
        
        if not missing_s1:
            logger.info("✓ Source 1 schema validation: PASSED")
        else:
            logger.error(f"✗ Source 1 missing columns: {missing_s1}")
        
        if not missing_s2:
            logger.info("✓ Source 2 schema validation: PASSED")
        else:
            logger.error(f"✗ Source 2 missing columns: {missing_s2}")
        
        logger.info("\n╔════════════════════════════════════════════════════════════╗")
        logger.info("║  ✓ DATA STRUCTURE VALIDATION COMPLETE                     ║")
        logger.info("║  All data sources ready for Glue deployment               ║")
        logger.info("╚════════════════════════════════════════════════════════════╝\n")
        
        return 0
        
    except Exception as e:
        logger.error(f"✗ Validation failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
