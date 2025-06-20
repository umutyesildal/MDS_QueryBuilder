#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL Pipeline Execution Script for Configuration 1 (Mean-based)
"""

import configg
import sys
from datetime import datetime

def run_config1_etl():
    """Execute ETL pipeline with Configuration 1"""
    print("Starting ETL Pipeline with Configuration 1")
    print("Started at: {}".format(datetime.now()))
    print("=" * 60)
    
    # Set active configuration
    configg.set_active_config(1)
    
    # Print configuration details
    config_summary = configg.get_config_summary()
    print("Configuration Details:")
    for key, value in config_summary.items():
        print("  {}: {}".format(key, value))
    
    print("\nETL Pipeline Steps:")
    print("  1. Data extraction from Silver layer")
    print("  2. Data cleaning and quality checks")
    print("  3. Mean-based aggregation")
    print("  4. Mean imputation for missing values")
    print("  5. IQR-based outlier removal")
    print("  6. Score calculations")
    print("  7. Loading to gold.gold_scores_config1")
    
    # Import and run the actual ETL pipeline
    from gold_etl_pipeline_simple import run_etl_pipeline
    success = run_etl_pipeline(configg.ACTIVE_CONFIG)
    
    if success:
        print("\nETL Pipeline completed successfully")
        print("Results saved to: {}".format(configg.ACTIVE_CONFIG['output_table']))
        print("Completed at: {}".format(datetime.now()))
        return True
    else:
        print("\nETL Pipeline failed")
        return False

if __name__ == "__main__":
    try:
        success = run_config1_etl()
        if not success:
            sys.exit(1)
    except Exception as e:
        print("ETL Pipeline failed: {}".format(e))
        sys.exit(1)
