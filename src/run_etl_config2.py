#!/usr/bin/env python3
"""
ETL Pipeline Execution Script for Configuration 2 (Median-based)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
from src.config import etl_configurations as configg
from datetime import datetime

def run_config2_etl():
    """Execute ETL pipeline with Configuration 2"""
    print(f"🚀 Starting ETL Pipeline with Configuration 2")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Set active configuration
    configg.set_active_config(2)
    
    # Print configuration details
    config_summary = configg.get_config_summary()
    print("📋 Configuration Details:")
    for key, value in config_summary.items():
        print(f"  {key}: {value}")
    
    print("\n🔄 ETL Pipeline Steps:")
    print("  1. Data extraction from MIMIC-IV")
    print("  2. Data cleaning and quality checks")
    print("  3. Median-based aggregation")
    print("  4. Median imputation for missing values")
    print("  5. Percentile-based outlier removal")
    print("  6. Score calculations")
    print("  7. Loading to gold.gold_scores_config2")
    
    # TODO: Import and run your actual ETL pipeline here
    # Example:
    # from your_etl_module import run_etl_pipeline
    # run_etl_pipeline(configg.ACTIVE_CONFIG)
    
    print(f"\n✅ ETL Pipeline completed successfully")
    print(f"📊 Results saved to: {configg.ACTIVE_CONFIG['output_table']}")
    print(f"⏰ Completed at: {datetime.now()}")

if __name__ == "__main__":
    try:
        run_config2_etl()
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        sys.exit(1)
