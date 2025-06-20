#!/usr/bin/env python3
"""
ETL Pipeline Execution Script for Configuration 1 (Mean-based)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
from src.config import etl_configurations as configg
from datetime import datetime

def run_config1_etl():
    """Execute ETL pipeline with Configuration 1"""
    print(f"🚀 Starting ETL Pipeline with Configuration 1")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Set active configuration
    configg.set_active_config(1)
    
    # Print configuration details
    config_summary = configg.get_config_summary()
    print("📋 Configuration Details:")
    for key, value in config_summary.items():
        print(f"  {key}: {value}")
    
    print("\n🔄 ETL Pipeline Steps:")
    print("  1. Data extraction from MIMIC-IV")
    print("  2. Data cleaning and quality checks")
    print("  3. Mean-based aggregation")
    print("  4. Mean imputation for missing values")
    print("  5. IQR-based outlier removal")
    print("  6. Score calculations")
    print("  7. Loading to gold.gold_scores_config1")
    
    # Run the actual ETL pipeline
    from src.etl.gold_etl_pipeline import GoldETLPipeline
    
    try:
        # Get current active configuration
        current_config = configg.ACTIVE_CONFIG
        
        # Run ETL pipeline with this configuration
        pipeline = GoldETLPipeline(current_config)
        pipeline.run_pipeline()
        
        print(f"\n✅ ETL Pipeline completed successfully")
        print(f"📊 Results saved to: {current_config['output_table']}")
        print(f"⏰ Completed at: {datetime.now()}")
        
    except Exception as e:
        print(f"❌ ETL Pipeline execution failed: {e}")
        raise e

if __name__ == "__main__":
    try:
        run_config1_etl()
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        sys.exit(1)
