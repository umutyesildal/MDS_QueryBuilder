#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration Comparison Analysis Script for Task 5.4
"""

import configg
import sys
from datetime import datetime

def run_comparison_analysis():
    """Execute comparison analysis between two configurations"""
    print(f"📊 Starting Configuration Comparison Analysis")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Get both configurations
    configs = configg.get_both_configs()
    comparison_tables = configg.get_comparison_tables()
    
    print("🔍 Analysis Configuration:")
    print(f"  Table 1: {comparison_tables['table_1']}")
    print(f"  Table 2: {comparison_tables['table_2']}")
    
    print("\n📈 Analysis Steps:")
    print("  1. Load data from both configuration tables")
    print("  2. Calculate statistical measures (correlation, MAD)")
    print("  3. Generate comparative visualizations")
    print("  4. Perform subgroup analysis")
    print("  5. Analyze mortality correlations")
    print("  6. Generate Bland-Altman plots")
    print("  7. Save results to gold.config_comparison_analysis")
    
    # Import and run the actual comparison analysis
    from comparison_analysis import run_comparison
    success = run_comparison(comparison_tables, configg.COMPARISON_CONFIG)
    
    if success:
        print(f"\n✅ Comparison Analysis completed successfully")
        print(f"⏰ Completed at: {datetime.now()}")
        return True
    else:
        print(f"\n❌ Comparison Analysis failed")
        return False

if __name__ == "__main__":
    try:
        success = run_comparison_analysis()
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"❌ Comparison Analysis failed: {e}")
        sys.exit(1)
        sys.exit(1)
