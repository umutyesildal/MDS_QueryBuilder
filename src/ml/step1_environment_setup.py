#!/usr/bin/env python3
"""
Übung 5: ML Implementation - Step 1: Data Verification & Environment Setup
Medical Data Science - ICU Mortality Prediction using SOFA Scores

This script verifies the current data structure and sets up the environment
for 48-hour mortality prediction modeling.
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config_local import DB_CONFIG
from src.utils.file_paths import get_log_path, get_report_path
import psycopg2
from sqlalchemy import create_engine
import logging

class MLEnvironmentSetup:
    """Handle ML environment setup and data verification"""
    
    def __init__(self):
        self.setup_logging()
        self.engine = self.create_db_connection()
        
    def setup_logging(self):
        """Setup logging for ML pipeline"""
        log_path = get_log_path('ml_setup.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('MLSetup')
        
    def create_db_connection(self):
        """Create database connection"""
        try:
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            engine = create_engine(connection_string)
            self.logger.info("✅ Database connection established")
            return engine
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
            
    def verify_gold_data(self):
        """Verify gold layer data availability and structure"""
        self.logger.info("🔍 Verifying gold layer data...")
        
        # Check available tables
        query_tables = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'gold'
        ORDER BY table_name;
        """
        
        tables_df = pd.read_sql(query_tables, self.engine)
        self.logger.info(f"📊 Available gold tables: {tables_df['table_name'].tolist()}")
        
        # Check sofa_scores table structure
        if 'sofa_scores' in tables_df['table_name'].values:
            query_structure = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'gold' AND table_name = 'sofa_scores'
            ORDER BY ordinal_position;
            """
            
            structure_df = pd.read_sql(query_structure, self.engine)
            self.logger.info("📋 SOFA scores table structure:")
            for _, row in structure_df.iterrows():
                self.logger.info(f"  - {row['column_name']} ({row['data_type']})")
                
            # Check data volume
            query_count = "SELECT COUNT(*) as record_count FROM gold.sofa_scores"
            count_df = pd.read_sql(query_count, self.engine)
            record_count = count_df['record_count'].iloc[0]
            self.logger.info(f"📈 Total SOFA records: {record_count:,}")
            
            # Check unique patients and stays
            query_patients = """
            SELECT 
                COUNT(DISTINCT subject_id) as unique_patients,
                COUNT(DISTINCT stay_id) as unique_stays,
                MIN(charttime) as earliest_record,
                MAX(charttime) as latest_record
            FROM gold.sofa_scores
            """
            
            patients_df = pd.read_sql(query_patients, self.engine)
            self.logger.info(f"👥 Unique patients: {patients_df['unique_patients'].iloc[0]}")
            self.logger.info(f"🏥 Unique ICU stays: {patients_df['unique_stays'].iloc[0]}")
            self.logger.info(f"📅 Date range: {patients_df['earliest_record'].iloc[0]} to {patients_df['latest_record'].iloc[0]}")
            
            return structure_df, patients_df
        else:
            self.logger.error("❌ sofa_scores table not found in gold schema")
            raise ValueError("SOFA scores table not available")
            
    def check_mimic_mortality_data(self):
        """Check MIMIC-IV mortality data availability"""
        self.logger.info("🔍 Checking MIMIC-IV mortality data availability...")
        
        # Check if we can access admissions table
        try:
            query_mortality_check = """
            SELECT 
                COUNT(*) as total_admissions,
                SUM(hospital_expire_flag) as deaths,
                ROUND(AVG(hospital_expire_flag::numeric) * 100, 2) as mortality_rate_percent
            FROM mimiciv_hosp.admissions
            """
            
            mortality_df = pd.read_sql(query_mortality_check, self.engine)
            total = mortality_df['total_admissions'].iloc[0]
            deaths = mortality_df['deaths'].iloc[0]
            rate = mortality_df['mortality_rate_percent'].iloc[0]
            
            self.logger.info(f"🏥 Total admissions: {total:,}")
            self.logger.info(f"💀 Total deaths: {deaths}")
            self.logger.info(f"📊 Hospital mortality rate: {rate}%")
            
            return mortality_df
            
        except Exception as e:
            self.logger.error(f"❌ Cannot access MIMIC-IV mortality data: {e}")
            raise
            
    def sample_data_preview(self, limit=5):
        """Preview sample data from SOFA scores"""
        self.logger.info(f"👀 Previewing {limit} sample records...")
        
        query_sample = f"""
        SELECT *
        FROM gold.sofa_scores
        ORDER BY subject_id, charttime
        LIMIT {limit}
        """
        
        sample_df = pd.read_sql(query_sample, self.engine)
        self.logger.info("📋 Sample data preview:")
        for col in sample_df.columns:
            self.logger.info(f"  {col}: {sample_df[col].iloc[0] if not pd.isna(sample_df[col].iloc[0]) else 'NULL'}")
            
        return sample_df
        
    def generate_setup_report(self):
        """Generate comprehensive setup report"""
        report_path = get_report_path('ml_setup_report.md')
        
        with open(report_path, 'w') as f:
            f.write("# Übung 5 - ML Setup Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Environment Verification\n\n")
            f.write("✅ Database connection established\n")
            f.write("✅ Gold layer data verified\n")
            f.write("✅ MIMIC-IV mortality data accessible\n\n")
            f.write("## Next Steps\n\n")
            f.write("1. **Step 2:** Extract mortality data and merge with SOFA scores\n")
            f.write("2. **Step 3:** Perform exploratory data analysis\n")
            f.write("3. **Step 4:** Feature engineering and temporal splitting\n")
            f.write("4. **Step 5:** Baseline model implementation\n")
            f.write("5. **Step 6:** Advanced ML models with Darts\n")
            f.write("6. **Step 7:** Model evaluation and XAI\n")
            
        self.logger.info(f"📄 Setup report saved: {report_path}")

def main():
    """Main execution function"""
    print("🚀 Starting Übung 5 - ML Implementation Setup")
    print("=" * 60)
    
    try:
        # Initialize setup
        setup = MLEnvironmentSetup()
        
        # Verify data
        structure_df, patients_df = setup.verify_gold_data()
        mortality_df = setup.check_mimic_mortality_data()
        sample_df = setup.sample_data_preview()
        
        # Generate report
        setup.generate_setup_report()
        
        print("\n✅ Step 1 completed successfully!")
        print("📄 Check logs/ml_setup.log for detailed information")
        print("📋 Check docs/reports/ml_setup_report.md for summary")
        print("\n🔄 Ready to proceed to Step 2: Mortality Data Extraction")
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return 1
        
    return 0

if __name__ == "__main__":
    exit(main())
