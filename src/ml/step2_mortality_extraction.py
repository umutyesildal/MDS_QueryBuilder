#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uebung 5: ML Implementation - Step 2: Mortality Data Extraction & Integration
Medical Data Science - ICU Mortality Prediction using SOFA Scores

This script extracts 48-hour mortality data from MIMIC-IV and integrates it
with existing SOFA scores for predictive modeling.
"""

import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config_local import DB_CONFIG
from src.utils.file_paths import get_log_path, get_report_path
from sqlalchemy import create_engine
import logging

class MortalityDataExtractor:
    """Extract and integrate mortality data with SOFA scores"""
    
    def __init__(self):
        self.setup_logging()
        self.engine = self.create_db_connection()
        
    def setup_logging(self):
        """Setup logging for mortality extraction"""
        log_path = get_log_path('ml_mortality_extraction.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('MortalityExtractor')
        
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
            
    def extract_icu_mortality_data(self):
        """Extract ICU-level mortality information"""
        self.logger.info("🔍 Extracting ICU mortality data...")
        
        query_icu_mortality = """
        SELECT 
            icu.stay_id,
            icu.subject_id,
            icu.hadm_id,
            icu.intime as icu_intime,
            icu.outtime as icu_outtime,
            adm.hospital_expire_flag,
            adm.deathtime,
            CASE 
                WHEN adm.deathtime IS NOT NULL 
                     AND adm.deathtime BETWEEN icu.intime AND icu.outtime + INTERVAL '48 hours'
                THEN 1
                ELSE 0
            END as mortality_48h
        FROM mimiciv_icu.icustays icu
        JOIN mimiciv_hosp.admissions adm ON icu.hadm_id = adm.hadm_id
        ORDER BY icu.subject_id, icu.intime
        """
        
        mortality_df = pd.read_sql(query_icu_mortality, self.engine)
        
        self.logger.info(f"📊 Total ICU stays: {len(mortality_df):,}")
        self.logger.info(f"💀 48-hour mortality cases: {mortality_df['mortality_48h'].sum()}")
        self.logger.info(f"📈 48-hour mortality rate: {mortality_df['mortality_48h'].mean()*100:.2f}%")
        
        return mortality_df
        
    def create_time_aware_dataset(self):
        """Create dataset with 48-hour prediction windows"""
        self.logger.info("⏰ Creating time-aware dataset with 48-hour prediction windows...")
        
        query_time_aware = """
        WITH sofa_with_mortality AS (
            SELECT 
                s.*,
                m.mortality_48h,
                m.icu_intime,
                m.icu_outtime,
                m.deathtime,
                -- Calculate time from ICU admission
                EXTRACT(EPOCH FROM (s.charttime - m.icu_intime))/3600.0 as hours_from_admission,
                -- Create prediction timepoint (48 hours after current measurement)
                s.charttime + INTERVAL '48 hours' as prediction_timepoint,
                -- Check if death occurs within 48 hours of this measurement
                CASE 
                    WHEN m.deathtime IS NOT NULL 
                         AND m.deathtime <= s.charttime + INTERVAL '48 hours'
                         AND m.deathtime > s.charttime
                    THEN 1
                    ELSE 0
                END as target_mortality_48h
            FROM gold.sofa_scores s
            JOIN (
                SELECT 
                    icu.stay_id,
                    icu.subject_id,
                    icu.hadm_id,
                    icu.intime as icu_intime,
                    icu.outtime as icu_outtime,
                    adm.deathtime,
                    CASE 
                        WHEN adm.deathtime IS NOT NULL 
                             AND adm.deathtime BETWEEN icu.intime AND icu.outtime + INTERVAL '48 hours'
                        THEN 1
                        ELSE 0
                    END as mortality_48h
                FROM mimiciv_icu.icustays icu
                JOIN mimiciv_hosp.admissions adm ON icu.hadm_id = adm.hadm_id
            ) m ON s.stay_id = m.stay_id
            -- Only include measurements that allow for 48-hour follow-up
            WHERE s.charttime <= m.icu_outtime - INTERVAL '48 hours'
               OR (m.deathtime IS NOT NULL AND s.charttime <= m.deathtime - INTERVAL '2 hours')
        )
        SELECT *
        FROM sofa_with_mortality
        WHERE hours_from_admission >= 0  -- Only measurements after ICU admission
        ORDER BY subject_id, stay_id, charttime
        """
        
        time_aware_df = pd.read_sql(query_time_aware, self.engine)
        
        self.logger.info(f"📊 Time-aware dataset size: {len(time_aware_df):,} records")
        self.logger.info(f"👥 Unique patients: {time_aware_df['subject_id'].nunique()}")
        self.logger.info(f"🏥 Unique stays: {time_aware_df['stay_id'].nunique()}")
        self.logger.info(f"💀 48h mortality events: {time_aware_df['target_mortality_48h'].sum()}")
        self.logger.info(f"📈 48h mortality rate: {time_aware_df['target_mortality_48h'].mean()*100:.2f}%")
        
        return time_aware_df
        
    def perform_data_quality_checks(self, df):
        """Perform comprehensive data quality checks"""
        self.logger.info("🔍 Performing data quality checks...")
        
        # Check for missing values
        missing_stats = df.isnull().sum()
        self.logger.info("📋 Missing values per column:")
        for col, missing_count in missing_stats.items():
            if missing_count > 0:
                missing_pct = (missing_count / len(df)) * 100
                self.logger.info(f"  {col}: {missing_count} ({missing_pct:.1f}%)")
                
        # Check SOFA score distributions
        sofa_cols = ['respiratory_score', 'cardiovascular_score', 'hepatic_score', 
                     'coagulation_score', 'renal_score', 'neurological_score', 'total_sofa_score']
        
        self.logger.info("📊 SOFA score distributions:")
        for col in sofa_cols:
            if col in df.columns:
                stats = df[col].describe()
                self.logger.info(f"  {col}: mean={stats['mean']:.2f}, std={stats['std']:.2f}, min={stats['min']}, max={stats['max']}")
                
        # Check temporal distribution
        if 'hours_from_admission' in df.columns:
            hours_stats = df['hours_from_admission'].describe()
            self.logger.info(f"⏰ Hours from admission: mean={hours_stats['mean']:.1f}, max={hours_stats['max']:.1f}")
            
        return missing_stats
        
    def save_processed_dataset(self, df, filename='ml_dataset_48h_mortality'):
        """Save processed dataset for ML pipeline"""
        self.logger.info(f"💾 Saving processed dataset...")
        
        # Save to CSV for ML processing
        csv_path = os.path.join(project_root, 'data', f'{filename}.csv')
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path, index=False)
        
        # Prepare dataframe for database storage (handle JSON columns)
        df_for_db = df.copy()
        if 'vasopressor_doses' in df_for_db.columns:
            # Convert dict/JSON columns to JSON strings for database storage
            df_for_db['vasopressor_doses'] = df_for_db['vasopressor_doses'].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )
        
        # Save to database table for future use
        table_name = 'ml_dataset_48h_mortality'
        df_for_db.to_sql(table_name, self.engine, schema='gold', if_exists='replace', index=False)
        
        self.logger.info(f"✅ Dataset saved to: {csv_path}")
        self.logger.info(f"✅ Dataset saved to database: gold.{table_name}")
        
        return csv_path
        
    def generate_extraction_report(self, df, missing_stats):
        """Generate comprehensive extraction report"""
        report_path = get_report_path('ml_mortality_extraction_report.md')
        
        with open(report_path, 'w') as f:
            f.write("# Übung 5 - Mortality Data Extraction Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Dataset Overview\n\n")
            f.write(f"- **Total Records:** {len(df):,}\n")
            f.write(f"- **Unique Patients:** {df['subject_id'].nunique()}\n")
            f.write(f"- **Unique ICU Stays:** {df['stay_id'].nunique()}\n")
            f.write(f"- **48h Mortality Events:** {df['target_mortality_48h'].sum()}\n")
            f.write(f"- **48h Mortality Rate:** {df['target_mortality_48h'].mean()*100:.2f}%\n\n")
            
            f.write("## Target Variable Distribution\n\n")
            target_counts = df['target_mortality_48h'].value_counts()
            f.write(f"- **Survivors (0):** {target_counts.get(0, 0):,} ({target_counts.get(0, 0)/len(df)*100:.1f}%)\n")
            f.write(f"- **48h Mortality (1):** {target_counts.get(1, 0):,} ({target_counts.get(1, 0)/len(df)*100:.1f}%)\n\n")
            
            f.write("## Data Quality Assessment\n\n")
            f.write("### Missing Values\n")
            for col, missing_count in missing_stats.items():
                if missing_count > 0:
                    missing_pct = (missing_count / len(df)) * 100
                    f.write(f"- **{col}:** {missing_count} ({missing_pct:.1f}%)\n")
            
            f.write("\n## Next Steps\n\n")
            f.write("1. **Step 3:** Exploratory Data Analysis (EDA)\n")
            f.write("2. **Step 4:** Feature Engineering & Temporal Splitting\n")
            f.write("3. **Step 5:** Handle Class Imbalance\n")
            f.write("4. **Step 6:** Baseline Model Implementation\n")
            
        self.logger.info(f"📄 Extraction report saved: {report_path}")

def main():
    """Main execution function"""
    print("🚀 Starting Step 2: Mortality Data Extraction")
    print("=" * 60)
    
    try:
        # Initialize extractor
        extractor = MortalityDataExtractor()
        
        # Extract mortality data
        mortality_df = extractor.extract_icu_mortality_data()
        
        # Create time-aware dataset
        time_aware_df = extractor.create_time_aware_dataset()
        
        # Perform quality checks
        missing_stats = extractor.perform_data_quality_checks(time_aware_df)
        
        # Save processed dataset
        csv_path = extractor.save_processed_dataset(time_aware_df)
        
        # Generate report
        extractor.generate_extraction_report(time_aware_df, missing_stats)
        
        print("\n✅ Step 2 completed successfully!")
        print(f"📄 Dataset saved: {csv_path}")
        print("📋 Check docs/reports/ml_mortality_extraction_report.md for details")
        print("\n🔄 Ready to proceed to Step 3: Exploratory Data Analysis")
        
        return 0
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
