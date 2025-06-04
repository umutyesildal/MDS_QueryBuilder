#!/usr/bin/env python3
"""
Validation Script for QueryBuilder Results
==========================================

This script validates the extracted data in the bronze.collection_disease table
and provides detailed analysis and quality checks.
"""

import logging
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import sys


class DataValidator:
    """Validates the extracted medical data in the bronze layer."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = None
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        """Setup logging for validation."""
        logger = logging.getLogger('DataValidator')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
        
    def connect(self):
        """Connect to database."""
        try:
            self.engine = create_engine(self.connection_string)
            return True
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
            
    def validate_data_quality(self):
        """Validate data quality and completeness."""
        self.logger.info("🔍 Starting Data Quality Validation...")
        
        try:
            with self.engine.connect() as conn:
                # 1. Basic Statistics
                stats_query = text("""
                    SELECT 
                        source_table,
                        COUNT(*) as total_records,
                        COUNT(DISTINCT subject_id) as unique_patients,
                        COUNT(DISTINCT itemid) as unique_items,
                        MIN(charttime) as earliest_time,
                        MAX(charttime) as latest_time
                    FROM bronze.collection_disease
                    GROUP BY source_table
                """)
                
                stats = conn.execute(stats_query).fetchall()
                
                self.logger.info("📊 Basic Statistics:")
                for row in stats:
                    self.logger.info(f"  {row[0]}:")
                    self.logger.info(f"    Records: {row[1]:,}")
                    self.logger.info(f"    Patients: {row[2]:,}")
                    self.logger.info(f"    Items: {row[3]:,}")
                    self.logger.info(f"    Time range: {row[4]} to {row[5]}")
                
                # 2. Data Quality Checks
                quality_query = text("""
                    SELECT 
                        source_table,
                        COUNT(CASE WHEN valuenum IS NULL THEN 1 END) as null_values,
                        COUNT(CASE WHEN valuenum < 0 THEN 1 END) as negative_values,
                        COUNT(CASE WHEN value IS NULL AND valuenum IS NULL THEN 1 END) as completely_null
                    FROM bronze.collection_disease
                    GROUP BY source_table
                """)
                
                quality = conn.execute(quality_query).fetchall()
                
                self.logger.info("🔍 Data Quality Checks:")
                for row in quality:
                    self.logger.info(f"  {row[0]}:")
                    self.logger.info(f"    Null numeric values: {row[1]:,}")
                    self.logger.info(f"    Negative values: {row[2]:,}")
                    self.logger.info(f"    Completely null: {row[3]:,}")
                
                # 3. Top Items by Frequency
                items_query = text("""
                    SELECT 
                        cd.itemid,
                        cd.source_table,
                        COALESCE(di.label, dl.label) as label,
                        COUNT(*) as frequency
                    FROM bronze.collection_disease cd
                    LEFT JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid AND cd.source_table = 'chartevents'
                    LEFT JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid AND cd.source_table = 'labevents'
                    GROUP BY cd.itemid, cd.source_table, COALESCE(di.label, dl.label)
                    ORDER BY frequency DESC
                    LIMIT 20
                """)
                
                items = conn.execute(items_query).fetchall()
                
                self.logger.info("🏆 Top 20 Items by Frequency:")
                for row in items:
                    self.logger.info(f"  {row[0]} ({row[1]}): {row[2]} - {row[3]:,} records")
                    
        except Exception as e:
            self.logger.error(f"Validation failed: {e}")
            
    def generate_summary_report(self):
        """Generate a comprehensive summary report."""
        self.logger.info("📋 Generating Summary Report...")
        
        try:
            with self.engine.connect() as conn:
                # Patient demographics
                patient_query = text("""
                    SELECT 
                        COUNT(DISTINCT cd.subject_id) as total_patients,
                        COUNT(DISTINCT cd.hadm_id) as total_admissions,
                        COUNT(DISTINCT cd.stay_id) as total_icu_stays
                    FROM bronze.collection_disease cd
                """)
                
                patient_stats = conn.execute(patient_query).fetchone()
                
                # Time range analysis
                time_query = text("""
                    SELECT 
                        MIN(charttime) as earliest_record,
                        MAX(charttime) as latest_record,
                        COUNT(DISTINCT DATE(charttime)) as unique_days
                    FROM bronze.collection_disease
                    WHERE charttime IS NOT NULL
                """)
                
                time_stats = conn.execute(time_query).fetchone()
                
                self.logger.info("📈 SUMMARY REPORT:")
                self.logger.info(f"  Total unique patients: {patient_stats[0]:,}")
                self.logger.info(f"  Total admissions: {patient_stats[1]:,}")
                self.logger.info(f"  Total ICU stays: {patient_stats[2]:,}")
                self.logger.info(f"  Data time range: {time_stats[0]} to {time_stats[1]}")
                self.logger.info(f"  Unique data days: {time_stats[2]:,}")
                
        except Exception as e:
            self.logger.error(f"Report generation failed: {e}")
            
    def check_missing_parameters(self):
        """Check if all expected parameters were found."""
        self.logger.info("🔎 Checking Parameter Coverage...")
        
        expected_chart_params = [
            'spo2', 'respiratory rate', 'heart rate', 'tidal volume', 'minute ventilation'
        ]
        
        expected_lab_params = [
            'ph', 'paco2', 'creatinine', 'albumin', 'd-dimer', 'procalcitonin'
        ]
        
        try:
            with self.engine.connect() as conn:
                # Check chart parameters
                chart_query = text("""
                    SELECT DISTINCT di.label
                    FROM bronze.collection_disease cd
                    JOIN mimiciv_icu.d_items di ON cd.itemid = di.itemid
                    WHERE cd.source_table = 'chartevents'
                """)
                
                found_chart = [row[0].lower() for row in conn.execute(chart_query).fetchall()]
                
                # Check lab parameters  
                lab_query = text("""
                    SELECT DISTINCT dl.label
                    FROM bronze.collection_disease cd
                    JOIN mimiciv_hosp.d_labitems dl ON cd.itemid = dl.itemid
                    WHERE cd.source_table = 'labevents'
                """)
                
                found_lab = [row[0].lower() for row in conn.execute(lab_query).fetchall()]
                
                self.logger.info("📋 Parameter Coverage Analysis:")
                self.logger.info(f"  Chart parameters found: {len(found_chart)}")
                self.logger.info(f"  Lab parameters found: {len(found_lab)}")
                
                # Check missing parameters
                for param in expected_chart_params:
                    found = any(param in label for label in found_chart)
                    status = "✅" if found else "❌"
                    self.logger.info(f"  {status} Chart parameter '{param}': {'Found' if found else 'Missing'}")
                    
                for param in expected_lab_params:
                    found = any(param in label for label in found_lab)
                    status = "✅" if found else "❌"
                    self.logger.info(f"  {status} Lab parameter '{param}': {'Found' if found else 'Missing'}")
                    
        except Exception as e:
            self.logger.error(f"Parameter check failed: {e}")


def main():
    """Main validation function."""
    connection_string = "postgresql://bernazehraural@localhost:5432/mimiciv"
    
    validator = DataValidator(connection_string)
    
    if not validator.connect():
        sys.exit(1)
        
    print("🔍 Starting Data Validation...")
    
    validator.validate_data_quality()
    validator.generate_summary_report() 
    validator.check_missing_parameters()
    
    print("\n✅ Validation completed successfully!")


if __name__ == "__main__":
    main()
