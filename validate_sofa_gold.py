#!/usr/bin/env python3
"""
Gold Layer Validation Script for SOFA Scores
============================================

This script validates the Gold layer SOFA score calculations and generates
comprehensive quality reports for the calculated SOFA scores.

Features:
- SOFA score range validation (0-24 total, 0-4 subscores)
- Imputation rate analysis
- Data completeness assessment
- Clinical consistency checks
- Comparative analysis (ARI vs non-ARI)

Author: Medical Data Science Team
Date: 2025-06-04
"""

import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine, text
import warnings
warnings.filterwarnings('ignore')

# Import configurations
from config_gold import *

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Configure logging for validation"""
    logger = logging.getLogger('gold_validation')
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

def create_db_connection():
    """Create database connection"""
    try:
        if DB_CONFIG.get('password'):
            conn_string = (
                f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
        else:
            conn_string = (
                f"postgresql://{DB_CONFIG['user']}"
                f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
        
        engine = create_engine(conn_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        return engine
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

class GoldLayerValidator:
    """Validates Gold layer SOFA calculations"""
    
    def __init__(self, engine, logger):
        self.engine = engine
        self.logger = logger
        self.validation_results = {}
    
    def run_all_validations(self):
        """Execute all validation checks"""
        self.logger.info("🔍 Starting Gold Layer SOFA validation...")
        
        try:
            # Basic existence and structure checks
            self.validate_table_existence()
            self.validate_basic_statistics()
            
            # SOFA score validations
            self.validate_sofa_score_ranges()
            self.validate_subscore_consistency()
            
            # Data quality validations
            self.validate_imputation_rates()
            self.validate_missing_data_patterns()
            self.validate_temporal_consistency()
            
            # Clinical validations
            self.validate_ari_classification()
            self.validate_clinical_distributions()
            
            # Generate comprehensive report
            self.generate_validation_report()
            
            self.logger.info("✅ Gold Layer validation completed successfully!")
            
        except Exception as e:
            self.logger.error(f"❌ Validation failed: {e}")
            raise
    
    def validate_table_existence(self):
        """Check if Gold table exists and has expected structure"""
        self.logger.info("🏗️ Validating table existence and structure...")
        
        # Check table existence
        query = f"""
        SELECT COUNT(*) as record_count
        FROM {GOLD_FULL_TABLE_NAME}
        """
        
        try:
            result = pd.read_sql_query(text(query), self.engine)
            record_count = result.iloc[0]['record_count']
            
            self.validation_results['table_exists'] = True
            self.validation_results['total_records'] = record_count
            
            self.logger.info(f"✅ Table exists with {record_count:,} records")
            
            if record_count == 0:
                self.logger.warning("⚠️ Gold table is empty!")
                
        except Exception as e:
            self.validation_results['table_exists'] = False
            self.logger.error(f"❌ Table validation failed: {e}")
            raise
    
    def validate_basic_statistics(self):
        """Validate basic statistics and coverage"""
        self.logger.info("📊 Validating basic statistics...")
        
        query = f"""
        SELECT 
            COUNT(*) as total_windows,
            COUNT(DISTINCT subject_id) as unique_patients,
            COUNT(DISTINCT stay_id) as unique_stays,
            COUNT(DISTINCT hadm_id) as unique_admissions,
            MIN(window_start) as earliest_window,
            MAX(window_end) as latest_window,
            AVG(icu_day) as avg_icu_day,
            MAX(icu_day) as max_icu_day
        FROM {GOLD_FULL_TABLE_NAME}
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        stats = result.iloc[0].to_dict()
        
        self.validation_results['basic_statistics'] = stats
        
        self.logger.info(f"📈 Total Windows: {stats['total_windows']:,}")
        self.logger.info(f"👥 Unique Patients: {stats['unique_patients']:,}")
        self.logger.info(f"🏥 Unique ICU Stays: {stats['unique_stays']:,}")
        self.logger.info(f"📅 Date Range: {stats['earliest_window']} to {stats['latest_window']}")
        self.logger.info(f"⏰ Average ICU Day: {stats['avg_icu_day']:.1f}, Max: {stats['max_icu_day']}")
    
    def validate_sofa_score_ranges(self):
        """Validate SOFA scores are within expected ranges"""
        self.logger.info("🎯 Validating SOFA score ranges...")
        
        query = f"""
        SELECT 
            -- Total SOFA validation
            COUNT(CASE WHEN sofa_total < 0 OR sofa_total > 24 THEN 1 END) as invalid_total_sofa,
            MIN(sofa_total) as min_total_sofa,
            MAX(sofa_total) as max_total_sofa,
            AVG(sofa_total) as avg_total_sofa,
            
            -- Subscore validations
            COUNT(CASE WHEN sofa_respiratory_subscore < 0 OR sofa_respiratory_subscore > 4 THEN 1 END) as invalid_resp,
            COUNT(CASE WHEN sofa_cardiovascular_subscore < 0 OR sofa_cardiovascular_subscore > 4 THEN 1 END) as invalid_cardio,
            COUNT(CASE WHEN sofa_coagulation_subscore < 0 OR sofa_coagulation_subscore > 4 THEN 1 END) as invalid_coag,
            COUNT(CASE WHEN sofa_liver_subscore < 0 OR sofa_liver_subscore > 4 THEN 1 END) as invalid_liver,
            COUNT(CASE WHEN sofa_cns_subscore < 0 OR sofa_cns_subscore > 4 THEN 1 END) as invalid_cns,
            COUNT(CASE WHEN sofa_renal_subscore < 0 OR sofa_renal_subscore > 4 THEN 1 END) as invalid_renal,
            
            COUNT(*) as total_records
        FROM {GOLD_FULL_TABLE_NAME}
        WHERE sofa_total IS NOT NULL
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        validation = result.iloc[0].to_dict()
        
        self.validation_results['sofa_ranges'] = validation
        
        # Check for invalid scores
        total_invalid = (validation['invalid_total_sofa'] + validation['invalid_resp'] + 
                        validation['invalid_cardio'] + validation['invalid_coag'] + 
                        validation['invalid_liver'] + validation['invalid_cns'] + 
                        validation['invalid_renal'])
        
        if total_invalid == 0:
            self.logger.info("✅ All SOFA scores within valid ranges")
        else:
            self.logger.warning(f"⚠️ Found {total_invalid} invalid SOFA scores")
        
        self.logger.info(f"📊 SOFA Total Range: {validation['min_total_sofa']}-{validation['max_total_sofa']} (avg: {validation['avg_total_sofa']:.1f})")
    
    def validate_subscore_consistency(self):
        """Validate that total SOFA equals sum of subscores"""
        self.logger.info("🔍 Validating subscore consistency...")
        
        query = f"""
        SELECT 
            COUNT(CASE WHEN 
                sofa_total != COALESCE(sofa_respiratory_subscore, 0) + 
                             COALESCE(sofa_cardiovascular_subscore, 0) + 
                             COALESCE(sofa_coagulation_subscore, 0) + 
                             COALESCE(sofa_liver_subscore, 0) + 
                             COALESCE(sofa_cns_subscore, 0) + 
                             COALESCE(sofa_renal_subscore, 0)
            THEN 1 END) as inconsistent_totals,
            COUNT(*) as total_records
        FROM {GOLD_FULL_TABLE_NAME}
        WHERE sofa_total IS NOT NULL
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        inconsistent = result.iloc[0]['inconsistent_totals']
        total = result.iloc[0]['total_records']
        
        self.validation_results['subscore_consistency'] = {
            'inconsistent_records': inconsistent,
            'total_records': total,
            'consistency_rate': (total - inconsistent) / total * 100 if total > 0 else 0
        }
        
        if inconsistent == 0:
            self.logger.info("✅ All SOFA totals consistent with subscores")
        else:
            self.logger.warning(f"⚠️ {inconsistent}/{total} records have inconsistent SOFA totals")
    
    def validate_imputation_rates(self):
        """Analyze imputation rates for all parameters"""
        self.logger.info("🔄 Analyzing imputation rates...")
        
        query = f"""
        SELECT 
            AVG(CASE WHEN pao2_fio2_imputed THEN 1.0 ELSE 0.0 END) * 100 as pao2_imputation_rate,
            AVG(CASE WHEN spo2_fio2_surrogate THEN 1.0 ELSE 0.0 END) * 100 as spo2_surrogate_rate,
            AVG(CASE WHEN map_imputed THEN 1.0 ELSE 0.0 END) * 100 as map_imputation_rate,
            AVG(CASE WHEN platelets_imputed THEN 1.0 ELSE 0.0 END) * 100 as platelets_imputation_rate,
            AVG(CASE WHEN bilirubin_imputed THEN 1.0 ELSE 0.0 END) * 100 as bilirubin_imputation_rate,
            AVG(CASE WHEN gcs_imputed THEN 1.0 ELSE 0.0 END) * 100 as gcs_imputation_rate,
            AVG(CASE WHEN creatinine_imputed THEN 1.0 ELSE 0.0 END) * 100 as creatinine_imputation_rate,
            AVG(CASE WHEN urine_output_imputed THEN 1.0 ELSE 0.0 END) * 100 as urine_imputation_rate,
            COUNT(*) as total_records
        FROM {GOLD_FULL_TABLE_NAME}
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        rates = result.iloc[0].to_dict()
        
        self.validation_results['imputation_rates'] = rates
        
        self.logger.info("📈 Imputation Rates:")
        self.logger.info(f"  PaO2/FiO2: {rates['pao2_imputation_rate']:.1f}%")
        self.logger.info(f"  SpO2 Surrogate: {rates['spo2_surrogate_rate']:.1f}%")
        self.logger.info(f"  MAP: {rates['map_imputation_rate']:.1f}%")
        self.logger.info(f"  Platelets: {rates['platelets_imputation_rate']:.1f}%")
        self.logger.info(f"  Bilirubin: {rates['bilirubin_imputation_rate']:.1f}%")
        self.logger.info(f"  GCS: {rates['gcs_imputation_rate']:.1f}%")
        self.logger.info(f"  Creatinine: {rates['creatinine_imputation_rate']:.1f}%")
        self.logger.info(f"  Urine Output: {rates['urine_imputation_rate']:.1f}%")
    
    def validate_missing_data_patterns(self):
        """Analyze missing data patterns"""
        self.logger.info("📉 Analyzing missing data patterns...")
        
        query = f"""
        SELECT 
            array_length(missing_components, 1) as missing_count,
            COUNT(*) as frequency
        FROM {GOLD_FULL_TABLE_NAME}
        WHERE missing_components IS NOT NULL
        GROUP BY array_length(missing_components, 1)
        ORDER BY missing_count
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        
        self.validation_results['missing_patterns'] = result.to_dict('records')
        
        self.logger.info("📊 Missing Components Distribution:")
        for _, row in result.iterrows():
            missing_count = row['missing_count'] or 0
            frequency = row['frequency']
            percentage = frequency / result['frequency'].sum() * 100
            self.logger.info(f"  {missing_count}/6 missing: {frequency:,} windows ({percentage:.1f}%)")
    
    def validate_temporal_consistency(self):
        """Validate temporal consistency of windows"""
        self.logger.info("⏰ Validating temporal consistency...")
        
        query = f"""
        SELECT 
            COUNT(CASE WHEN window_start >= window_end THEN 1 END) as invalid_windows,
            COUNT(CASE WHEN icu_day != window_number THEN 1 END) as day_number_mismatch,
            COUNT(*) as total_windows
        FROM {GOLD_FULL_TABLE_NAME}
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        temporal = result.iloc[0].to_dict()
        
        self.validation_results['temporal_consistency'] = temporal
        
        if temporal['invalid_windows'] == 0 and temporal['day_number_mismatch'] == 0:
            self.logger.info("✅ All windows have consistent temporal properties")
        else:
            if temporal['invalid_windows'] > 0:
                self.logger.warning(f"⚠️ {temporal['invalid_windows']} windows have start >= end time")
            if temporal['day_number_mismatch'] > 0:
                self.logger.warning(f"⚠️ {temporal['day_number_mismatch']} windows have ICU day != window number")
    
    def validate_ari_classification(self):
        """Validate ARI patient classification"""
        self.logger.info("🏥 Validating ARI classification...")
        
        query = f"""
        SELECT 
            disease_type,
            COUNT(*) as window_count,
            COUNT(DISTINCT subject_id) as patient_count,
            AVG(sofa_total) as avg_sofa_score
        FROM {GOLD_FULL_TABLE_NAME}
        WHERE sofa_total IS NOT NULL
        GROUP BY disease_type
        ORDER BY disease_type
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        
        self.validation_results['ari_classification'] = result.to_dict('records')
        
        self.logger.info("🔍 Disease Type Distribution:")
        for _, row in result.iterrows():
            self.logger.info(f"  {row['disease_type']}: {row['patient_count']:,} patients, "
                           f"{row['window_count']:,} windows, avg SOFA: {row['avg_sofa_score']:.1f}")
    
    def validate_clinical_distributions(self):
        """Validate clinical parameter distributions"""
        self.logger.info("📊 Validating clinical parameter distributions...")
        
        query = f"""
        SELECT 
            -- Respiratory parameters
            AVG(pao2_fio2_ratio) as avg_pao2_fio2,
            STDDEV(pao2_fio2_ratio) as std_pao2_fio2,
            
            -- Cardiovascular
            AVG(map) as avg_map,
            STDDEV(map) as std_map,
            
            -- Other parameters
            AVG(platelets) as avg_platelets,
            AVG(bilirubin) as avg_bilirubin,
            AVG(gcs_total) as avg_gcs,
            AVG(creatinine) as avg_creatinine,
            
            COUNT(*) as total_records
        FROM {GOLD_FULL_TABLE_NAME}
        WHERE sofa_total IS NOT NULL
        """
        
        result = pd.read_sql_query(text(query), self.engine)
        distributions = result.iloc[0].to_dict()
        
        self.validation_results['clinical_distributions'] = distributions
        
        self.logger.info("📈 Clinical Parameter Averages:")
        self.logger.info(f"  PaO2/FiO2 Ratio: {distributions['avg_pao2_fio2']:.1f} ± {distributions['std_pao2_fio2']:.1f}")
        self.logger.info(f"  MAP: {distributions['avg_map']:.1f} ± {distributions['std_map']:.1f} mmHg")
        self.logger.info(f"  Platelets: {distributions['avg_platelets']:.1f} ×10³/μL")
        self.logger.info(f"  Bilirubin: {distributions['avg_bilirubin']:.2f} mg/dL")
        self.logger.info(f"  GCS: {distributions['avg_gcs']:.1f}")
        self.logger.info(f"  Creatinine: {distributions['avg_creatinine']:.2f} mg/dL")
    
    def generate_validation_report(self):
        """Generate comprehensive validation report"""
        self.logger.info("📝 Generating validation report...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"gold_validation_report_{timestamp}.txt"
        
        with open(report_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("GOLD LAYER SOFA VALIDATION REPORT\n")
            f.write("="*80 + "\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Table: {GOLD_FULL_TABLE_NAME}\n")
            f.write("\n")
            
            # Basic Statistics
            f.write("BASIC STATISTICS\n")
            f.write("-"*40 + "\n")
            stats = self.validation_results.get('basic_statistics', {})
            for key, value in stats.items():
                f.write(f"{key}: {value}\n")
            f.write("\n")
            
            # SOFA Score Validation
            f.write("SOFA SCORE VALIDATION\n")
            f.write("-"*40 + "\n")
            sofa_ranges = self.validation_results.get('sofa_ranges', {})
            f.write(f"Total SOFA Range: {sofa_ranges.get('min_total_sofa')}-{sofa_ranges.get('max_total_sofa')}\n")
            f.write(f"Average SOFA: {sofa_ranges.get('avg_total_sofa', 0):.2f}\n")
            f.write(f"Invalid Total SOFA Scores: {sofa_ranges.get('invalid_total_sofa', 0)}\n")
            f.write(f"Invalid Respiratory Subscores: {sofa_ranges.get('invalid_resp', 0)}\n")
            f.write(f"Invalid Cardiovascular Subscores: {sofa_ranges.get('invalid_cardio', 0)}\n")
            f.write(f"Invalid Coagulation Subscores: {sofa_ranges.get('invalid_coag', 0)}\n")
            f.write(f"Invalid Liver Subscores: {sofa_ranges.get('invalid_liver', 0)}\n")
            f.write(f"Invalid CNS Subscores: {sofa_ranges.get('invalid_cns', 0)}\n")
            f.write(f"Invalid Renal Subscores: {sofa_ranges.get('invalid_renal', 0)}\n")
            f.write("\n")
            
            # Subscore Consistency
            f.write("SUBSCORE CONSISTENCY\n")
            f.write("-"*40 + "\n")
            consistency = self.validation_results.get('subscore_consistency', {})
            f.write(f"Inconsistent Records: {consistency.get('inconsistent_records', 0)}\n")
            f.write(f"Consistency Rate: {consistency.get('consistency_rate', 0):.2f}%\n")
            f.write("\n")
            
            # Imputation Rates
            f.write("IMPUTATION RATES\n")
            f.write("-"*40 + "\n")
            rates = self.validation_results.get('imputation_rates', {})
            f.write(f"PaO2/FiO2 Imputation: {rates.get('pao2_imputation_rate', 0):.1f}%\n")
            f.write(f"SpO2 Surrogate Usage: {rates.get('spo2_surrogate_rate', 0):.1f}%\n")
            f.write(f"MAP Imputation: {rates.get('map_imputation_rate', 0):.1f}%\n")
            f.write(f"Platelets Imputation: {rates.get('platelets_imputation_rate', 0):.1f}%\n")
            f.write(f"Bilirubin Imputation: {rates.get('bilirubin_imputation_rate', 0):.1f}%\n")
            f.write(f"GCS Imputation: {rates.get('gcs_imputation_rate', 0):.1f}%\n")
            f.write(f"Creatinine Imputation: {rates.get('creatinine_imputation_rate', 0):.1f}%\n")
            f.write(f"Urine Output Imputation: {rates.get('urine_imputation_rate', 0):.1f}%\n")
            f.write("\n")
            
            # Missing Data Patterns
            f.write("MISSING DATA PATTERNS\n")
            f.write("-"*40 + "\n")
            missing_patterns = self.validation_results.get('missing_patterns', [])
            for pattern in missing_patterns:
                missing_count = pattern.get('missing_count', 0)
                frequency = pattern.get('frequency', 0)
                f.write(f"{missing_count}/6 components missing: {frequency:,} windows\n")
            f.write("\n")
            
            # ARI Classification
            f.write("ARI CLASSIFICATION\n")
            f.write("-"*40 + "\n")
            ari_data = self.validation_results.get('ari_classification', [])
            for disease in ari_data:
                f.write(f"{disease['disease_type']}: {disease['patient_count']:,} patients, "
                       f"{disease['window_count']:,} windows, avg SOFA: {disease['avg_sofa_score']:.1f}\n")
            f.write("\n")
            
            # Clinical Distributions
            f.write("CLINICAL PARAMETER DISTRIBUTIONS\n")
            f.write("-"*40 + "\n")
            distributions = self.validation_results.get('clinical_distributions', {})
            f.write(f"Average PaO2/FiO2 Ratio: {distributions.get('avg_pao2_fio2', 0):.1f}\n")
            f.write(f"Average MAP: {distributions.get('avg_map', 0):.1f} mmHg\n")
            f.write(f"Average Platelets: {distributions.get('avg_platelets', 0):.1f} ×10³/μL\n")
            f.write(f"Average Bilirubin: {distributions.get('avg_bilirubin', 0):.2f} mg/dL\n")
            f.write(f"Average GCS: {distributions.get('avg_gcs', 0):.1f}\n")
            f.write(f"Average Creatinine: {distributions.get('avg_creatinine', 0):.2f} mg/dL\n")
            f.write("\n")
            
            f.write("="*80 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*80 + "\n")
        
        self.logger.info(f"📄 Validation report saved to: {report_file}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main validation execution"""
    logger = setup_logging()
    
    try:
        logger.info("🔍 Starting Gold Layer SOFA validation...")
        
        # Create database connection
        engine = create_db_connection()
        
        # Run validation
        validator = GoldLayerValidator(engine, logger)
        validator.run_all_validations()
        
        logger.info("✅ Gold Layer validation completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
