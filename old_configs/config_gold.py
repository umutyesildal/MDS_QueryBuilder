#!/usr/bin/env python3
"""
Gold Layer Configuration for SOFA Score Calculation
==================================================

Configuration settings for the Gold Layer ETL pipeline that calculates
SOFA scores from Silver layer standardized data.

This includes database settings, windowing parameters, aggregation rules,
and imputation strategies specific to SOFA score calculation.

Author: Medical Data Science Team
Date: 2025-06-04
"""

import os
from datetime import timedelta

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================

# Import database configuration from config_local.py
try:
    from config_local import DB_CONFIG
except ImportError:
    print("⚠️  Warning: config_local.py not found. Using template configuration.")
    from config_template import DB_CONFIG

# Gold schema configuration
GOLD_SCHEMA = "gold"
GOLD_TABLE_NAME = "sofa_scores"
GOLD_FULL_TABLE_NAME = f"{GOLD_SCHEMA}.{GOLD_TABLE_NAME}"

# Source tables
SILVER_TABLE = "silver.collection_disease_std"
ICU_STAYS_TABLE = "mimiciv_icu.icustays"
DIAGNOSES_TABLE = "mimiciv_hosp.diagnoses_icd"

# =============================================================================
# TIME WINDOWING CONFIGURATION
# =============================================================================

WINDOWING_CONFIG = {
    'window_duration_hours': 24,        # 24-hour windows
    'window_overlap': False,            # Non-overlapping consecutive windows
    'reference_time': 'icu_intime',     # Use ICU admission time as reference
    'max_windows_per_stay': 30,         # Maximum 30 days per ICU stay
    'min_measurements_per_window': 1,   # Minimum measurements required per window
}

# =============================================================================
# SOFA PARAMETER AGGREGATION RULES
# =============================================================================

SOFA_AGGREGATION_RULES = {
    # Respiratory: Take minimum (worst) PaO2/FiO2 ratio in window
    'respiratory': {
        'pao2': {'aggregation': 'min', 'omop_concept': 40762499},  # Not available in Silver layer
        'spo2': {'aggregation': 'min', 'omop_concept': 40764520},  # CORRECT: Available in Silver layer
        'fio2': {'aggregation': 'mean', 'omop_concept': 4353936},  # Not available in Silver layer
        'mechanical_ventilation': {'aggregation': 'max', 'omop_concept': 4298651}  # Not available in Silver layer
    },
    
    # Cardiovascular: Take minimum MAP and maximum vasopressor doses
    'cardiovascular': {
        'map': {'aggregation': 'min', 'omop_concept': 3004249},  # Not available in Silver layer
        'dopamine': {'aggregation': 'max', 'omop_concept': 1307046},  # Not available in Silver layer
        'epinephrine': {'aggregation': 'max', 'omop_concept': 1343916},  # Not available in Silver layer
        'norepinephrine': {'aggregation': 'max', 'omop_concept': 1344965},  # Not available in Silver layer
        'dobutamine': {'aggregation': 'max', 'omop_concept': 1307863}  # Not available in Silver layer
    },
    
    # Coagulation: Take minimum (worst) platelet count  
    'coagulation': {
        'platelets': {'aggregation': 'min', 'omop_concept': 3013650}  # Not available in Silver layer
    },
    
    # Liver: Take maximum (worst) bilirubin
    'liver': {
        'bilirubin': {'aggregation': 'max', 'omop_concept': 3017044}  # Not available in Silver layer
    },
    
    # CNS: Take minimum (worst) GCS
    'cns': {
        'gcs': {'aggregation': 'min', 'omop_concept': 3012386}
    },
    
    # Renal: Take maximum creatinine, sum urine output
    'renal': {
        'creatinine': {'aggregation': 'max', 'omop_concept': 3016723},
        'urine_output': {'aggregation': 'sum', 'omop_concept': 3012110}
    }
}

# =============================================================================
# IMPUTATION CONFIGURATION
# =============================================================================

IMPUTATION_CONFIG = {
    # Last Observation Carried Forward settings
    'locf': {
        'enabled': True,
        'max_lookback_hours': 48,       # Look back up to 48 hours
        'within_stay_only': True,       # Don't carry forward across ICU stays
    },
    
    # Population median imputation
    'population_median': {
        'enabled': True,
        'population': 'ari_patients',   # Use ARI patients only for median calculation
        'min_sample_size': 10,          # Minimum samples required for reliable median
    },
    
    # SpO2/FiO2 surrogate for PaO2/FiO2
    'spo2_surrogate': {
        'enabled': True,
        'conversion_factor': 1.0,       # Direct SpO2/FiO2 ratio
        'flag_as_imputed': True,        # Mark as imputed when using surrogate
    },
    
    # Missing data thresholds
    'missing_data': {
        'max_missing_components': 5,    # Skip if more than 5/6 SOFA components missing (relaxed)
        'require_respiratory': True,    # Respiratory component is mandatory
        'require_cardiovascular': False, # Cardiovascular can be imputed
    }
}

# =============================================================================
# DATA QUALITY FILTERS
# =============================================================================

QUALITY_FILTERS = {
    # Silver layer data quality filters
    'exclude_outliers': True,           # Skip records flagged as outliers in Silver
    'exclude_error_records': True,      # Skip records with error flags
    'min_stay_duration_hours': 6,       # Minimum ICU stay duration
    'max_stay_duration_days': 100,      # Maximum reasonable ICU stay
    
    # SOFA-specific filters
    'sofa_value_ranges': {
        'pao2_fio2_min': 10,           # Minimum reasonable PaO2/FiO2 ratio
        'pao2_fio2_max': 600,          # Maximum reasonable PaO2/FiO2 ratio
        'map_min': 20,                 # Minimum reasonable MAP
        'map_max': 200,                # Maximum reasonable MAP
        'platelets_min': 1,            # Minimum platelet count (×10³/μL)
        'platelets_max': 1000,         # Maximum platelet count
        'bilirubin_min': 0.1,          # Minimum bilirubin
        'bilirubin_max': 50,           # Maximum bilirubin
        'gcs_min': 3,                  # Minimum GCS
        'gcs_max': 15,                 # Maximum GCS
        'creatinine_min': 0.1,         # Minimum creatinine
        'creatinine_max': 15,          # Maximum creatinine
        'urine_output_max': 5000,      # Maximum urine output per 24h (mL)
    }
}

# =============================================================================
# ARI PATIENT IDENTIFICATION
# =============================================================================

ARI_IDENTIFICATION = {
    'use_icd_codes': True,
    'icd_code_types': ['icd_code'],
    'icd_codes': [
        # ICD-10 codes for Acute Respiratory Failure (actual codes from database)
        'J9600', 'J9601', 'J9610', 'J9621', 'J9622', 'J9690', 'J9691',  # Acute respiratory failure variants
        'J80',                                    # ARDS
        'J441', 'J449',                          # COPD with acute exacerbation
        
        # ICD-9 codes (actual codes from database)
        '51881', '51889', '5184', '5180', '5187', '51851',  # Acute respiratory failure variants
        '41401',                                 # Coronary atherosclerosis (often comorbid)
    ],
    'add_disease_column': True,                 # Add disease_type column to output
    'default_disease_type': 'OTHER',            # For non-ARI patients
    'ari_disease_type': 'ARI',                  # For ARI patients
}

# =============================================================================
# OUTPUT SCHEMA CONFIGURATION
# =============================================================================

GOLD_TABLE_SCHEMA = {
    # Patient and stay identifiers
    'subject_id': 'INTEGER NOT NULL',
    'hadm_id': 'INTEGER',
    'stay_id': 'INTEGER NOT NULL',
    'disease_type': 'VARCHAR(10) DEFAULT \'OTHER\'',
    
    # Time window information
    'window_start': 'TIMESTAMP NOT NULL',
    'window_end': 'TIMESTAMP NOT NULL', 
    'window_number': 'INTEGER NOT NULL',
    'icu_day': 'INTEGER',
    
    # Raw SOFA parameter values (aggregated within window)
    'pao2': 'NUMERIC',
    'fio2': 'NUMERIC', 
    'pao2_fio2_ratio': 'NUMERIC',
    'spo2': 'NUMERIC',
    'spo2_fio2_ratio': 'NUMERIC',
    'is_mechanically_ventilated': 'BOOLEAN DEFAULT FALSE',
    
    'map': 'NUMERIC',
    'dopamine_dose': 'NUMERIC',
    'epinephrine_dose': 'NUMERIC',
    'norepinephrine_dose': 'NUMERIC',
    'dobutamine_dose': 'NUMERIC',
    
    'platelets': 'NUMERIC',
    'bilirubin': 'NUMERIC',
    'gcs_total': 'INTEGER',
    'creatinine': 'NUMERIC',
    'urine_output_24h': 'NUMERIC',
    
    # Imputation flags
    'pao2_fio2_imputed': 'BOOLEAN DEFAULT FALSE',
    'spo2_fio2_surrogate': 'BOOLEAN DEFAULT FALSE',
    'map_imputed': 'BOOLEAN DEFAULT FALSE',
    'platelets_imputed': 'BOOLEAN DEFAULT FALSE',
    'bilirubin_imputed': 'BOOLEAN DEFAULT FALSE',
    'gcs_imputed': 'BOOLEAN DEFAULT FALSE',
    'creatinine_imputed': 'BOOLEAN DEFAULT FALSE',
    'urine_output_imputed': 'BOOLEAN DEFAULT FALSE',
    
    # SOFA subscores
    'sofa_respiratory_subscore': 'INTEGER',
    'sofa_cardiovascular_subscore': 'INTEGER',
    'sofa_coagulation_subscore': 'INTEGER',
    'sofa_liver_subscore': 'INTEGER',
    'sofa_cns_subscore': 'INTEGER',
    'sofa_renal_subscore': 'INTEGER',
    
    # SOFA total and metadata
    'sofa_total': 'INTEGER',
    'sofa_severity': 'VARCHAR(20)',
    'missing_components': 'TEXT[]',
    'calculation_notes': 'TEXT',
    
    # Audit fields
    'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
    'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
}

# Primary key and indexes
GOLD_TABLE_INDEXES = [
    'PRIMARY KEY (subject_id, stay_id, window_start)',
    'CREATE INDEX idx_sofa_subject ON gold.sofa_scores (subject_id)',
    'CREATE INDEX idx_sofa_stay ON gold.sofa_scores (stay_id)', 
    'CREATE INDEX idx_sofa_window ON gold.sofa_scores (window_start)',
    'CREATE INDEX idx_sofa_total ON gold.sofa_scores (sofa_total)',
    'CREATE INDEX idx_sofa_disease ON gold.sofa_scores (disease_type)',
]

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOGGING_CONFIG = {
    'log_file': 'gold_sofa_calculation.log',
    'log_level': 'INFO',
    'log_format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'console_output': True,
    'file_output': True,
}

# =============================================================================
# BATCH PROCESSING CONFIGURATION
# =============================================================================

BATCH_CONFIG = {
    'enabled': True,
    'batch_size': 1000,                 # Process 1000 patients at a time
    'parallel_processing': False,       # Single-threaded for now
    'progress_reporting': True,         # Report progress every batch
    'checkpoint_frequency': 5,          # Save progress every 5 batches
}

# =============================================================================
# VALIDATION SETTINGS
# =============================================================================

VALIDATION_CONFIG = {
    'run_validation': True,
    'validation_checks': [
        'sofa_score_ranges',            # Check SOFA scores are 0-24
        'subscore_ranges',              # Check subscores are 0-4
        'missing_data_rates',           # Report imputation rates
        'temporal_consistency',         # Check window ordering
        'data_completeness',            # Overall completeness report
    ],
    'generate_report': True,
    'report_file': 'gold_validation_report.txt',
}

# =============================================================================
# EXAMPLE QUERIES CONFIGURATION
# =============================================================================

EXAMPLE_QUERIES = {
    'patient_sofa_trajectory': """
        SELECT subject_id, stay_id, window_start, icu_day, sofa_total, sofa_severity
        FROM {gold_table}
        WHERE subject_id = %s
        ORDER BY window_start;
    """,
    
    'sofa_distribution': """
        SELECT sofa_total, COUNT(*) as frequency,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM {gold_table}
        GROUP BY sofa_total
        ORDER BY sofa_total;
    """,
    
    'ari_vs_other_sofa': """
        SELECT disease_type, 
               AVG(sofa_total) as avg_sofa,
               STDDEV(sofa_total) as std_sofa,
               COUNT(*) as total_windows
        FROM {gold_table}
        GROUP BY disease_type;
    """,
    
    'imputation_summary': """
        SELECT 
            AVG(CASE WHEN pao2_fio2_imputed THEN 1.0 ELSE 0.0 END) as pao2_imputation_rate,
            AVG(CASE WHEN spo2_fio2_surrogate THEN 1.0 ELSE 0.0 END) as spo2_surrogate_rate,
            AVG(CASE WHEN map_imputed THEN 1.0 ELSE 0.0 END) as map_imputation_rate,
            COUNT(*) as total_records
        FROM {gold_table};
    """
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_gold_table_create_sql():
    """Generate CREATE TABLE SQL for Gold table"""
    columns = []
    for col_name, col_type in GOLD_TABLE_SCHEMA.items():
        columns.append(f"    {col_name} {col_type}")
    
    indexes = []
    for index in GOLD_TABLE_INDEXES:
        if index.startswith('PRIMARY KEY'):
            continue  # Primary key is handled in column definition
        elif index.startswith('CREATE INDEX'):
            # Replace CREATE INDEX with CREATE INDEX IF NOT EXISTS
            index_fixed = index.replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
            indexes.append(f"{index_fixed};")  
        else:
            indexes.append(f"CREATE INDEX IF NOT EXISTS {index};")
    
    create_sql = f"""
CREATE TABLE IF NOT EXISTS {GOLD_FULL_TABLE_NAME} (
{','.join(columns)},
    PRIMARY KEY (subject_id, stay_id, window_start)
);

{chr(10).join(indexes)}
"""
    return create_sql

def get_example_query(query_name, **kwargs):
    """Get formatted example query"""
    if query_name in EXAMPLE_QUERIES:
        return EXAMPLE_QUERIES[query_name].format(
            gold_table=GOLD_FULL_TABLE_NAME,
            **kwargs
        )
    return None

if __name__ == "__main__":
    print("🔧 Gold Layer Configuration Initialized")
    print(f"📊 Gold Table: {GOLD_FULL_TABLE_NAME}")
    print(f"⏰ Window Duration: {WINDOWING_CONFIG['window_duration_hours']} hours")
    print(f"🏥 ARI Identification: {len(ARI_IDENTIFICATION['icd_codes'])} ICD codes")
    print(f"📈 Schema Columns: {len(GOLD_TABLE_SCHEMA)} columns")
    print("✅ Configuration loaded successfully!")
