# -*- coding: utf-8 -*-
"""
Configuration file for QueryBuilder
===================================

Contains parameter mappings and general configuration.
For database configuration, see config_template.py and create config_local.py
"""

# Database Configuration Import
try:
    from config_local import DB_CONFIG
except ImportError:
    print("⚠️ config_local.py not found. Using default configuration.")
    print("📝 Please copy config_template.py to config_local.py and update with your credentials.")
    
    # Default configuration (for testing/template purposes)
    DB_CONFIG = {
        'host': 'localhost',
        'port': 5432,
        'database': 'mimiciv',
        'user': 'your_username',    # UPDATE THIS
        'password': None            # UPDATE THIS
    }

# OMOP Concept Mappings (from Übung 2)
OMOP_CONCEPTS = {
    'PaO2_FiO2_Ratio': 40762499,
    'SpO2_FiO2_Ratio': 40764520,
    'Respiratory_Rate': 3027018,
    'Heart_Rate': 3027017,
    'PaCO2': 3020656,
    'Tidal_Volume': 3024289,
    'Minute_Ventilation': 3024328,
    'Creatinine': 3016723,
    'pH': 3014605,
    'Albumin': 3013705,
    'Uric_Acid': 3013466,
    'NT_proBNP': 3016728,
    'D_Dimer': 3003737,
    'Homocysteine': 3002304,
    'Procalcitonin': 3013682,
    'IL_6': 3015039,
    'IL_8': 3015040,
    'IL_10': 3015037,
    'ST2': 43013695,
    'Pentraxin_3': 4172961,
    'Fraktalkin': 4273690,
    'sRAGE': 3022415,
    'KL_6': 43531668,
    'PAI_1': 4314768,
    'VEGF': 43013099
}

# Chart Events Parameters (mimiciv_icu.chartevents)
CHART_PARAMETERS = [
    'spo2',
    'respiratory rate', 
    'respiration rate',
    'heart rate',
    'tidal volume',
    'minute ventilation',
    'oxygen saturation',
    'pulse oximetry',
    'ventilator rate',
    'spontaneous rate'
]

# Lab Events Parameters (mimiciv_hosp.labevents) 
LAB_PARAMETERS = [
    'ph',
    'paco2',
    'carbon dioxide',
    'creatinine',
    'albumin',
    'd-dimer',
    'procalcitonin',
    'interleukin-6',
    'il-6',
    'interleukin-8', 
    'il-8',
    'interleukin-10',
    'il-10',
    'nt-probnp',
    'pro-bnp',
    'homocysteine',
    'harnsäure',
    'uric acid',
    'srage',
    'kl-6',
    'pai-1',
    'vegf',
    'lactate',
    'lactic acid'
]

# =============================================================================
# ETL CONFIGURATION - Two Different Configurations for Task 5.4
# =============================================================================

# Configuration 1: Mean-based approach (from Task 5.3)
CONFIG_1 = {
    'name': 'mean_based_config',
    'description': 'Uses mean-based aggregation and mean imputation',
    'aggregation_method': 'mean',
    'imputation_method': 'mean',
    'outlier_handling': 'iqr',        # IQR method for outlier removal
    'outlier_threshold': 1.5,         # IQR multiplier
    'time_window_hours': 24,          # 24-hour time window
    'min_observations': 2,            # Minimum number of observations
    'output_table': 'gold_scores_config1'
}

# Configuration 2: Median-based approach (alternative for Task 5.4)
CONFIG_2 = {
    'name': 'median_based_config',
    'description': 'Uses median-based aggregation and median imputation',
    'aggregation_method': 'median',
    'imputation_method': 'median',
    'outlier_handling': 'percentile',  # Percentile-based outlier removal
    'outlier_threshold': 0.05,         # Remove top/bottom 5%
    'time_window_hours': 12,           # 12-hour time window (shorter)
    'min_observations': 3,             # Higher minimum observations
    'output_table': 'gold_scores_config2'
}

# Active Configuration (default to CONFIG_1)
ACTIVE_CONFIG = CONFIG_1

# Configuration switching function
def set_active_config(config_number):
    """
    Changes the active configuration
    
    Args:
        config_number (int): 1 or 2
    """
    global ACTIVE_CONFIG
    if config_number == 1:
        ACTIVE_CONFIG = CONFIG_1
        print("Active configuration: {}".format(CONFIG_1['name']))
    elif config_number == 2:
        ACTIVE_CONFIG = CONFIG_2
        print("Active configuration: {}".format(CONFIG_2['name']))
    else:
        print("Invalid configuration number. Must be 1 or 2.")

# =============================================================================
# ANALYSIS CONFIGURATION - Task 5.4 Analysis Settings
# =============================================================================

# Comparative Analysis Settings
COMPARISON_CONFIG = {
    'statistical_tests': {
        'correlation_methods': ['pearson', 'spearman'],
        'significance_level': 0.05,
        'multiple_testing_correction': 'bonferroni'
    },
    'visualization': {
        'plot_types': ['histogram', 'boxplot', 'scatter', 'bland_altman'],
        'figure_size': (12, 8),
        'save_plots': True,
        'plot_format': 'png'
    },
    'subgroup_analysis': {
        'age_groups': [18, 45, 65, 85],    # Age group thresholds
        'gender_stratification': True,
        'mortality_analysis': True
    }
}

# Clinical Outcome Settings
OUTCOME_CONFIG = {
    'mortality_types': [
        'hospital_mortality',
        'icu_mortality', 
        '30_day_mortality'
    ],
    'outcome_tables': {
        'mortality': 'mimiciv_core.patients',
        'icu_stays': 'mimiciv_icu.icustays',
        'hospital_stays': 'mimiciv_hosp.admissions'
    }
}

# Disease Comparison Settings (Option B from Task 5.4)
DISEASE_COMPARISON_CONFIG = {
    'icd_codes': {
        # Add your chosen disease ICD codes from Exercise 1
        'primary_disease': [],        # Your chosen disease codes
        'control_conditions': []      # Control/comparison conditions
    },
    'comparison_metrics': [
        'score_distributions',
        'mortality_rates',
        'length_of_stay',
        'severity_scores'
    ]
}

# =============================================================================
# DATA QUALITY FILTERS (Existing settings preserved)
# =============================================================================

QUALITY_FILTERS = {
    'chartevents': {
        'exclude_error': True,        # Exclude error = 1
        'require_valuenum': True      # Require non-null valuenum
    },
    'labevents': {
        'exclude_flags': ['abnormal', 'error'],  # Exclude these flags
        'require_valuenum': True      # Require non-null valuenum
    }
}

# Output Schema Configuration
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'
OUTPUT_TABLE = 'collection_disease'

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_config_summary():
    """Returns summary of active configuration"""
    return {
        'active_config': ACTIVE_CONFIG['name'],
        'aggregation': ACTIVE_CONFIG['aggregation_method'],
        'imputation': ACTIVE_CONFIG['imputation_method'],
        'outlier_handling': ACTIVE_CONFIG['outlier_handling'],
        'time_window': ACTIVE_CONFIG['time_window_hours'],
        'output_table': ACTIVE_CONFIG['output_table']
    }

def validate_config():
    """Validates configuration settings"""
    required_keys = ['name', 'aggregation_method', 'imputation_method', 
                     'outlier_handling', 'time_window_hours', 'output_table']
    
    for key in required_keys:
        if key not in ACTIVE_CONFIG:
            raise ValueError("Missing configuration key: {}".format(key))
    
    print("Configuration validation successful")
    return True

def get_both_configs():
    """Returns both configurations for comparison analysis"""
    return {
        'config_1': CONFIG_1,
        'config_2': CONFIG_2
    }

def get_comparison_tables():
    """Returns table names for both configurations"""
    return {
        'table_1': CONFIG_1['output_table'],
        'table_2': CONFIG_2['output_table']
    }

# Configuration justification for Task 5.4
CONFIG_JUSTIFICATION = {
    'config_1_rationale': """
    Mean-based approach with 24-hour windows:
    - Mean aggregation provides smooth estimates
    - Suitable for normally distributed parameters
    - Longer time window captures daily patterns
    - IQR outlier removal preserves data integrity
    """,
    'config_2_rationale': """
    Median-based approach with 12-hour windows:
    - Median aggregation robust to outliers
    - Better for skewed distributions
    - Shorter time window captures acute changes
    - Percentile-based outlier removal more aggressive
    - Higher minimum observations ensure reliability
    """
}

# Initialize configuration on import
if __name__ == "__main__":
    validate_config()
    print("Active Configuration: {}".format(ACTIVE_CONFIG['name']))
    print("Output Table: {}".format(ACTIVE_CONFIG['output_table']))