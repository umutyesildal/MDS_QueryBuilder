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

# Data Quality Filters
QUALITY_FILTERS = {
    'chartevents': {
        'exclude_error': True,  # Exclude error = 1
        'require_valuenum': True  # Require non-null valuenum
    },
    'labevents': {
        'exclude_flags': ['abnormal', 'error'],  # Exclude these flags
        'require_valuenum': True  # Require non-null valuenum
    }
}

# Output Schema Configuration
BRONZE_SCHEMA = 'bronze'
OUTPUT_TABLE = 'collection_disease'
