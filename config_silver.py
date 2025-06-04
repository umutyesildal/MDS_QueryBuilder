"""
Silver Layer Configuration
=========================

Configuration for Silver layer data transformation including OMOP mappings,
unit conversions, and clinical parameter limits.
"""

# OMOP Concept Mappings with additional metadata
OMOP_MAPPING = {
    # Respiratory Parameters
    220210: {
        'concept_id': 3027018,
        'concept_name': 'Respiratory Rate',
        'parameter_type': 'vital',
        'standard_unit': 'breaths/min',
        'limits': (8, 60)
    },
    224688: {
        'concept_id': 3027018,
        'concept_name': 'Respiratory Rate',
        'parameter_type': 'vital',
        'standard_unit': 'breaths/min',
        'limits': (8, 60)
    },
    224689: {
        'concept_id': 3027018,
        'concept_name': 'Respiratory Rate',
        'parameter_type': 'vital',
        'standard_unit': 'breaths/min',
        'limits': (8, 60)
    },
    224690: {
        'concept_id': 3027018,
        'concept_name': 'Respiratory Rate',
        'parameter_type': 'vital',
        'standard_unit': 'breaths/min',
        'limits': (8, 60)
    },
    
    # Heart Rate Parameters
    220045: {
        'concept_id': 3027017,
        'concept_name': 'Heart Rate',
        'parameter_type': 'vital',
        'standard_unit': 'beats/min',
        'limits': (20, 250)
    },
    220046: {
        'concept_id': 3027017,
        'concept_name': 'Heart Rate Alarm High',
        'parameter_type': 'vital',
        'standard_unit': 'beats/min',
        'limits': (20, 250)
    },
    220047: {
        'concept_id': 3027017,
        'concept_name': 'Heart Rate Alarm Low',
        'parameter_type': 'vital',
        'standard_unit': 'beats/min',
        'limits': (20, 250)
    },
    
    # Oxygen Saturation
    226253: {
        'concept_id': 40764520,
        'concept_name': 'Oxygen Saturation',
        'parameter_type': 'vital',
        'standard_unit': '%',
        'limits': (70, 100)
    },
    
    # Tidal Volume Parameters
    224684: {
        'concept_id': 3024289,
        'concept_name': 'Tidal Volume Set',
        'parameter_type': 'ventilation',
        'standard_unit': 'mL',
        'limits': (200, 1500)
    },
    224685: {
        'concept_id': 3024289,
        'concept_name': 'Tidal Volume Observed',
        'parameter_type': 'ventilation',
        'standard_unit': 'mL',
        'limits': (200, 1500)
    },
    224686: {
        'concept_id': 3024289,
        'concept_name': 'Tidal Volume Spontaneous',
        'parameter_type': 'ventilation',
        'standard_unit': 'mL',
        'limits': (200, 1500)
    },
    
    # Laboratory Parameters
    50820: {
        'concept_id': 3014605,
        'concept_name': 'pH',
        'parameter_type': 'lab',
        'standard_unit': 'pH',
        'limits': (6.8, 8.0)
    },
    50912: {
        'concept_id': 3016723,
        'concept_name': 'Creatinine',
        'parameter_type': 'lab',
        'standard_unit': 'mg/dL',
        'limits': (0.3, 15.0)
    },
    50862: {
        'concept_id': 3013705,
        'concept_name': 'Albumin',
        'parameter_type': 'lab',
        'standard_unit': 'g/dL',
        'limits': (1.0, 6.0)
    },
    50813: {
        'concept_id': 3004501,
        'concept_name': 'Lactate',
        'parameter_type': 'lab',
        'standard_unit': 'mmol/L',
        'limits': (0.3, 25.0)
    },
    50970: {
        'concept_id': 3019550,
        'concept_name': 'Phosphate',
        'parameter_type': 'lab',
        'standard_unit': 'mg/dL',
        'limits': (1.0, 10.0)
    },
    50863: {
        'concept_id': 3006595,
        'concept_name': 'Alkaline Phosphatase',
        'parameter_type': 'lab',
        'standard_unit': 'U/L',
        'limits': (30, 1000)
    },
    
    # Blood Cell Parameters
    51146: {
        'concept_id': 3003510,
        'concept_name': 'Basophils',
        'parameter_type': 'lab',
        'standard_unit': '%',
        'limits': (0, 5)
    },
    51200: {
        'concept_id': 3003846,
        'concept_name': 'Eosinophils',
        'parameter_type': 'lab',
        'standard_unit': '%',
        'limits': (0, 10)
    },
    51256: {
        'concept_id': 3013650,
        'concept_name': 'Neutrophils',
        'parameter_type': 'lab',
        'standard_unit': '%',
        'limits': (40, 95)
    }
}

# Unit Conversion Functions
UNIT_CONVERSIONS = {
    # Creatinine conversions
    ('µmol/L', 'mg/dL'): lambda x: x * 0.0113,
    ('umol/L', 'mg/dL'): lambda x: x * 0.0113,
    ('mmol/L', 'mg/dL'): lambda x: x * 11.3,
    
    # Albumin conversions  
    ('g/L', 'g/dL'): lambda x: x / 10.0,
    
    # Phosphate conversions
    ('mmol/L', 'mg/dL'): lambda x: x * 3.1,
    
    # Lactate conversions
    ('mg/dL', 'mmol/L'): lambda x: x / 9.0,
    
    # Volume conversions
    ('L', 'mL'): lambda x: x * 1000,
    ('cc', 'mL'): lambda x: x,  # Same unit
    
    # Standardize unit names
    ('insp/min', 'breaths/min'): lambda x: x,
    ('bpm', 'beats/min'): lambda x: x,
    ('per minute', 'breaths/min'): lambda x: x,
    ('/min', 'beats/min'): lambda x: x,
    
    # Handle percentage formats
    ('percent', '%'): lambda x: x,
    ('ratio', '%'): lambda x: x * 100 if x <= 1 else x
}

# Clinical Parameter Limits (for outlier detection)
CLINICAL_LIMITS = {
    'Respiratory Rate': (8, 60),
    'Heart Rate': (20, 250),
    'Oxygen Saturation': (70, 100),
    'Tidal Volume': (200, 1500),
    'pH': (6.8, 8.0),
    'Creatinine': (0.3, 15.0),
    'Albumin': (1.0, 6.0),
    'Lactate': (0.3, 25.0),
    'Phosphate': (1.0, 10.0),
    'Alkaline Phosphatase': (30, 1000),
    'Basophils': (0, 5),
    'Eosinophils': (0, 10),
    'Neutrophils': (40, 95)
}

# Silver Schema Configuration
SILVER_SCHEMA = 'silver'
SILVER_TABLE = 'collection_disease_std'

# Quality flags
QUALITY_FLAGS = {
    'UNIT_CONVERTED': 'Unit converted during standardization',
    'OUTLIER_DETECTED': 'Value outside clinical normal range',
    'DUPLICATE_RESOLVED': 'Duplicate record resolved by latest storetime',
    'VALUE_CLEANED': 'Non-numeric value cleaned or converted',
    'ERROR_FLAGGED': 'Value flagged as potentially erroneous'
}
