#!/usr/bin/env python3
"""
Enhanced Gold Layer with SOFA Scoring
=====================================

This module creates the Gold layer with comprehensive SOFA (Sequential Organ Failure Assessment)
scoring based on the enhanced Silver layer data with full parameter coverage.

Features:
- Comprehensive SOFA scoring across all 6 organ systems
- Time-window based calculations (24-hour windows)
- Patient-level and stay-level aggregations
- Clinical analytics and dashboards
- Real-time SOFA trend analysis

SOFA Components:
1. Respiratory: PaO2/FiO2, ventilation support
2. Cardiovascular: MAP, vasopressor support
3. Hepatic: Bilirubin levels
4. Coagulation: Platelet count
5. Renal: Creatinine, urine output
6. Neurological: Glasgow Coma Scale

Author: Medical Data Science Team
Date: 2025-06-05
"""

import pandas as pd
import numpy as np
import psycopg2
import json
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Tuple

# Import configurations
from config_local import DB_CONFIG

class EnhancedSOFACalculator:
    """Enhanced SOFA calculator with comprehensive organ system scoring"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.conn = psycopg2.connect(**DB_CONFIG)
        
        # Build connection string with proper password handling
        if DB_CONFIG.get('password'):
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        else:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        self.engine = create_engine(connection_string)
        
        # SOFA scoring criteria
        self.sofa_criteria = self._define_sofa_criteria()
        self.processing_stats = {
            'total_patients': 0,
            'total_stays': 0,
            'sofa_scores_calculated': 0,
            'time_windows_processed': 0,
            'start_time': datetime.now(),
            'errors': []
        }
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logger = logging.getLogger('EnhancedSOFACalculator')
        logger.setLevel(logging.INFO)
        
        # Setup log file path
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
        from file_paths import get_log_path
        
        # Create handlers
        file_handler = logging.FileHandler(get_log_path('sofa_calculator.log'))
        console_handler = logging.StreamHandler()
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def _define_sofa_criteria(self):
        """Define comprehensive SOFA scoring criteria for all organ systems"""
        return {
            'respiratory': {
                'pao2_fio2': {
                    0: {'min': 400, 'max': float('inf')},
                    1: {'min': 300, 'max': 399},
                    2: {'min': 200, 'max': 299},
                    3: {'min': 100, 'max': 199, 'ventilated': False},
                    4: {'min': 0, 'max': 99, 'ventilated': True}
                }
            },
            'cardiovascular': {
                'map_and_vasopressors': {
                    0: {'map_min': 70},
                    1: {'map_min': 0, 'map_max': 69},
                    2: {'vasopressor': 'dopamine', 'dose_max': 5},
                    3: {'vasopressor': ['dopamine', 'norepinephrine'], 'dose_max': 15},
                    4: {'vasopressor': ['dopamine', 'norepinephrine'], 'dose_min': 15}
                }
            },
            'hepatic': {
                'bilirubin': {
                    0: {'max': 1.2},
                    1: {'min': 1.2, 'max': 1.9},
                    2: {'min': 2.0, 'max': 5.9},
                    3: {'min': 6.0, 'max': 11.9},
                    4: {'min': 12.0}
                }
            },
            'coagulation': {
                'platelets': {
                    0: {'min': 150},
                    1: {'min': 100, 'max': 149},
                    2: {'min': 50, 'max': 99},
                    3: {'min': 20, 'max': 49},
                    4: {'max': 19}
                }
            },
            'renal': {
                'creatinine_urine': {
                    0: {'creatinine_max': 1.2},
                    1: {'creatinine_min': 1.2, 'creatinine_max': 1.9},
                    2: {'creatinine_min': 2.0, 'creatinine_max': 3.4},
                    3: {'creatinine_min': 3.5, 'creatinine_max': 4.9, 'urine_max': 500},
                    4: {'creatinine_min': 5.0, 'urine_max': 200}
                }
            },
            'neurological': {
                'gcs': {
                    0: {'gcs_min': 15},
                    1: {'gcs_min': 13, 'gcs_max': 14},
                    2: {'gcs_min': 10, 'gcs_max': 12},
                    3: {'gcs_min': 6, 'gcs_max': 9},
                    4: {'gcs_max': 5}
                }
            }
        }

    def create_gold_schema(self):
        """Create Gold schema and SOFA tables"""
        self.logger.info("🥇 Creating Gold schema and SOFA tables...")
        
        cur = self.conn.cursor()
        
        # Drop and recreate gold schema
        cur.execute("DROP SCHEMA IF EXISTS gold CASCADE")
        cur.execute("CREATE SCHEMA gold")
        
        # Create SOFA scores table
        create_sofa_table = """
        CREATE TABLE gold.sofa_scores (
            id SERIAL PRIMARY KEY,
            
            -- Patient and stay identifiers
            subject_id INTEGER NOT NULL,
            stay_id INTEGER NOT NULL,
            
            -- Time window
            charttime TIMESTAMP NOT NULL,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            
            -- Individual organ system scores (0-4 each)
            respiratory_score INTEGER DEFAULT 0,
            cardiovascular_score INTEGER DEFAULT 0,
            hepatic_score INTEGER DEFAULT 0,
            coagulation_score INTEGER DEFAULT 0,
            renal_score INTEGER DEFAULT 0,
            neurological_score INTEGER DEFAULT 0,
            
            -- Total SOFA score (0-24)
            total_sofa_score INTEGER DEFAULT 0,
            
            -- Supporting values used in calculation
            pao2_fio2_ratio NUMERIC,
            mean_arterial_pressure NUMERIC,
            vasopressor_doses JSONB,
            bilirubin_level NUMERIC,
            platelet_count NUMERIC,
            creatinine_level NUMERIC,
            urine_output_24h NUMERIC,
            gcs_total INTEGER,
            
            -- Data availability flags
            respiratory_data_available BOOLEAN DEFAULT FALSE,
            cardiovascular_data_available BOOLEAN DEFAULT FALSE,
            hepatic_data_available BOOLEAN DEFAULT FALSE,
            coagulation_data_available BOOLEAN DEFAULT FALSE,
            renal_data_available BOOLEAN DEFAULT FALSE,
            neurological_data_available BOOLEAN DEFAULT FALSE,
            
            -- Metadata
            calculation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_completeness_score NUMERIC,
            
            -- Indexes
            UNIQUE(subject_id, stay_id, charttime)
        );
        
        -- Create indexes
        CREATE INDEX idx_sofa_subject ON gold.sofa_scores (subject_id);
        CREATE INDEX idx_sofa_stay ON gold.sofa_scores (stay_id);
        CREATE INDEX idx_sofa_time ON gold.sofa_scores (charttime);
        CREATE INDEX idx_sofa_total ON gold.sofa_scores (total_sofa_score);
        """
        
        cur.execute(create_sofa_table)
        
        # Create SOFA analytics views
        create_analytics_views = """
        -- Patient-level SOFA summary
        CREATE VIEW gold.patient_sofa_summary AS
        SELECT 
            subject_id,
            COUNT(DISTINCT stay_id) as total_stays,
            COUNT(*) as total_measurements,
            MIN(charttime) as first_measurement,
            MAX(charttime) as last_measurement,
            
            -- SOFA statistics
            MIN(total_sofa_score) as min_sofa,
            MAX(total_sofa_score) as max_sofa,
            ROUND(AVG(total_sofa_score), 2) as avg_sofa,
            
            -- Organ system averages
            ROUND(AVG(respiratory_score), 2) as avg_respiratory,
            ROUND(AVG(cardiovascular_score), 2) as avg_cardiovascular,
            ROUND(AVG(hepatic_score), 2) as avg_hepatic,
            ROUND(AVG(coagulation_score), 2) as avg_coagulation,
            ROUND(AVG(renal_score), 2) as avg_renal,
            ROUND(AVG(neurological_score), 2) as avg_neurological,
            
            -- Data completeness
            ROUND(AVG(data_completeness_score), 2) as avg_completeness
        FROM gold.sofa_scores
        GROUP BY subject_id;
        
        -- Daily SOFA trends
        CREATE VIEW gold.daily_sofa_trends AS
        SELECT 
            DATE(charttime) as measurement_date,
            COUNT(DISTINCT subject_id) as unique_patients,
            COUNT(*) as total_scores,
            
            -- SOFA score distribution
            ROUND(AVG(total_sofa_score), 2) as avg_total_sofa,
            MIN(total_sofa_score) as min_total_sofa,
            MAX(total_sofa_score) as max_total_sofa,
            
            -- High-risk patients (SOFA >= 10)
            COUNT(CASE WHEN total_sofa_score >= 10 THEN 1 END) as high_risk_count,
            ROUND(100.0 * COUNT(CASE WHEN total_sofa_score >= 10 THEN 1 END) / COUNT(*), 2) as high_risk_percentage,
            
            -- System-specific averages
            ROUND(AVG(respiratory_score), 2) as avg_respiratory,
            ROUND(AVG(cardiovascular_score), 2) as avg_cardiovascular,
            ROUND(AVG(renal_score), 2) as avg_renal,
            ROUND(AVG(neurological_score), 2) as avg_neurological
        FROM gold.sofa_scores
        GROUP BY DATE(charttime)
        ORDER BY measurement_date;
        """
        
        cur.execute(create_analytics_views)
        self.conn.commit()
        
        self.logger.info("✅ Gold schema and SOFA tables created successfully")

    def calculate_respiratory_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate respiratory SOFA score (0-4)"""
        respiratory_data = data_window[data_window['sofa_system'] == 'respiratory']
        
        if respiratory_data.empty:
            return 0, False, {}
        
        # Get latest PaO2/FiO2 ratio or SpO2/FiO2 estimation
        pao2_fio2 = None
        ventilated = False
        
        # Look for direct PaO2/FiO2 measurements or calculate from available data
        spo2_values = respiratory_data[respiratory_data['concept_name'].str.contains('SpO2|Oxygen Saturation', case=False, na=False)]
        fio2_values = respiratory_data[respiratory_data['concept_name'].str.contains('FiO2|Inspired Oxygen', case=False, na=False)]
        
        if not spo2_values.empty and not fio2_values.empty:
            # Estimate PaO2/FiO2 from SpO2/FiO2
            latest_spo2 = spo2_values.iloc[-1]['valuenum_std']
            latest_fio2 = fio2_values.iloc[-1]['valuenum_std']
            
            if pd.notna(latest_spo2) and pd.notna(latest_fio2) and latest_fio2 > 0:
                # Rough approximation: PaO2 ≈ SpO2 * 1.5 (simplified)
                estimated_pao2 = latest_spo2 * 1.5
                pao2_fio2 = estimated_pao2 / (latest_fio2 / 100) if latest_fio2 > 1 else estimated_pao2 / latest_fio2
        
        # Check for ventilation
        vent_data = respiratory_data[respiratory_data['concept_name'].str.contains('Ventilator|PEEP|Mechanical', case=False, na=False)]
        ventilated = not vent_data.empty
        
        # Calculate score
        if pao2_fio2 is None:
            return 0, False, {}
        
        criteria = self.sofa_criteria['respiratory']['pao2_fio2']
        for score in [4, 3, 2, 1, 0]:
            if score == 4 and pao2_fio2 < 100 and ventilated:
                return 4, True, {'pao2_fio2': pao2_fio2, 'ventilated': ventilated}
            elif score == 3 and 100 <= pao2_fio2 < 200 and not ventilated:
                return 3, True, {'pao2_fio2': pao2_fio2, 'ventilated': ventilated}
            elif score <= 2 and pao2_fio2 >= criteria[score]['min']:
                return score, True, {'pao2_fio2': pao2_fio2, 'ventilated': ventilated}
        
        return 0, True, {'pao2_fio2': pao2_fio2, 'ventilated': ventilated}

    def calculate_cardiovascular_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate cardiovascular SOFA score (0-4)"""
        cardio_data = data_window[data_window['sofa_system'] == 'cardiovascular']
        
        if cardio_data.empty:
            return 0, False, {}
        
        # Calculate mean arterial pressure
        map_data = cardio_data[cardio_data['concept_name'].str.contains('Mean|MAP', case=False, na=False)]
        systolic_data = cardio_data[cardio_data['concept_name'].str.contains('systolic', case=False, na=False)]
        diastolic_data = cardio_data[cardio_data['concept_name'].str.contains('diastolic', case=False, na=False)]
        
        map_value = None
        if not map_data.empty:
            map_value = map_data.iloc[-1]['valuenum_std']
        elif not systolic_data.empty and not diastolic_data.empty:
            # Calculate MAP = (2 * DBP + SBP) / 3
            sys_val = systolic_data.iloc[-1]['valuenum_std']
            dia_val = diastolic_data.iloc[-1]['valuenum_std']
            if pd.notna(sys_val) and pd.notna(dia_val):
                map_value = (2 * dia_val + sys_val) / 3
        
        # Check for vasopressors
        vasopressor_data = cardio_data[cardio_data['concept_name'].str.contains('dopamine|norepinephrine|epinephrine|vasopressin', case=False, na=False)]
        vasopressor_doses = {}
        
        if not vasopressor_data.empty:
            for _, row in vasopressor_data.iterrows():
                drug_name = row['concept_name'].lower()
                dose = row['valuenum_std']
                if pd.notna(dose):
                    vasopressor_doses[drug_name] = dose
        
        # Calculate score
        if map_value is None and not vasopressor_doses:
            return 0, False, {}
        
        # Score based on MAP and vasopressors
        if vasopressor_doses:
            # Check for high-dose vasopressors (score 4)
            high_dose = any(dose >= 15 for dose in vasopressor_doses.values())
            if high_dose:
                return 4, True, {'map': map_value, 'vasopressors': vasopressor_doses}
            
            # Medium dose vasopressors (score 3)
            medium_dose = any(dose >= 5 for dose in vasopressor_doses.values())
            if medium_dose:
                return 3, True, {'map': map_value, 'vasopressors': vasopressor_doses}
            
            # Low dose vasopressors (score 2)
            return 2, True, {'map': map_value, 'vasopressors': vasopressor_doses}
        
        # No vasopressors, score based on MAP
        if map_value is not None:
            if map_value >= 70:
                return 0, True, {'map': map_value}
            else:
                return 1, True, {'map': map_value}
        
        return 0, True, {'map': map_value, 'vasopressors': vasopressor_doses}

    def calculate_hepatic_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate hepatic SOFA score (0-4)"""
        liver_data = data_window[data_window['sofa_system'] == 'liver']
        
        if liver_data.empty:
            return 0, False, {}
        
        # Get latest bilirubin level
        bilirubin_data = liver_data[liver_data['concept_name'].str.contains('bilirubin', case=False, na=False)]
        
        if bilirubin_data.empty:
            return 0, False, {}
        
        bilirubin_level = bilirubin_data.iloc[-1]['valuenum_std']
        
        if pd.isna(bilirubin_level):
            return 0, False, {}
        
        # Score based on bilirubin level (mg/dL)
        if bilirubin_level >= 12.0:
            return 4, True, {'bilirubin': bilirubin_level}
        elif bilirubin_level >= 6.0:
            return 3, True, {'bilirubin': bilirubin_level}
        elif bilirubin_level >= 2.0:
            return 2, True, {'bilirubin': bilirubin_level}
        elif bilirubin_level >= 1.2:
            return 1, True, {'bilirubin': bilirubin_level}
        else:
            return 0, True, {'bilirubin': bilirubin_level}

    def calculate_coagulation_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate coagulation SOFA score (0-4)"""
        coag_data = data_window[data_window['sofa_system'] == 'coagulation']
        
        if coag_data.empty:
            return 0, False, {}
        
        # Get latest platelet count
        platelet_data = coag_data[coag_data['concept_name'].str.contains('platelet', case=False, na=False)]
        
        if platelet_data.empty:
            return 0, False, {}
        
        platelet_count = platelet_data.iloc[-1]['valuenum_std']
        
        if pd.isna(platelet_count):
            return 0, False, {}
        
        # Score based on platelet count (×10³/μL)
        if platelet_count < 20:
            return 4, True, {'platelets': platelet_count}
        elif platelet_count < 50:
            return 3, True, {'platelets': platelet_count}
        elif platelet_count < 100:
            return 2, True, {'platelets': platelet_count}
        elif platelet_count < 150:
            return 1, True, {'platelets': platelet_count}
        else:
            return 0, True, {'platelets': platelet_count}

    def calculate_renal_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate renal SOFA score (0-4)"""
        renal_data = data_window[data_window['sofa_system'] == 'renal']
        
        if renal_data.empty:
            return 0, False, {}
        
        # Get creatinine and urine output
        creatinine_data = renal_data[renal_data['concept_name'].str.contains('creatinine', case=False, na=False)]
        urine_data = renal_data[renal_data['concept_name'].str.contains('urine|foley', case=False, na=False)]
        
        creatinine_level = None
        urine_output_24h = None
        
        if not creatinine_data.empty:
            creatinine_level = creatinine_data.iloc[-1]['valuenum_std']
        
        if not urine_data.empty:
            # Sum urine output over the window (approximate 24h)
            urine_output_24h = urine_data['valuenum_std'].sum()
        
        if pd.isna(creatinine_level) and pd.isna(urine_output_24h):
            return 0, False, {}
        
        # Score based on creatinine and urine output
        data_available = True
        supporting_data = {'creatinine': creatinine_level, 'urine_24h': urine_output_24h}
        
        # Prioritize creatinine if available
        if pd.notna(creatinine_level):
            if creatinine_level >= 5.0 or (pd.notna(urine_output_24h) and urine_output_24h < 200):
                return 4, data_available, supporting_data
            elif creatinine_level >= 3.5 or (pd.notna(urine_output_24h) and urine_output_24h < 500):
                return 3, data_available, supporting_data
            elif creatinine_level >= 2.0:
                return 2, data_available, supporting_data
            elif creatinine_level >= 1.2:
                return 1, data_available, supporting_data
            else:
                return 0, data_available, supporting_data
        
        # If only urine output available
        elif pd.notna(urine_output_24h):
            if urine_output_24h < 200:
                return 4, data_available, supporting_data
            elif urine_output_24h < 500:
                return 3, data_available, supporting_data
            else:
                return 0, data_available, supporting_data
        
        return 0, data_available, supporting_data

    def calculate_neurological_score(self, data_window: pd.DataFrame) -> Tuple[int, bool, Dict]:
        """Calculate neurological SOFA score (0-4)"""
        neuro_data = data_window[data_window['sofa_system'] == 'cns']
        
        if neuro_data.empty:
            return 0, False, {}
        
        # Calculate total GCS from components
        gcs_components = {}
        gcs_data = neuro_data[neuro_data['concept_name'].str.contains('GCS', case=False, na=False)]
        
        for _, row in gcs_data.iterrows():
            concept_name = row['concept_name']
            value = row['valuenum_std']
            
            if pd.notna(value):
                if 'eye' in concept_name.lower():
                    gcs_components['eye'] = value
                elif 'verbal' in concept_name.lower():
                    gcs_components['verbal'] = value
                elif 'motor' in concept_name.lower():
                    gcs_components['motor'] = value
        
        # Calculate total GCS
        if len(gcs_components) == 3:
            gcs_total = sum(gcs_components.values())
        else:
            return 0, False, {}
        
        # Score based on GCS total
        if gcs_total <= 5:
            return 4, True, {'gcs_total': gcs_total, 'gcs_components': gcs_components}
        elif gcs_total <= 9:
            return 3, True, {'gcs_total': gcs_total, 'gcs_components': gcs_components}
        elif gcs_total <= 12:
            return 2, True, {'gcs_total': gcs_total, 'gcs_components': gcs_components}
        elif gcs_total <= 14:
            return 1, True, {'gcs_total': gcs_total, 'gcs_components': gcs_components}
        else:
            return 0, True, {'gcs_total': gcs_total, 'gcs_components': gcs_components}

    def calculate_sofa_for_window(self, subject_id: int, stay_id: int, window_data: pd.DataFrame, charttime: datetime) -> Dict:
        """Calculate complete SOFA score for a time window"""
        
        # Calculate individual organ system scores
        resp_score, resp_available, resp_data = self.calculate_respiratory_score(window_data)
        cardio_score, cardio_available, cardio_data = self.calculate_cardiovascular_score(window_data)
        hepatic_score, hepatic_available, hepatic_data = self.calculate_hepatic_score(window_data)
        coag_score, coag_available, coag_data = self.calculate_coagulation_score(window_data)
        renal_score, renal_available, renal_data = self.calculate_renal_score(window_data)
        neuro_score, neuro_available, neuro_data = self.calculate_neurological_score(window_data)
        
        # Calculate total SOFA score
        total_sofa = resp_score + cardio_score + hepatic_score + coag_score + renal_score + neuro_score
        
        # Calculate data completeness score
        available_systems = sum([resp_available, cardio_available, hepatic_available, 
                                coag_available, renal_available, neuro_available])
        completeness = available_systems / 6.0
        
        return {
            'subject_id': subject_id,
            'stay_id': stay_id,
            'charttime': charttime,
            'window_start': charttime - timedelta(hours=24),
            'window_end': charttime,
            
            'respiratory_score': resp_score,
            'cardiovascular_score': cardio_score,
            'hepatic_score': hepatic_score,
            'coagulation_score': coag_score,
            'renal_score': renal_score,
            'neurological_score': neuro_score,
            'total_sofa_score': total_sofa,
            
            'pao2_fio2_ratio': resp_data.get('pao2_fio2'),
            'mean_arterial_pressure': cardio_data.get('map'),
            'vasopressor_doses': json.dumps(cardio_data.get('vasopressors', {})),
            'bilirubin_level': hepatic_data.get('bilirubin'),
            'platelet_count': coag_data.get('platelets'),
            'creatinine_level': renal_data.get('creatinine'),
            'urine_output_24h': renal_data.get('urine_24h'),
            'gcs_total': neuro_data.get('gcs_total'),
            
            'respiratory_data_available': resp_available,
            'cardiovascular_data_available': cardio_available,
            'hepatic_data_available': hepatic_available,
            'coagulation_data_available': coag_available,
            'renal_data_available': renal_available,
            'neurological_data_available': neuro_available,
            
            'data_completeness_score': completeness
        }

    def calculate_sofa_scores(self):
        """Calculate SOFA scores for all patients and time windows"""
        self.logger.info("🧮 Calculating SOFA scores for all patients...")
        
        # Load Silver layer data
        query = """
        SELECT 
            subject_id, stay_id, charttime, concept_name, valuenum_std,
            sofa_system, quality_flags
        FROM silver.collection_disease_std
        WHERE valuenum_std IS NOT NULL
        ORDER BY subject_id, stay_id, charttime
        """
        
        df = pd.read_sql(query, self.engine)
        self.logger.info(f"📊 Loaded {len(df):,} Silver layer records")
        
        # Group by patient and stay
        sofa_scores = []
        patients_processed = 0
        
        for (subject_id, stay_id), group in df.groupby(['subject_id', 'stay_id']):
            patients_processed += 1
            
            if patients_processed % 10 == 0:
                self.logger.info(f"   Processing patient {patients_processed}...")
            
            # Get time range for this stay
            min_time = group['charttime'].min()
            max_time = group['charttime'].max()
            
            # Generate 24-hour windows (daily SOFA scoring)
            current_time = min_time + timedelta(hours=24)
            while current_time <= max_time:
                window_start = current_time - timedelta(hours=24)
                window_data = group[
                    (group['charttime'] >= window_start) & 
                    (group['charttime'] <= current_time)
                ]
                
                if not window_data.empty:
                    sofa_result = self.calculate_sofa_for_window(
                        subject_id, stay_id, window_data, current_time
                    )
                    sofa_scores.append(sofa_result)
                    self.processing_stats['time_windows_processed'] += 1
                
                current_time += timedelta(hours=24)
        
        self.processing_stats['total_patients'] = patients_processed
        self.processing_stats['sofa_scores_calculated'] = len(sofa_scores)
        
        self.logger.info(f"✅ Calculated {len(sofa_scores):,} SOFA scores for {patients_processed} patients")
        
        return pd.DataFrame(sofa_scores)

    def write_sofa_scores(self, sofa_df: pd.DataFrame):
        """Write SOFA scores to Gold layer"""
        self.logger.info("💾 Writing SOFA scores to Gold layer...")
        
        # Write to database
        sofa_df.to_sql(
            'sofa_scores',
            self.engine,
            schema='gold',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        self.logger.info(f"✅ Successfully wrote {len(sofa_df):,} SOFA scores to Gold layer")

    def generate_sofa_report(self):
        """Generate comprehensive SOFA calculation report"""
        self.logger.info("📋 Generating SOFA calculation report...")
        
        duration = datetime.now() - self.processing_stats['start_time']
        
        # Get statistics from Gold layer
        cur = self.conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM gold.sofa_scores")
        total_scores = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(DISTINCT subject_id) FROM gold.sofa_scores")
        unique_patients = cur.fetchone()[0]
        
        cur.execute("SELECT AVG(total_sofa_score) FROM gold.sofa_scores")
        avg_sofa = cur.fetchone()[0]
        
        cur.execute("SELECT AVG(data_completeness_score) FROM gold.sofa_scores")
        avg_completeness = cur.fetchone()[0]
        
        # High-risk patients (SOFA >= 10)
        cur.execute("SELECT COUNT(*) FROM gold.sofa_scores WHERE total_sofa_score >= 10")
        high_risk_scores = cur.fetchone()[0]
        
        report = []
        report.append("# Enhanced SOFA Calculation Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Processing Duration: {duration}")
        report.append("")
        
        report.append("## SOFA Calculation Summary")
        report.append(f"- **Total SOFA Scores Calculated**: {total_scores:,}")
        report.append(f"- **Unique Patients**: {unique_patients:,}")
        report.append(f"- **Time Windows Processed**: {self.processing_stats['time_windows_processed']:,}")
        report.append(f"- **Average SOFA Score**: {avg_sofa:.2f}")
        report.append(f"- **Average Data Completeness**: {avg_completeness:.1%}")
        report.append("")
        
        report.append("## Risk Stratification")
        report.append(f"- **High-Risk Scores (≥10)**: {high_risk_scores:,} ({high_risk_scores/total_scores*100:.1f}%)")
        report.append("")
        
        # Organ system statistics
        cur.execute("""
            SELECT 
                ROUND(AVG(respiratory_score), 2) as avg_resp,
                ROUND(AVG(cardiovascular_score), 2) as avg_cardio,
                ROUND(AVG(hepatic_score), 2) as avg_hepatic,
                ROUND(AVG(coagulation_score), 2) as avg_coag,
                ROUND(AVG(renal_score), 2) as avg_renal,
                ROUND(AVG(neurological_score), 2) as avg_neuro
            FROM gold.sofa_scores
        """)
        avg_scores = cur.fetchone()
        
        report.append("## Average Organ System Scores")
        report.append(f"- **Respiratory**: {avg_scores[0]}")
        report.append(f"- **Cardiovascular**: {avg_scores[1]}")
        report.append(f"- **Hepatic**: {avg_scores[2]}")
        report.append(f"- **Coagulation**: {avg_scores[3]}")
        report.append(f"- **Renal**: {avg_scores[4]}")
        report.append(f"- **Neurological**: {avg_scores[5]}")
        
        # Import file_paths utility and write report to docs/reports
        import sys
        import os
        sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
        from file_paths import get_report_path
        
        report_path = get_report_path('sofa_calculation_report.md')
        with open(report_path, 'w') as f:
            f.write('\n'.join(report))
        
        self.logger.info(f"📋 SOFA calculation report saved: {report_path}")

    def build_gold_layer(self):
        """Build complete Gold layer with SOFA scoring"""
        self.logger.info("🚀 Starting enhanced Gold layer build with SOFA scoring...")
        
        try:
            # Create schema and tables
            self.create_gold_schema()
            
            # Calculate SOFA scores
            sofa_df = self.calculate_sofa_scores()
            
            # Write to Gold layer
            self.write_sofa_scores(sofa_df)
            
            # Generate report
            self.generate_sofa_report()
            
            self.logger.info(f"✅ Gold layer build completed successfully!")
            self.logger.info(f"📊 Total SOFA scores: {len(sofa_df):,}")
            
            return len(sofa_df)
            
        except Exception as e:
            self.logger.error(f"❌ Gold layer build failed: {e}", exc_info=True)
            raise

def main():
    """Main execution function"""
    print("🚀 Starting Enhanced Gold Layer Build with SOFA Scoring...")
    print("=" * 70)
    
    try:
        calculator = EnhancedSOFACalculator()
        total_scores = calculator.build_gold_layer()
        
        print("✅ Gold layer build completed successfully!")
        print(f"📊 Results:")
        print(f"   - Total SOFA scores calculated: {total_scores:,}")
        print(f"   - Comprehensive organ system coverage")
        print(f"   - Clinical analytics and dashboards created")
        print("")
        print("📋 Reports generated:")
        print("   - sofa_calculation_report.md")
        print("   - sofa_calculator.log")
        
        return 0
        
    except Exception as e:
        print(f"❌ Gold layer build failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
