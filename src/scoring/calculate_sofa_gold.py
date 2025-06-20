#!/usr/bin/env python3
"""
Gold Layer SOFA Score Calculation Pipeline
==========================================

This script implements the Gold Layer ETL pipeline for calculating SOFA scores
from Silver layer standardized data. It processes patients in 24-hour windows
and calculates Sequential Organ Failure Assessment (SOFA) scores for ICU patients.

Key Features:
- 24-hour windowing from ICU admission
- ARI patient identification via ICD codes
- SOFA subscore calculation for all 6 organ systems
- Intelligent imputation with LOCF and population medians
- Comprehensive logging and validation

Author: Medical Data Science Team
Date: 2025-06-04
"""

import sys
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import warnings
warnings.filterwarnings('ignore')

# Import configurations and mappings
from config_gold import *
from sofa_mappings import SOFAMappings, calculate_total_sofa, get_sofa_severity_category

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging():
    """Configure logging for the Gold layer pipeline"""
    logger = logging.getLogger('gold_sofa')
    logger.setLevel(getattr(logging, LOGGING_CONFIG['log_level']))
    
    # Clear existing handlers
    logger.handlers.clear()
    
    formatter = logging.Formatter(LOGGING_CONFIG['log_format'])
    
    # Console handler
    if LOGGING_CONFIG['console_output']:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler  
    if LOGGING_CONFIG['file_output']:
        file_handler = logging.FileHandler(LOGGING_CONFIG['log_file'])
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

class DatabaseConnection:
    """Manages database connections and operations"""
    
    def __init__(self, logger):
        self.logger = logger
        self.engine = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        try:
            # Build connection string
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
            
            self.engine = create_engine(conn_string)
            
            # Test connection
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            
            self.logger.info("✅ Database connection established")
            
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            raise
    
    def execute_query(self, query, params=None):
        """Execute a query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return result
        except Exception as e:
            self.logger.error(f"❌ Query execution failed: {e}")
            raise
    
    def read_dataframe(self, query, params=None):
        """Execute query and return pandas DataFrame"""
        try:
            return pd.read_sql_query(text(query), self.engine, params=params)
        except Exception as e:
            self.logger.error(f"❌ DataFrame read failed: {e}")
            raise

# =============================================================================
# ARI PATIENT IDENTIFICATION
# =============================================================================

class ARIPatientIdentifier:
    """Identifies ARI patients using ICD codes"""
    
    def __init__(self, db_conn, logger):
        self.db_conn = db_conn
        self.logger = logger
        self.ari_patients = set()
    
    def identify_ari_patients(self):
        """Identify patients with ARI diagnosis"""
        self.logger.info("🔍 Identifying ARI patients...")
        
        # Build ICD code filter
        icd_codes = ARI_IDENTIFICATION['icd_codes']
        icd_placeholders = ','.join([f"'{code}'" for code in icd_codes])
        
        query = f"""
        SELECT DISTINCT subject_id, hadm_id
        FROM {DIAGNOSES_TABLE} 
        WHERE (icd_code IN ({icd_placeholders}) AND icd_version = 9)
           OR (icd_code IN ({icd_placeholders}) AND icd_version = 10)
        """
        
        try:
            result = self.db_conn.read_dataframe(query)
            self.ari_patients = set(result['subject_id'].tolist())
            
            self.logger.info(f"📊 Found {len(self.ari_patients)} ARI patients")
            self.logger.info(f"📋 ARI admissions: {len(result)} hospital admissions")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ ARI identification failed: {e}")
            raise
    
    def is_ari_patient(self, subject_id):
        """Check if patient has ARI diagnosis"""
        return subject_id in self.ari_patients

# =============================================================================
# TIME WINDOWING
# =============================================================================

class TimeWindowing:
    """Handles 24-hour time windowing for SOFA calculations"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def get_icu_stays(self, db_conn):
        """Get ICU stay information"""
        query = f"""
        SELECT subject_id, hadm_id, stay_id, intime, outtime,
               EXTRACT(EPOCH FROM (outtime - intime))/3600 as stay_duration_hours
        FROM {ICU_STAYS_TABLE}
        WHERE EXTRACT(EPOCH FROM (outtime - intime))/3600 >= {QUALITY_FILTERS['min_stay_duration_hours']}
          AND EXTRACT(EPOCH FROM (outtime - intime))/3600 <= {QUALITY_FILTERS['max_stay_duration_days'] * 24}
        ORDER BY subject_id, intime
        """
        
        return db_conn.read_dataframe(query)
    
    def generate_windows(self, stay_info):
        """Generate 24-hour windows for an ICU stay"""
        windows = []
        
        intime = stay_info['intime']
        outtime = stay_info['outtime']
        
        window_start = intime
        window_number = 1
        
        while window_start < outtime and window_number <= WINDOWING_CONFIG['max_windows_per_stay']:
            window_end = window_start + timedelta(hours=WINDOWING_CONFIG['window_duration_hours'])
            
            # Don't extend beyond ICU discharge
            if window_end > outtime:
                window_end = outtime
            
            # Calculate ICU day (1-based)
            icu_day = window_number
            
            windows.append({
                'subject_id': stay_info['subject_id'],
                'hadm_id': stay_info['hadm_id'], 
                'stay_id': stay_info['stay_id'],
                'window_start': window_start,
                'window_end': window_end,
                'window_number': window_number,
                'icu_day': icu_day
            })
            
            window_start = window_end
            window_number += 1
        
        return windows

# =============================================================================
# SOFA DATA EXTRACTOR
# =============================================================================

class SOFADataExtractor:
    """Extracts and aggregates data for SOFA calculations"""
    
    def __init__(self, db_conn, logger):
        self.db_conn = db_conn
        self.logger = logger
    
    def extract_window_data(self, window):
        """Extract all measurements for a specific time window"""
        query = f"""
        SELECT 
            concept_id,
            concept_name,
            value,
            valueuom as standard_unit,
            charttime,
            is_outlier
        FROM {SILVER_TABLE}
        WHERE subject_id = :subject_id
          AND stay_id = :stay_id
          AND charttime >= :window_start
          AND charttime < :window_end
          AND is_outlier = FALSE
        ORDER BY charttime
        """
        
        params = {
            'subject_id': int(window['subject_id']),
            'stay_id': int(window['stay_id']),
            'window_start': window['window_start'],
            'window_end': window['window_end']
        }
        
        return self.db_conn.read_dataframe(query, params)
    
    def aggregate_sofa_parameters(self, data, window):
        """Aggregate measurements according to SOFA rules"""
        aggregated = {}
        
        for system, parameters in SOFA_AGGREGATION_RULES.items():
            for param_name, config in parameters.items():
                omop_concept = config['omop_concept']
                agg_func = config['aggregation']
                
                # Filter data for this parameter
                param_data = data[data['concept_id'] == omop_concept]
                
                if not param_data.empty:
                    values = param_data['value'].dropna()
                    
                    if not values.empty:
                        if agg_func == 'min':
                            aggregated[param_name] = values.min()
                        elif agg_func == 'max':
                            aggregated[param_name] = values.max()
                        elif agg_func == 'mean':
                            aggregated[param_name] = values.mean()
                        elif agg_func == 'sum':
                            aggregated[param_name] = values.sum()
                        else:
                            aggregated[param_name] = values.iloc[-1]  # Last value
                
        return aggregated

# =============================================================================
# IMPUTATION ENGINE
# =============================================================================

class ImputationEngine:
    """Handles missing data imputation for SOFA parameters"""
    
    def __init__(self, db_conn, logger):
        self.db_conn = db_conn
        self.logger = logger
        self.population_medians = {}
    
    def calculate_population_medians(self, ari_patients):
        """Calculate population medians for ARI patients"""
        self.logger.info("📊 Calculating population medians for imputation...")
        
        # If no ARI patients found, use all patients for population medians
        if not ari_patients:
            self.logger.warning("⚠️ No ARI patients found, using all patients for population medians")
            query = f"""
            SELECT concept_id, concept_name,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value
            FROM {SILVER_TABLE}
            WHERE is_outlier = FALSE
              AND value IS NOT NULL
            GROUP BY concept_id, concept_name
            HAVING COUNT(*) >= {IMPUTATION_CONFIG['population_median']['min_sample_size']}
            """
        else:
            # Get all SOFA-relevant parameters for ARI patients
            ari_patient_list = ','.join([str(p) for p in ari_patients])
            
            query = f"""
            SELECT concept_id, concept_name,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value) as median_value
            FROM {SILVER_TABLE}
            WHERE subject_id IN ({ari_patient_list})
              AND is_outlier = FALSE
              AND value IS NOT NULL
            GROUP BY concept_id, concept_name
            HAVING COUNT(*) >= {IMPUTATION_CONFIG['population_median']['min_sample_size']}
            """
        
        result = self.db_conn.read_dataframe(query)
        
        for _, row in result.iterrows():
            self.population_medians[row['concept_id']] = row['median_value']
        
        self.logger.info(f"📈 Calculated medians for {len(self.population_medians)} parameters")
    
    def apply_locf_imputation(self, param_name, window, omop_concept):
        """Apply Last Observation Carried Forward imputation"""
        if not IMPUTATION_CONFIG['locf']['enabled']:
            return None, False
        
        # Look back for previous values
        lookback_hours = IMPUTATION_CONFIG['locf']['max_lookback_hours']
        lookback_start = window['window_start'] - timedelta(hours=lookback_hours)
        
        query = f"""
        SELECT value, charttime
        FROM {SILVER_TABLE}
        WHERE subject_id = :subject_id
          AND stay_id = :stay_id
          AND concept_id = :omop_concept
          AND charttime >= :lookback_start
          AND charttime < :window_start
          AND is_outlier = FALSE
          AND value IS NOT NULL
        ORDER BY charttime DESC
        LIMIT 1
        """
        
        params = {
            'subject_id': int(window['subject_id']),
            'stay_id': int(window['stay_id']),
            'omop_concept': omop_concept,
            'lookback_start': lookback_start,
            'window_start': window['window_start']
        }
        
        try:
            result = self.db_conn.read_dataframe(query, params)
            if not result.empty:
                return result.iloc[0]['value'], True
        except Exception as e:
            self.logger.warning(f"⚠️ LOCF failed for {param_name}: {e}")
        
        return None, False
    
    def apply_population_imputation(self, omop_concept):
        """Apply population median imputation"""
        if omop_concept in self.population_medians:
            return self.population_medians[omop_concept], True
        return None, False
    
    def calculate_spo2_surrogate(self, spo2, fio2):
        """Calculate SpO2/FiO2 as surrogate for PaO2/FiO2"""
        if spo2 is not None and fio2 is not None and fio2 > 0:
            # Convert FiO2 to decimal if it's a percentage
            if fio2 > 1:
                fio2 = fio2 / 100
            
            # SpO2/FiO2 ratio calculation
            surrogate_ratio = spo2 / fio2
            return surrogate_ratio, True
        
        return None, False

# =============================================================================
# SOFA CALCULATOR
# =============================================================================

class SOFACalculator:
    """Calculates SOFA scores from aggregated and imputed data"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def calculate_window_sofa(self, window, aggregated_data, imputation_flags):
        """Calculate SOFA scores for a single window"""
        
        # Initialize result structure
        result = {
            # Patient and window info
            'subject_id': window['subject_id'],
            'hadm_id': window['hadm_id'],
            'stay_id': window['stay_id'],
            'window_start': window['window_start'],
            'window_end': window['window_end'],
            'window_number': window['window_number'],
            'icu_day': window['icu_day'],
            
            # Raw values (will be filled)
            'pao2': None, 'fio2': None, 'pao2_fio2_ratio': None,
            'spo2': None, 'spo2_fio2_ratio': None,
            'is_mechanically_ventilated': False,
            'map': None, 'platelets': None, 'bilirubin': None,
            'gcs_total': None, 'creatinine': None, 'urine_output_24h': None,
            
            # Imputation flags (will be filled)
            'pao2_fio2_imputed': False, 'spo2_fio2_surrogate': False,
            'map_imputed': False, 'platelets_imputed': False,
            'bilirubin_imputed': False, 'gcs_imputed': False,
            'creatinine_imputed': False, 'urine_output_imputed': False,
            
            # SOFA subscores (will be calculated)
            'sofa_respiratory_subscore': None,
            'sofa_cardiovascular_subscore': None,
            'sofa_coagulation_subscore': None,
            'sofa_liver_subscore': None,
            'sofa_cns_subscore': None,
            'sofa_renal_subscore': None,
            
            # SOFA total and metadata
            'sofa_total': None,
            'sofa_severity': None,
            'missing_components': [],
            'calculation_notes': []
        }
        
        # Copy raw values and imputation flags
        for key in aggregated_data:
            if key in result:
                result[key] = aggregated_data[key]
        
        for key in imputation_flags:
            if key in result:
                result[key] = imputation_flags[key]
        
        # Calculate PaO2/FiO2 ratio
        if result['pao2'] is not None and result['fio2'] is not None:
            fio2_decimal = result['fio2'] / 100 if result['fio2'] > 1 else result['fio2']
            if fio2_decimal > 0:
                result['pao2_fio2_ratio'] = result['pao2'] / fio2_decimal
        
        # Calculate SpO2/FiO2 ratio if needed
        if result['spo2'] is not None and result['fio2'] is not None:
            fio2_decimal = result['fio2'] / 100 if result['fio2'] > 1 else result['fio2']
            if fio2_decimal > 0:
                result['spo2_fio2_ratio'] = result['spo2'] / fio2_decimal
        
        # Calculate SOFA subscores
        subscores = {}
        
        # Respiratory
        ratio_for_resp = result['pao2_fio2_ratio']
        if ratio_for_resp is None and result['spo2_fio2_ratio'] is not None:
            ratio_for_resp = result['spo2_fio2_ratio']
            result['spo2_fio2_surrogate'] = True
        
        resp_score, resp_desc = SOFAMappings.respiratory_score(
            ratio_for_resp, result['is_mechanically_ventilated']
        )
        result['sofa_respiratory_subscore'] = resp_score
        subscores['respiratory'] = resp_score
        if resp_score is not None:
            result['calculation_notes'].append(f"Respiratory: {resp_desc}")
        
        # Cardiovascular
        vasopressor_doses = {
            'dopamine': result.get('dopamine_dose'),
            'epinephrine': result.get('epinephrine_dose'),
            'norepinephrine': result.get('norepinephrine_dose'),
            'dobutamine': result.get('dobutamine_dose')
        }
        cardio_score, cardio_desc = SOFAMappings.cardiovascular_score(
            result['map'], vasopressor_doses
        )
        result['sofa_cardiovascular_subscore'] = cardio_score
        subscores['cardiovascular'] = cardio_score
        if cardio_score is not None:
            result['calculation_notes'].append(f"Cardiovascular: {cardio_desc}")
        
        # Coagulation
        coag_score, coag_desc = SOFAMappings.coagulation_score(result['platelets'])
        result['sofa_coagulation_subscore'] = coag_score
        subscores['coagulation'] = coag_score
        if coag_score is not None:
            result['calculation_notes'].append(f"Coagulation: {coag_desc}")
        
        # Liver
        liver_score, liver_desc = SOFAMappings.liver_score(result['bilirubin'])
        result['sofa_liver_subscore'] = liver_score
        subscores['liver'] = liver_score
        if liver_score is not None:
            result['calculation_notes'].append(f"Liver: {liver_desc}")
        
        # CNS
        cns_score, cns_desc = SOFAMappings.cns_score(result['gcs_total'])
        result['sofa_cns_subscore'] = cns_score
        subscores['cns'] = cns_score
        if cns_score is not None:
            result['calculation_notes'].append(f"CNS: {cns_desc}")
        
        # Renal
        renal_score, renal_desc = SOFAMappings.renal_score(
            result['creatinine'], result['urine_output_24h']
        )
        result['sofa_renal_subscore'] = renal_score
        subscores['renal'] = renal_score
        if renal_score is not None:
            result['calculation_notes'].append(f"Renal: {renal_desc}")
        
        # Calculate total SOFA
        total_score, missing_components = calculate_total_sofa(subscores)
        result['sofa_total'] = total_score
        result['missing_components'] = missing_components
        result['sofa_severity'] = get_sofa_severity_category(total_score)
        
        # Join calculation notes
        result['calculation_notes'] = '; '.join(result['calculation_notes'])
        
        return result

# =============================================================================
# MAIN PIPELINE
# =============================================================================

class SOFAPipeline:
    """Main SOFA calculation pipeline orchestrator"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.db_conn = DatabaseConnection(self.logger)
        self.ari_identifier = ARIPatientIdentifier(self.db_conn, self.logger)
        self.windowing = TimeWindowing(self.logger)
        self.extractor = SOFADataExtractor(self.db_conn, self.logger)
        self.imputation = ImputationEngine(self.db_conn, self.logger)
        self.calculator = SOFACalculator(self.logger)
        
        self.ari_patients = set()
        self.processed_windows = 0
        self.total_windows = 0
    
    def create_gold_schema(self):
        """Create Gold schema and table if they don't exist"""
        self.logger.info("🏗️ Creating Gold schema and table...")
        
        try:
            # Create schema
            self.db_conn.execute_query(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
            self.logger.info(f"✅ Schema {GOLD_SCHEMA} created/verified")
            
            # Create table
            create_sql = get_gold_table_create_sql()
            self.db_conn.execute_query(create_sql)
            self.logger.info(f"✅ Table {GOLD_FULL_TABLE_NAME} created/verified")
            
        except Exception as e:
            self.logger.error(f"❌ Schema/table creation failed: {e}")
            raise
    
    def run_pipeline(self):
        """Execute the complete SOFA calculation pipeline"""
        start_time = datetime.now()
        self.logger.info("🚀 Starting Gold Layer SOFA calculation pipeline...")
        
        try:
            # Step 1: Create Gold schema and table
            self.create_gold_schema()
            
            # Step 2: Identify ARI patients
            ari_df = self.ari_identifier.identify_ari_patients()
            self.ari_patients = set(ari_df['subject_id'].tolist())
            
            # Step 3: Calculate population medians for imputation
            self.imputation.calculate_population_medians(self.ari_patients)
            
            # Step 4: Get ICU stays
            self.logger.info("🏥 Loading ICU stay information...")
            icu_stays = self.windowing.get_icu_stays(self.db_conn)
            self.logger.info(f"📊 Found {len(icu_stays)} ICU stays to process")
            
            # Step 5: Process each ICU stay
            all_results = []
            
            for idx, stay in icu_stays.iterrows():
                # Generate windows for this stay
                windows = self.windowing.generate_windows(stay)
                self.total_windows += len(windows)
                
                # Determine disease type
                disease_type = 'ARI' if self.ari_identifier.is_ari_patient(stay['subject_id']) else 'OTHER'
                
                # Process each window
                for window in windows:
                    try:
                        # Extract measurements for this window
                        window_data = self.extractor.extract_window_data(window)
                        
                        # Skip if insufficient data
                        if len(window_data) < WINDOWING_CONFIG['min_measurements_per_window']:
                            continue
                        
                        # Aggregate SOFA parameters
                        aggregated = self.extractor.aggregate_sofa_parameters(window_data, window)
                        
                        # Apply imputation
                        imputation_flags = self.apply_imputation(window, aggregated)
                        
                        # Calculate SOFA scores
                        result = self.calculator.calculate_window_sofa(window, aggregated, imputation_flags)
                        result['disease_type'] = disease_type
                        
                        # Check if we have enough data (skip if too many missing components)
                        missing_count = len(result['missing_components'])
                        if missing_count > IMPUTATION_CONFIG['missing_data']['max_missing_components']:
                            self.logger.debug(f"⏭️ Skipping window with {missing_count}/6 missing SOFA components")
                            continue
                        
                        all_results.append(result)
                        self.processed_windows += 1
                        
                    except Exception as e:
                        self.logger.warning(f"⚠️ Window processing failed: {e}")
                        continue
                
                # Progress reporting
                if (idx + 1) % BATCH_CONFIG['batch_size'] == 0:
                    self.logger.info(f"📈 Processed {idx + 1}/{len(icu_stays)} ICU stays, {self.processed_windows} windows")
            
            # Step 6: Save results to database
            if all_results:
                self.save_results(all_results)
            
            # Step 7: Generate summary
            duration = datetime.now() - start_time
            self.generate_summary(duration)
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline failed: {e}")
            raise
    
    def apply_imputation(self, window, aggregated):
        """Apply imputation strategies for missing values"""
        imputation_flags = {}
        
        # Process each SOFA parameter
        for system, parameters in SOFA_AGGREGATION_RULES.items():
            for param_name, config in parameters.items():
                flag_name = f"{param_name}_imputed"
                omop_concept = config['omop_concept']
                
                if param_name not in aggregated or aggregated[param_name] is None:
                    # Try LOCF imputation
                    value, imputed = self.imputation.apply_locf_imputation(param_name, window, omop_concept)
                    
                    if value is not None:
                        aggregated[param_name] = value
                        imputation_flags[flag_name] = imputed
                    else:
                        # Try population median
                        value, imputed = self.imputation.apply_population_imputation(omop_concept)
                        if value is not None:
                            aggregated[param_name] = value
                            imputation_flags[flag_name] = imputed
                        else:
                            imputation_flags[flag_name] = False
                else:
                    imputation_flags[flag_name] = False
        
        # Special handling for SpO2/FiO2 surrogate
        if ('pao2' not in aggregated or aggregated['pao2'] is None) and \
           ('spo2' in aggregated and aggregated['spo2'] is not None) and \
           ('fio2' in aggregated and aggregated['fio2'] is not None):
            
            surrogate_ratio, is_surrogate = self.imputation.calculate_spo2_surrogate(
                aggregated['spo2'], aggregated['fio2']
            )
            if surrogate_ratio is not None:
                imputation_flags['spo2_fio2_surrogate'] = True
        
        return imputation_flags
    
    def save_results(self, results):
        """Save SOFA calculation results to database"""
        self.logger.info(f"💾 Saving {len(results)} SOFA calculation results...")
        
        try:
            # Convert results to DataFrame
            df = pd.DataFrame(results)
            
            # Handle array/list columns
            df['missing_components'] = df['missing_components'].apply(
                lambda x: '{' + ','.join(x) + '}' if x else '{}'
            )
            
            # Save to database
            df.to_sql(
                GOLD_TABLE_NAME,
                self.db_conn.engine,
                schema=GOLD_SCHEMA,
                if_exists='replace',  # For now, replace existing data
                index=False,
                method='multi'
            )
            
            self.logger.info(f"✅ Successfully saved {len(results)} SOFA calculations")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to save results: {e}")
            raise
    
    def generate_summary(self, duration):
        """Generate pipeline execution summary"""
        self.logger.info("📊 Generating pipeline summary...")
        
        try:
            # Basic statistics
            summary = {
                'execution_time': str(duration),
                'total_windows_generated': self.total_windows,
                'windows_processed': self.processed_windows,
                'ari_patients_identified': len(self.ari_patients),
                'processing_rate': f"{self.processed_windows / duration.total_seconds():.2f} windows/second"
            }
            
            # Query Gold table for additional statistics
            stats_query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT subject_id) as unique_patients,
                COUNT(DISTINCT stay_id) as unique_stays,
                AVG(sofa_total) as avg_sofa_score,
                MIN(sofa_total) as min_sofa_score,
                MAX(sofa_total) as max_sofa_score,
                COUNT(CASE WHEN disease_type = 'ARI' THEN 1 END) as ari_windows,
                COUNT(CASE WHEN disease_type = 'OTHER' THEN 1 END) as other_windows
            FROM {GOLD_FULL_TABLE_NAME}
            """
            
            stats = self.db_conn.read_dataframe(stats_query)
            
            if not stats.empty:
                summary.update(stats.iloc[0].to_dict())
            
            # Log summary
            self.logger.info("🎯 GOLD LAYER SOFA CALCULATION SUMMARY:")
            self.logger.info("=" * 50)
            for key, value in summary.items():
                self.logger.info(f"{key.replace('_', ' ').title()}: {value}")
            self.logger.info("=" * 50)
            
            self.logger.info("✅ Gold Layer SOFA calculation pipeline completed successfully!")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Summary generation failed: {e}")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main execution function"""
    try:
        pipeline = SOFAPipeline()
        pipeline.run_pipeline()
        return 0
    except Exception as e:
        print(f"❌ Gold Layer pipeline failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
