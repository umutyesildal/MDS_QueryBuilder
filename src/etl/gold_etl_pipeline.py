#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold Layer ETL Pipeline for Task 5.4 Dual Configurations
========================================================

This module implements the complete ETL pipeline that:
1. Loads data from Silver layer
2. Applies configuration-specific processing
3. Calculates clinical scores (APACHE-II, SOFA, SAPS-II, OASIS)
4. Populates gold_scores_config1 and gold_scores_config2 tables

Author: Medical Data Science Team
Date: 2025-06-17
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import sys
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from etl_configurations import *
from config_local import DB_CONFIG

class GoldETLPipeline:
    """ETL Pipeline for Gold layer score calculations with dual configurations"""
    
    def __init__(self, config):
        self.config = config
        self.logger = self._setup_logging()
        self.engine = self._create_engine()
        self.stats = {
            'patients_processed': 0,
            'measurements_processed': 0,
            'scores_calculated': 0,
            'quality_issues': 0
        }
    
    def _setup_logging(self):
        """Setup logging for ETL pipeline"""
        logger = logging.getLogger(f'GoldETL_{self.config["name"]}')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        
        if not logger.handlers:
            logger.addHandler(handler)
        
        return logger
    
    def _create_engine(self):
        """Create database engine"""
        if DB_CONFIG.get('password'):
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        else:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        return create_engine(connection_string)
    
    def load_silver_data(self):
        """Load and prepare data from Silver layer"""
        self.logger.info("📊 Loading data from Silver layer...")
        
        query = """
        SELECT 
            subject_id,
            hadm_id,
            stay_id,
            charttime,
            concept_name,
            valuenum_std as value,
            unit_std as unit,
            sofa_system,
            quality_flags::text as quality_flags
        FROM silver.collection_disease_std
        WHERE valuenum_std IS NOT NULL
        ORDER BY subject_id, stay_id, charttime
        """
        
        df = pd.read_sql(query, self.engine)
        self.stats['measurements_processed'] = len(df)
        
        self.logger.info(f"✅ Loaded {len(df):,} measurements from Silver layer")
        return df
    
    def aggregate_measurements(self, df):
        """Apply configuration-specific aggregation"""
        self.logger.info(f"🔧 Applying {self.config['aggregation_method']} aggregation...")
        
        # Create time windows based on configuration
        df['time_window'] = df.groupby(['subject_id', 'stay_id'])['charttime'].transform(
            lambda x: pd.cut(x, bins=pd.date_range(x.min(), x.max() + timedelta(hours=1), 
                           freq=f"{self.config['time_window_hours']}H"), 
                           include_lowest=True, duplicates='drop')
        )
        
        # Apply outlier removal based on configuration
        if self.config['outlier_handling'] == 'iqr':
            df = self._remove_outliers_iqr(df)
        elif self.config['outlier_handling'] == 'percentile':
            df = self._remove_outliers_percentile(df)
        
        # Group by patient, stay, time window, and concept
        group_cols = ['subject_id', 'hadm_id', 'stay_id', 'time_window', 'concept_name']
        
        # Apply aggregation method
        if self.config['aggregation_method'] == 'mean':
            aggregated = df.groupby(group_cols).agg({
                'value': 'mean',
                'charttime': 'mean'  # Take middle time of window
            }).reset_index()
        elif self.config['aggregation_method'] == 'median':
            aggregated = df.groupby(group_cols).agg({
                'value': 'median',
                'charttime': 'median'
            }).reset_index()
        
        # Filter groups with minimum observations
        group_sizes = df.groupby(group_cols).size()
        valid_groups = group_sizes[group_sizes >= self.config['min_observations']].index
        aggregated = aggregated.set_index(group_cols).loc[valid_groups].reset_index()
        
        self.logger.info(f"✅ Aggregated to {len(aggregated):,} measurements")
        return aggregated
    
    def _remove_outliers_iqr(self, df):
        """Remove outliers using IQR method"""
        outlier_count = 0
        
        for concept in df['concept_name'].unique():
            concept_data = df[df['concept_name'] == concept]['value']
            Q1 = concept_data.quantile(0.25)
            Q3 = concept_data.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - self.config['outlier_threshold'] * IQR
            upper_bound = Q3 + self.config['outlier_threshold'] * IQR
            
            outliers = (df['concept_name'] == concept) & \
                      ((df['value'] < lower_bound) | (df['value'] > upper_bound))
            
            outlier_count += outliers.sum()
            df = df[~outliers]
        
        self.logger.info(f"🚨 Removed {outlier_count:,} outliers using IQR method")
        return df
    
    def _remove_outliers_percentile(self, df):
        """Remove outliers using percentile method"""
        outlier_count = 0
        threshold = self.config['outlier_threshold']
        
        for concept in df['concept_name'].unique():
            concept_data = df[df['concept_name'] == concept]['value']
            lower_bound = concept_data.quantile(threshold)
            upper_bound = concept_data.quantile(1 - threshold)
            
            outliers = (df['concept_name'] == concept) & \
                      ((df['value'] < lower_bound) | (df['value'] > upper_bound))
            
            outlier_count += outliers.sum()
            df = df[~outliers]
        
        self.logger.info(f"🚨 Removed {outlier_count:,} outliers using percentile method")
        return df
    
    def pivot_measurements(self, df):
        """Pivot measurements to create patient-time matrix"""
        self.logger.info("🔄 Pivoting measurements for score calculation...")
        
        # Pivot to get one row per patient-time window
        pivoted = df.pivot_table(
            index=['subject_id', 'hadm_id', 'stay_id', 'charttime'],
            columns='concept_name',
            values='value',
            aggfunc='first'  # Take first value if duplicates
        ).reset_index()
        
        # Rename columns to match our schema
        column_mapping = {
            'Respiratory Rate': 'respiratory_rate',
            'Heart Rate': 'heart_rate',
            'PaCO2': 'paco2',
            'Tidal Volume': 'tidal_volume',
            'Minute Ventilation': 'minute_ventilation',
            'Creatinine': 'creatinine',
            'pH': 'ph',
            'Albumin': 'albumin',
            'Uric Acid': 'uric_acid',
            'NT-proBNP': 'nt_probnp',
            'D-Dimer': 'd_dimer',
            'Homocysteine': 'homocysteine',
            'Procalcitonin': 'procalcitonin',
            'IL-6': 'il_6',
            'IL-8': 'il_8',
            'IL-10': 'il_10',
            'ST2': 'st2',
            'Pentraxin-3': 'pentraxin_3',
            'Fraktalkin': 'fraktalkin',
            'sRAGE': 'srage',
            'KL-6': 'kl_6',
            'PAI-1': 'pai_1',
            'VEGF': 'vegf'
        }
        
        # Rename columns that exist
        existing_columns = {k: v for k, v in column_mapping.items() if k in pivoted.columns}
        pivoted = pivoted.rename(columns=existing_columns)
        
        self.logger.info(f"✅ Created {len(pivoted):,} patient-time records")
        return pivoted
    
    def apply_imputation(self, df):
        """Apply configuration-specific imputation"""
        self.logger.info(f"🔧 Applying {self.config['imputation_method']} imputation...")
        
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        missing_before = df[numeric_columns].isnull().sum().sum()
        
        if self.config['imputation_method'] == 'mean':
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].mean())
        elif self.config['imputation_method'] == 'median':
            df[numeric_columns] = df[numeric_columns].fillna(df[numeric_columns].median())
        
        missing_after = df[numeric_columns].isnull().sum().sum()
        imputed_values = missing_before - missing_after
        
        self.logger.info(f"✅ Imputed {imputed_values:,} missing values")
        return df
    
    def calculate_scores(self, df):
        """Calculate clinical severity scores"""
        self.logger.info("🏥 Calculating clinical severity scores...")
        
        # Simplified score calculations (can be enhanced with proper formulas)
        
        # APACHE-II Score (simplified - normally needs age, physiology, chronic health)
        df['apache_ii_score'] = self._calculate_apache_ii(df)
        
        # SOFA Score (simplified - normally needs 6 organ systems)
        df['sofa_score'] = self._calculate_sofa(df)
        
        # SAPS-II Score (simplified)
        df['saps_ii_score'] = self._calculate_saps_ii(df)
        
        # OASIS Score (simplified)
        df['oasis_score'] = self._calculate_oasis(df)
        
        # Add quality metrics
        total_params = 25  # Total possible parameters
        df['total_parameters_used'] = df.select_dtypes(include=[np.number]).notna().sum(axis=1)
        df['missing_parameters'] = total_params - df['total_parameters_used']
        df['data_quality_score'] = df['total_parameters_used'] / total_params
        
        self.stats['scores_calculated'] = len(df)
        self.logger.info(f"✅ Calculated scores for {len(df):,} records")
        
        return df
    
    def _calculate_apache_ii(self, df):
        """Simplified APACHE-II calculation"""
        score = 0
        
        # Respiratory component (using available parameters)
        if 'paco2' in df.columns:
            score += np.where(df['paco2'] > 50, 3, 0)  # Simplified
        
        # Cardiovascular component
        if 'heart_rate' in df.columns:
            score += np.where(df['heart_rate'] > 150, 3, 
                     np.where(df['heart_rate'] > 110, 2, 0))
        
        # Renal component
        if 'creatinine' in df.columns:
            score += np.where(df['creatinine'] > 3.5, 4,
                     np.where(df['creatinine'] > 2.0, 3, 0))
        
        return np.clip(score, 0, 71)  # APACHE-II max is 71
    
    def _calculate_sofa(self, df):
        """Simplified SOFA calculation"""
        score = 0
        
        # Respiratory (using available parameters)
        if 'respiratory_rate' in df.columns:
            score += np.where(df['respiratory_rate'] > 30, 2,
                     np.where(df['respiratory_rate'] > 25, 1, 0))
        
        # Cardiovascular
        if 'heart_rate' in df.columns:
            score += np.where(df['heart_rate'] > 120, 2, 0)
        
        # Renal
        if 'creatinine' in df.columns:
            score += np.where(df['creatinine'] > 5.0, 4,
                     np.where(df['creatinine'] > 3.5, 3,
                     np.where(df['creatinine'] > 2.0, 2,
                     np.where(df['creatinine'] > 1.2, 1, 0))))
        
        return np.clip(score, 0, 24)  # SOFA max is 24
    
    def _calculate_saps_ii(self, df):
        """Simplified SAPS-II calculation"""
        score = 0
        
        # Physiological variables
        if 'heart_rate' in df.columns:
            score += np.where(df['heart_rate'] > 160, 7,
                     np.where(df['heart_rate'] > 120, 4, 0))
        
        if 'respiratory_rate' in df.columns:
            score += np.where(df['respiratory_rate'] > 35, 6, 0)
        
        return np.clip(score, 0, 163)  # SAPS-II max is 163
    
    def _calculate_oasis(self, df):
        """Simplified OASIS calculation"""
        score = 0
        
        # OASIS is simpler, uses fewer variables
        if 'heart_rate' in df.columns:
            score += np.where(df['heart_rate'] > 125, 3, 0)
        
        if 'respiratory_rate' in df.columns:
            score += np.where(df['respiratory_rate'] > 30, 1, 0)
        
        return np.clip(score, 0, 7)  # OASIS max is 7
    
    def prepare_output_data(self, df):
        """Prepare final dataset for insertion"""
        self.logger.info("📝 Preparing output data...")
        
        # Add configuration metadata
        df['config_name'] = self.config['name']
        df['aggregation_method'] = self.config['aggregation_method']
        df['imputation_method'] = self.config['imputation_method']
        df['outlier_handling'] = self.config['outlier_handling']
        df['time_window_hours'] = self.config['time_window_hours']
        
        # Add timing information
        df['measurement_time'] = df['charttime']
        df['calculation_time'] = datetime.now()
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Rename for output schema
        df = df.rename(columns={
            'subject_id': 'patient_id'
        })
        
        # Calculate imputation count (simplified)
        df['imputed_parameters'] = 0  # Would need to track during imputation
        
        # Select final columns
        output_columns = [
            'patient_id', 'hadm_id', 'stay_id', 'measurement_time', 'calculation_time',
            'config_name', 'aggregation_method', 'imputation_method', 'outlier_handling',
            'time_window_hours', 'respiratory_rate', 'heart_rate', 'paco2', 
            'tidal_volume', 'minute_ventilation', 'creatinine', 'ph', 'albumin',
            'apache_ii_score', 'sofa_score', 'saps_ii_score', 'oasis_score',
            'total_parameters_used', 'missing_parameters', 'imputed_parameters',
            'data_quality_score', 'created_at', 'updated_at'
        ]
        
        # Only include columns that exist
        available_columns = [col for col in output_columns if col in df.columns]
        df_output = df[available_columns].copy()
        
        # Fill missing columns with NULL
        for col in output_columns:
            if col not in df_output.columns:
                df_output[col] = None
        
        self.stats['patients_processed'] = df_output['patient_id'].nunique()
        
        self.logger.info(f"✅ Prepared {len(df_output):,} records for output")
        return df_output[output_columns]
    
    def write_to_gold(self, df):
        """Write results to gold layer table"""
        table_name = self.config['output_table']
        self.logger.info(f"💾 Writing results to gold.{table_name}...")
        
        # Clear existing data for this configuration
        with self.engine.connect() as conn:
            conn.execute(text(f"DELETE FROM gold.{table_name} WHERE config_name = '{self.config['name']}'"))
            conn.commit()
        
        # Write new data
        df.to_sql(
            table_name,
            self.engine,
            schema='gold',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        self.logger.info(f"✅ Successfully wrote {len(df):,} records to gold.{table_name}")
    
    def run_pipeline(self):
        """Execute complete ETL pipeline"""
        start_time = datetime.now()
        self.logger.info(f"🚀 Starting Gold ETL Pipeline: {self.config['name']}")
        self.logger.info("=" * 60)
        
        try:
            # ETL Steps
            df = self.load_silver_data()
            df = self.aggregate_measurements(df)
            df = self.pivot_measurements(df)
            df = self.apply_imputation(df)
            df = self.calculate_scores(df)
            df = self.prepare_output_data(df)
            self.write_to_gold(df)
            
            # Final report
            duration = datetime.now() - start_time
            self.logger.info("=" * 60)
            self.logger.info("🎉 ETL Pipeline completed successfully!")
            self.logger.info(f"⏰ Duration: {duration}")
            self.logger.info(f"👥 Patients processed: {self.stats['patients_processed']:,}")
            self.logger.info(f"📊 Measurements processed: {self.stats['measurements_processed']:,}")
            self.logger.info(f"🏥 Scores calculated: {self.stats['scores_calculated']:,}")
            self.logger.info(f"📋 Output table: gold.{self.config['output_table']}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ ETL Pipeline failed: {e}")
            raise

def run_etl_pipeline(config):
    """Main function to run ETL pipeline with given configuration"""
    pipeline = GoldETLPipeline(config)
    return pipeline.run_pipeline()

if __name__ == "__main__":
    # Test with active configuration
    try:
        success = run_etl_pipeline(ACTIVE_CONFIG)
        if success:
            print("✅ ETL Pipeline completed successfully")
        else:
            print("❌ ETL Pipeline failed")
            sys.exit(1)
    except Exception as e:
        print(f"❌ ETL Pipeline error: {e}")
        sys.exit(1)
