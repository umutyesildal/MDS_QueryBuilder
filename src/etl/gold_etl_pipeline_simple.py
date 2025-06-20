# -*- coding: utf-8 -*-
"""
Gold Layer ETL Pipeline for Task 5.4 Dual Configurations - Simplified Version
============================================================================

This module implements the complete ETL pipeline that:
1. Loads data from Silver layer
2. Applies configuration-specific processing
3. Calculates clinical scores
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
import configg
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
        logger_name = 'GoldETL_' + self.config["name"]
        logger = logging.getLogger(logger_name)
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
            connection_string = "postgresql://{}:{}@{}:{}/{}".format(
                DB_CONFIG['user'], DB_CONFIG['password'], DB_CONFIG['host'], 
                DB_CONFIG['port'], DB_CONFIG['database']
            )
        else:
            connection_string = "postgresql://{}@{}:{}/{}".format(
                DB_CONFIG['user'], DB_CONFIG['host'], DB_CONFIG['port'], DB_CONFIG['database']
            )
        
        return create_engine(connection_string)
    
    def load_silver_data(self):
        """Load and prepare data from Silver layer"""
        self.logger.info("Loading data from Silver layer...")
        
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
        
        self.logger.info("Loaded {} measurements from Silver layer".format(len(df)))
        return df
    
    def prepare_for_gold_calculation(self, df):
        """Prepare and aggregate data for gold layer calculation"""
        self.logger.info("Preparing data for gold layer calculation...")
        
        # Apply configuration-specific filtering and aggregation
        if self.config['aggregation_method'] == 'mean':
            # Group by patient and take mean values
            grouped = df.groupby(['subject_id', 'hadm_id', 'stay_id', 'concept_name']).agg({
                'value': 'mean',
                'charttime': 'first'  # Take first charttime for reference
            }).reset_index()
        elif self.config['aggregation_method'] == 'median':
            # Group by patient and take median values
            grouped = df.groupby(['subject_id', 'hadm_id', 'stay_id', 'concept_name']).agg({
                'value': 'median',
                'charttime': 'first'
            }).reset_index()
        else:
            grouped = df
        
        # Pivot to get one row per patient with all measurements as columns
        pivoted = grouped.pivot_table(
            index=['subject_id', 'hadm_id', 'stay_id', 'charttime'],
            columns='concept_name',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Rename columns to match our expected schema
        column_mapping = {
            'Respiratory Rate': 'respiratory_rate',
            'Heart Rate': 'heart_rate',
            'PaCO2': 'paco2',
            'Creatinine': 'creatinine',
            'pH': 'ph'
        }
        
        # Rename columns that exist
        for old_name, new_name in column_mapping.items():
            if old_name in pivoted.columns:
                pivoted = pivoted.rename(columns={old_name: new_name})
        
        self.logger.info("Created {} patient records for gold calculation".format(len(pivoted)))
        return pivoted
    
    def calculate_simplified_scores(self, df):
        """Calculate simplified clinical scores"""
        self.logger.info("Calculating simplified clinical scores...")
        
        # Initialize scores
        df['apache_ii_score'] = 0
        df['sofa_score'] = 0
        df['saps_ii_score'] = 0
        df['oasis_score'] = 0
        
        # Simplified APACHE-II calculation
        if 'heart_rate' in df.columns:
            df['apache_ii_score'] += np.where(df['heart_rate'] > 150, 4,
                                     np.where(df['heart_rate'] > 110, 2, 0))
        
        if 'respiratory_rate' in df.columns:
            df['apache_ii_score'] += np.where(df['respiratory_rate'] > 35, 4,
                                     np.where(df['respiratory_rate'] > 25, 1, 0))
        
        if 'creatinine' in df.columns:
            df['apache_ii_score'] += np.where(df['creatinine'] > 3.5, 4,
                                     np.where(df['creatinine'] > 2.0, 3, 
                                     np.where(df['creatinine'] > 1.2, 2, 0)))
        
        # Simplified SOFA calculation
        if 'respiratory_rate' in df.columns:
            df['sofa_score'] += np.where(df['respiratory_rate'] > 30, 2,
                               np.where(df['respiratory_rate'] > 25, 1, 0))
        
        if 'heart_rate' in df.columns:
            df['sofa_score'] += np.where(df['heart_rate'] > 120, 2, 0)
        
        if 'creatinine' in df.columns:
            df['sofa_score'] += np.where(df['creatinine'] > 5.0, 4,
                               np.where(df['creatinine'] > 3.5, 3,
                               np.where(df['creatinine'] > 2.0, 2,
                               np.where(df['creatinine'] > 1.2, 1, 0))))
        
        # Simplified SAPS-II
        if 'heart_rate' in df.columns:
            df['saps_ii_score'] += np.where(df['heart_rate'] > 160, 7,
                                   np.where(df['heart_rate'] > 120, 4, 0))
        
        # Simplified OASIS
        if 'heart_rate' in df.columns:
            df['oasis_score'] += np.where(df['heart_rate'] > 125, 3, 0)
        
        # Clip scores to reasonable ranges
        df['apache_ii_score'] = np.clip(df['apache_ii_score'], 0, 71)
        df['sofa_score'] = np.clip(df['sofa_score'], 0, 24)
        df['saps_ii_score'] = np.clip(df['saps_ii_score'], 0, 163)
        df['oasis_score'] = np.clip(df['oasis_score'], 0, 7)
        
        self.stats['scores_calculated'] = len(df)
        self.logger.info("Calculated scores for {} records".format(len(df)))
        
        return df
    
    def prepare_output_data(self, df):
        """Prepare final dataset for insertion"""
        self.logger.info("Preparing output data...")
        
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
        df = df.rename(columns={'subject_id': 'patient_id'})
        
        # Add quality metrics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df['total_parameters_used'] = df[numeric_cols].notna().sum(axis=1)
        df['missing_parameters'] = 25 - df['total_parameters_used']  # Assuming 25 total possible
        df['data_quality_score'] = df['total_parameters_used'] / 25.0
        df['imputed_parameters'] = 0  # Simplified
        
        # Select final columns for output
        output_columns = [
            'patient_id', 'hadm_id', 'stay_id', 'measurement_time', 'calculation_time',
            'config_name', 'aggregation_method', 'imputation_method', 'outlier_handling',
            'time_window_hours', 'apache_ii_score', 'sofa_score', 'saps_ii_score', 'oasis_score',
            'total_parameters_used', 'missing_parameters', 'imputed_parameters',
            'data_quality_score', 'created_at', 'updated_at'
        ]
        
        # Add any parameter columns that exist
        parameter_columns = ['respiratory_rate', 'heart_rate', 'paco2', 'creatinine', 'ph']
        for col in parameter_columns:
            if col in df.columns:
                output_columns.insert(-6, col)  # Insert before the last 6 metadata columns
        
        # Only include columns that exist
        available_columns = [col for col in output_columns if col in df.columns]
        df_output = df[available_columns].copy()
        
        # Fill missing required columns with NULL
        for col in output_columns:
            if col not in df_output.columns:
                df_output[col] = None
        
        self.stats['patients_processed'] = df_output['patient_id'].nunique()
        
        self.logger.info("Prepared {} records for output".format(len(df_output)))
        return df_output[output_columns]
    
    def write_to_gold(self, df):
        """Write results to gold layer table"""
        table_name = self.config['output_table']
        self.logger.info("Writing results to gold.{}...".format(table_name))
        
        # Clear existing data for this configuration
        with self.engine.connect() as conn:
            delete_query = "DELETE FROM gold.{} WHERE config_name = '{}'".format(
                table_name, self.config['name']
            )
            conn.execute(text(delete_query))
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
        
        self.logger.info("Successfully wrote {} records to gold.{}".format(
            len(df), table_name
        ))
    
    def run_pipeline(self):
        """Execute complete ETL pipeline"""
        start_time = datetime.now()
        self.logger.info("Starting Gold ETL Pipeline: {}".format(self.config['name']))
        self.logger.info("=" * 60)
        
        try:
            # ETL Steps
            df = self.load_silver_data()
            df = self.prepare_for_gold_calculation(df)
            df = self.calculate_simplified_scores(df)
            df = self.prepare_output_data(df)
            self.write_to_gold(df)
            
            # Final report
            duration = datetime.now() - start_time
            self.logger.info("=" * 60)
            self.logger.info("ETL Pipeline completed successfully!")
            self.logger.info("Duration: {}".format(duration))
            self.logger.info("Patients processed: {}".format(self.stats['patients_processed']))
            self.logger.info("Measurements processed: {}".format(self.stats['measurements_processed']))
            self.logger.info("Scores calculated: {}".format(self.stats['scores_calculated']))
            self.logger.info("Output table: gold.{}".format(self.config['output_table']))
            
            return True
            
        except Exception as e:
            self.logger.error("ETL Pipeline failed: {}".format(e))
            raise

def run_etl_pipeline(config):
    """Main function to run ETL pipeline with given configuration"""
    pipeline = GoldETLPipeline(config)
    return pipeline.run_pipeline()

if __name__ == "__main__":
    # Test with active configuration
    try:
        success = run_etl_pipeline(configg.ACTIVE_CONFIG)
        if success:
            print("ETL Pipeline completed successfully")
        else:
            print("ETL Pipeline failed")
            sys.exit(1)
    except Exception as e:
        print("ETL Pipeline error: {}".format(e))
        sys.exit(1)
