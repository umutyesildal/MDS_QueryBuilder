#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Silver Layer Data Standardization Pipeline
==========================================

Transforms Bronze layer raw clinical data into standardized Silver layer
with OMOP concept mapping, unit conversions, and quality assurance.

QueryBuilder for Medical Data Science (Uebungsblatt 3.2)
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import sys
from config_local import DB_CONFIG
from config_silver import (
    OMOP_MAPPING, UNIT_CONVERSIONS, CLINICAL_LIMITS, 
    SILVER_SCHEMA, SILVER_TABLE, QUALITY_FLAGS
)

class SilverLayerProcessor:
    """Processes Bronze layer data into standardized Silver layer."""
    
    def __init__(self):
        self.engine = None
        self.logger = self._setup_logging()
        self.stats = {
            'records_processed': 0,
            'records_converted': 0,
            'outliers_detected': 0,
            'errors_flagged': 0,
            'duplicates_resolved': 0,
            'unit_conversions': {}
        }
        
    def _setup_logging(self):
        """Setup comprehensive logging for Silver layer processing."""
        logger = logging.getLogger('SilverProcessor')
        logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler('standardize.log')
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        return logger
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            self.engine = create_engine(connection_string)
            self.logger.info("✅ Connected to database successfully")
            return True
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def create_silver_schema(self):
        """Create Silver schema and standardized table."""
        try:
            with self.engine.connect() as conn:
                # Create schema
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}"))
                self.logger.info(f"✅ Created schema: {SILVER_SCHEMA}")
                
                # Create standardized table
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{SILVER_TABLE} (
                    id SERIAL PRIMARY KEY,
                    bronze_id INTEGER,
                    subject_id INTEGER,
                    hadm_id INTEGER,
                    stay_id INTEGER,
                    charttime TIMESTAMP,
                    storetime TIMESTAMP,
                    itemid INTEGER,
                    concept_name TEXT,
                    concept_id INTEGER,
                    parameter_type TEXT,
                    value NUMERIC,
                    valueuom TEXT,
                    source_table TEXT,
                    is_outlier BOOLEAN DEFAULT FALSE,
                    error_flag BOOLEAN DEFAULT FALSE,
                    transformation_log TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                conn.execute(text(create_table_sql))
                conn.commit()
                self.logger.info(f"✅ Created table: {SILVER_SCHEMA}.{SILVER_TABLE}")
                
        except Exception as e:
            self.logger.error(f"❌ Schema creation failed: {e}")
            raise
    
    def load_bronze_data(self):
        """Load all data from Bronze layer."""
        try:
            query = """
            SELECT 
                ROW_NUMBER() OVER (ORDER BY subject_id, charttime) as bronze_id,
                subject_id,
                hadm_id,
                stay_id,
                charttime,
                storetime,
                itemid,
                value,
                valuenum,
                valueuom,
                source_table
            FROM bronze.collection_disease
            WHERE valuenum IS NOT NULL
            ORDER BY subject_id, charttime
            """
            
            df = pd.read_sql_query(query, self.engine)
            self.stats['records_processed'] = len(df)
            
            self.logger.info(f"📊 Loaded {len(df):,} records from Bronze layer")
            return df
            
        except Exception as e:
            self.logger.error(f"❌ Failed to load Bronze data: {e}")
            raise
    
    def enrich_with_omop(self, df):
        """Enrich data with OMOP concept mappings."""
        self.logger.info("🏷️ Enriching data with OMOP concepts...")
        
        # Create OMOP mapping DataFrame
        omop_df = pd.DataFrame.from_dict(OMOP_MAPPING, orient='index').reset_index()
        omop_df.rename(columns={'index': 'itemid'}, inplace=True)
        
        # Merge with bronze data
        enriched_df = df.merge(omop_df, on='itemid', how='left')
        
        # Log unmapped items
        unmapped = enriched_df[enriched_df['concept_id'].isna()]
        if len(unmapped) > 0:
            unmapped_items = unmapped['itemid'].unique()
            self.logger.warning(f"⚠️ Unmapped itemids: {unmapped_items}")
        
        # Remove unmapped records
        enriched_df = enriched_df.dropna(subset=['concept_id'])
        
        self.logger.info(f"✅ Enriched {len(enriched_df):,} records with OMOP concepts")
        return enriched_df
    
    def standardize_units(self, df):
        """Convert values to standardized units."""
        self.logger.info("🔧 Standardizing units and values...")
        
        df['transformation_log'] = ''
        df['original_value'] = df['valuenum'].copy()
        df['original_unit'] = df['valueuom'].copy()
        
        conversions_performed = 0
        
        for idx, row in df.iterrows():
            current_unit = str(row['valueuom']).strip() if pd.notna(row['valueuom']) else 'unknown'
            target_unit = row['standard_unit']
            
            # Skip if units already match
            if current_unit.lower() == target_unit.lower():
                df.at[idx, 'valueuom'] = target_unit
                continue
                
            # Try to find conversion
            conversion_key = (current_unit, target_unit)
            if conversion_key in UNIT_CONVERSIONS:
                try:
                    old_value = row['valuenum']
                    new_value = UNIT_CONVERSIONS[conversion_key](old_value)
                    
                    df.at[idx, 'valuenum'] = new_value
                    df.at[idx, 'valueuom'] = target_unit
                    df.at[idx, 'transformation_log'] = f"Unit converted: {current_unit}→{target_unit} ({old_value}→{new_value:.3f})"
                    
                    conversions_performed += 1
                    
                    # Track conversion statistics
                    if conversion_key not in self.stats['unit_conversions']:
                        self.stats['unit_conversions'][conversion_key] = 0
                    self.stats['unit_conversions'][conversion_key] += 1
                    
                except Exception as e:
                    self.logger.warning(f"⚠️ Unit conversion failed for {conversion_key}: {e}")
                    df.at[idx, 'error_flag'] = True
                    df.at[idx, 'transformation_log'] = f"Unit conversion failed: {current_unit}→{target_unit}"
            else:
                # Log unknown conversion
                if current_unit != 'unknown' and current_unit != target_unit:
                    self.logger.warning(f"⚠️ No conversion found: {current_unit} → {target_unit}")
                    df.at[idx, 'transformation_log'] = f"Unknown unit conversion: {current_unit}"
        
        self.stats['records_converted'] = conversions_performed
        self.logger.info(f"✅ Performed {conversions_performed:,} unit conversions")
        
        return df
    
    def detect_outliers(self, df):
        """Detect and flag outliers based on clinical limits."""
        self.logger.info("🔍 Detecting outliers using clinical limits...")
        
        outliers_detected = 0
        
        for idx, row in df.iterrows():
            concept_name = row['concept_name']
            value = row['valuenum']
            
            # Use concept-specific limits or parameter-specific limits
            limits = row.get('limits') or CLINICAL_LIMITS.get(concept_name)
            
            if limits and pd.notna(value):
                min_val, max_val = limits
                
                if value < min_val or value > max_val:
                    df.at[idx, 'is_outlier'] = True
                    current_log = df.at[idx, 'transformation_log']
                    df.at[idx, 'transformation_log'] = f"{current_log}; Outlier: {value} outside [{min_val}, {max_val}]".strip('; ')
                    
                    outliers_detected += 1
                    
                    self.logger.warning(f"⚠️ Outlier detected: {concept_name}={value} (limits: {min_val}-{max_val}), subject_id={row['subject_id']}")
        
        self.stats['outliers_detected'] = outliers_detected
        self.logger.info(f"🚨 Detected {outliers_detected:,} outliers")
        
        return df
    
    def resolve_duplicates(self, df):
        """Resolve duplicate records by keeping the latest storetime."""
        self.logger.info("🔄 Resolving duplicate records...")
        
        initial_count = len(df)
        
        # Sort by storetime to keep latest
        df = df.sort_values(['storetime'], na_position='last')
        
        # Identify duplicates
        duplicate_mask = df.duplicated(subset=['subject_id', 'itemid', 'charttime'], keep='last')
        duplicates = df[duplicate_mask]
        
        if len(duplicates) > 0:
            self.logger.info(f"📋 Found {len(duplicates):,} duplicate records")
            
            # Log duplicate resolution
            for idx in duplicates.index:
                current_log = df.at[idx, 'transformation_log']
                df.at[idx, 'transformation_log'] = f"{current_log}; Duplicate resolved (kept latest storetime)".strip('; ')
        
        # Remove duplicates
        df = df[~duplicate_mask]
        
        final_count = len(df)
        duplicates_removed = initial_count - final_count
        
        self.stats['duplicates_resolved'] = duplicates_removed
        self.logger.info(f"✅ Resolved {duplicates_removed:,} duplicates, {final_count:,} records remaining")
        
        return df
    
    def validate_data_quality(self, df):
        """Final data quality validation."""
        self.logger.info("✅ Performing final data quality validation...")
        
        # Check for missing critical fields
        missing_concept = df['concept_id'].isna().sum()
        missing_value = df['valuenum'].isna().sum()
        missing_unit = df['valueuom'].isna().sum()
        
        if missing_concept > 0:
            self.logger.warning(f"⚠️ {missing_concept} records missing concept_id")
        if missing_value > 0:
            self.logger.warning(f"⚠️ {missing_value} records missing valuenum")
        if missing_unit > 0:
            self.logger.warning(f"⚠️ {missing_unit} records missing valueuom")
        
        # Flag records with quality issues
        quality_issues = (df['concept_id'].isna()) | (df['valuenum'].isna())
        df.loc[quality_issues, 'error_flag'] = True
        
        errors_flagged = quality_issues.sum()
        self.stats['errors_flagged'] = errors_flagged
        
        self.logger.info(f"🏁 Validation complete: {errors_flagged:,} records flagged with errors")
        
        return df
    
    def prepare_silver_data(self, df):
        """Prepare final DataFrame for Silver layer insertion."""
        
        # Select and rename columns for Silver schema
        silver_df = df[[
            'bronze_id', 'subject_id', 'hadm_id', 'stay_id', 'charttime', 'storetime',
            'itemid', 'concept_name', 'concept_id', 'parameter_type', 
            'valuenum', 'valueuom', 'source_table', 'is_outlier', 'error_flag', 'transformation_log'
        ]].copy()
        
        # Rename valuenum to value for Silver schema
        silver_df.rename(columns={'valuenum': 'value'}, inplace=True)
        
        # Fill NaN values appropriately
        silver_df['is_outlier'] = silver_df['is_outlier'].fillna(False)
        silver_df['error_flag'] = silver_df['error_flag'].fillna(False)
        silver_df['transformation_log'] = silver_df['transformation_log'].fillna('')
        
        return silver_df
    
    def write_to_silver(self, df):
        """Write standardized data to Silver layer."""
        try:
            # Clear existing data
            with self.engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {SILVER_SCHEMA}.{SILVER_TABLE} RESTART IDENTITY"))
                conn.commit()
            
            # Write new data
            df.to_sql(
                SILVER_TABLE, 
                self.engine, 
                schema=SILVER_SCHEMA, 
                if_exists='append', 
                index=False,
                method='multi',
                chunksize=1000
            )
            
            self.logger.info(f"✅ Successfully wrote {len(df):,} records to {SILVER_SCHEMA}.{SILVER_TABLE}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to write to Silver layer: {e}")
            raise
    
    def generate_summary_report(self):
        """Generate comprehensive summary report."""
        self.logger.info("📊 Generating Silver layer summary report...")
        
        try:
            with self.engine.connect() as conn:
                # Get final statistics
                stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT subject_id) as unique_patients,
                    COUNT(DISTINCT concept_id) as unique_concepts,
                    COUNT(DISTINCT parameter_type) as parameter_types,
                    SUM(CASE WHEN is_outlier THEN 1 ELSE 0 END) as outliers,
                    SUM(CASE WHEN error_flag THEN 1 ELSE 0 END) as errors,
                    COUNT(DISTINCT valueuom) as unique_units
                FROM {SILVER_SCHEMA}.{SILVER_TABLE}
                """
                
                result = conn.execute(text(stats_query)).fetchone()
                
                self.logger.info("🏆 SILVER LAYER SUMMARY REPORT")
                self.logger.info("=" * 50)
                self.logger.info(f"📊 Processing Statistics:")
                self.logger.info(f"  Records processed: {self.stats['records_processed']:,}")
                self.logger.info(f"  Records in Silver: {result[0]:,}")
                self.logger.info(f"  Unit conversions: {self.stats['records_converted']:,}")
                self.logger.info(f"  Outliers detected: {self.stats['outliers_detected']:,}")
                self.logger.info(f"  Errors flagged: {self.stats['errors_flagged']:,}")
                self.logger.info(f"  Duplicates resolved: {self.stats['duplicates_resolved']:,}")
                
                self.logger.info(f"\n🎯 Data Quality Metrics:")
                self.logger.info(f"  Unique patients: {result[1]:,}")
                self.logger.info(f"  Unique OMOP concepts: {result[2]:,}")
                self.logger.info(f"  Parameter types: {result[3]:,}")
                self.logger.info(f"  Standardized units: {result[6]:,}")
                
                if self.stats['unit_conversions']:
                    self.logger.info(f"\n🔧 Unit Conversions Performed:")
                    for (from_unit, to_unit), count in self.stats['unit_conversions'].items():
                        self.logger.info(f"  {from_unit} → {to_unit}: {count:,} conversions")
                
        except Exception as e:
            self.logger.error(f"❌ Report generation failed: {e}")
    
    def run(self):
        """Execute the complete Silver layer processing pipeline."""
        self.logger.info("🥈 Starting Silver Layer Processing Pipeline...")
        
        try:
            # Step 1: Setup
            if not self.connect():
                return False
            
            self.create_silver_schema()
            
            # Step 2: Load and process data
            df = self.load_bronze_data()
            df = self.enrich_with_omop(df)
            df = self.standardize_units(df)
            df = self.detect_outliers(df)
            df = self.resolve_duplicates(df)
            df = self.validate_data_quality(df)
            
            # Step 3: Prepare and write to Silver
            silver_df = self.prepare_silver_data(df)
            self.write_to_silver(silver_df)
            
            # Step 4: Generate reports
            self.generate_summary_report()
            
            self.logger.info("🎉 Silver layer processing completed successfully!")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Silver layer processing failed: {e}")
            return False


def main():
    """Main execution function."""
    print("🥈 Silver Layer Data Standardization")
    print("=" * 50)
    
    processor = SilverLayerProcessor()
    
    if processor.run():
        print("\n✅ Silver layer processing completed successfully!")
        print("📊 Check 'standardize.log' for detailed processing logs")
    else:
        print("\n❌ Silver layer processing failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
