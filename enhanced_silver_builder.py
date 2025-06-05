#!/usr/bin/env python3
"""
Enhanced Silver Layer Builder
============================

This module builds the Silver layer with comprehensive OMOP mapping and unit
standardization for all discovered SOFA parameters from the enhanced Bronze layer.

Features:
- Comprehensive OMOP concept mapping based on discovery results
- Dynamic unit conversions and standardization
- Quality flagging and validation
- Proper SOFA system classification
- Integration with discovered parameter metadata

Author: Medical Data Science Team
Date: 2025-06-05
"""

import pandas as pd
import numpy as np
import psycopg2
import json
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from typing import Dict, List, Any

# Import configurations
from config_local import DB_CONFIG

class EnhancedSilverBuilder:
    """Enhanced Silver layer builder with comprehensive OMOP mapping"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.conn = psycopg2.connect(**DB_CONFIG)
        
        # Build connection string with proper password handling
        if DB_CONFIG.get('password'):
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        else:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        self.engine = create_engine(connection_string)
        
        # Load discovery results and OMOP mappings
        self.omop_mappings = self._load_omop_mappings()
        self.processing_stats = {
            'total_bronze_records': 0,
            'filtered_records': 0,
            'quality_records': 0,
            'retention_rate': 0,
            'mapped_records': 0,
            'unmapped_records': 0,
            'outliers_flagged': 0,
            'units_converted': 0,
            'conversions_applied': 0,
            'start_time': datetime.now(),
            'errors': []
        }
    
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logger = logging.getLogger('EnhancedSilverBuilder')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler('silver_builder.log')
        console_handler = logging.StreamHandler()
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def _load_omop_mappings(self):
        """Load OMOP concept mappings from discovery results"""
        try:
            with open('omop_concept_mappings.json', 'r') as f:
                concept_mappings = json.load(f)
            
            # Reorganize mappings by source_itemid for easier lookup
            itemid_mappings = {}
            for concept_id, mapping in concept_mappings.items():
                source_itemid = str(mapping['source_itemid'])
                itemid_mappings[source_itemid] = mapping
            
            self.logger.info(f"✅ Loaded {len(itemid_mappings)} OMOP concept mappings")
            return itemid_mappings
        except FileNotFoundError:
            self.logger.error("❌ OMOP mappings not found. Run parameter_discovery.py first!")
            raise

    def create_silver_schema(self):
        """Create Silver schema and table with enhanced structure"""
        self.logger.info("🥈 Creating Silver schema and table...")
        
        cur = self.conn.cursor()
        
        # Drop and recreate silver schema
        cur.execute("DROP SCHEMA IF EXISTS silver CASCADE")
        cur.execute("CREATE SCHEMA silver")
        
        # Create enhanced silver table
        create_table_sql = """
        CREATE TABLE silver.collection_disease_std (
            -- Auto-incrementing primary key
            id SERIAL PRIMARY KEY,
            
            -- Reference to Bronze
            bronze_id INTEGER NOT NULL,
            
            -- Patient identifiers
            subject_id INTEGER NOT NULL,
            hadm_id INTEGER,
            stay_id INTEGER,
            
            -- Temporal data
            charttime TIMESTAMP NOT NULL,
            storetime TIMESTAMP,
            
            -- Item identification
            itemid INTEGER NOT NULL,
            label TEXT,
            category TEXT,
            
            -- OMOP mapping
            concept_id INTEGER,
            concept_name TEXT,
            concept_domain TEXT,
            vocabulary_id TEXT,
            
            -- SOFA classification
            sofa_system VARCHAR(50),
            sofa_parameter_type VARCHAR(100),
            
            -- Values (original and standardized)
            value_original TEXT,
            valuenum_original NUMERIC,
            valueuom_original VARCHAR(50),
            
            valuenum_std NUMERIC,
            unit_std VARCHAR(50),
            
            -- Source tracking
            source_table VARCHAR(50) NOT NULL,
            source_fluid VARCHAR(50),
            
            -- Quality flags (JSON for flexibility)
            quality_flags JSONB DEFAULT '{}',
            
            -- Transformation log
            transformation_log TEXT,
            
            -- Metadata
            processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes separately
        CREATE INDEX idx_silver_subject ON silver.collection_disease_std (subject_id);
        CREATE INDEX idx_silver_stay ON silver.collection_disease_std (stay_id);
        CREATE INDEX idx_silver_charttime ON silver.collection_disease_std (charttime);
        CREATE INDEX idx_silver_concept ON silver.collection_disease_std (concept_id);
        CREATE INDEX idx_silver_sofa_system ON silver.collection_disease_std (sofa_system);
        """
        
        cur.execute(create_table_sql)
        self.conn.commit()
        
        self.logger.info("✅ Silver schema and table created successfully")

    def load_bronze_data(self):
        """Load and filter Bronze layer data based on quality flags"""
        self.logger.info("📊 Loading Bronze layer data with quality filtering...")
        
        # First get total count for reporting
        total_query = "SELECT COUNT(*) FROM bronze.collection_disease"
        total_records = pd.read_sql(total_query, self.engine).iloc[0, 0]
        
        # Load only good quality records (filter out outliers and suspicious records)
        query = """
        SELECT 
            id as bronze_id,
            subject_id, hadm_id, stay_id, itemid, charttime,
            value, valuenum, valueuom, label, category,
            source_table, source_fluid, sofa_system, search_term_matched,
            is_outlier, is_suspicious,
            extraction_timestamp as storetime
        FROM bronze.collection_disease
        WHERE is_outlier = false AND is_suspicious = false
        ORDER BY subject_id, charttime
        """
        
        df = pd.read_sql(query, self.engine)
        
        # Update processing stats
        self.processing_stats['total_bronze_records'] = total_records
        self.processing_stats['filtered_records'] = total_records - len(df)
        self.processing_stats['quality_records'] = len(df)
        self.processing_stats['retention_rate'] = len(df) / total_records * 100
        
        self.logger.info(f"📊 Bronze Data Quality Filtering:")
        self.logger.info(f"   Total Bronze records: {total_records:,}")
        self.logger.info(f"   Filtered out (outliers/suspicious): {self.processing_stats['filtered_records']:,}")
        self.logger.info(f"   Quality records loaded: {len(df):,}")
        self.logger.info(f"   Retention rate: {self.processing_stats['retention_rate']:.1f}%")
        
        return df

    def apply_omop_mapping(self, df):
        """Apply OMOP concept mappings to the data"""
        self.logger.info("🏷️ Applying OMOP concept mappings...")
        
        # Initialize OMOP columns
        df['concept_id'] = None
        df['concept_name'] = None
        df['concept_domain'] = None
        df['vocabulary_id'] = None
        df['clinical_limits'] = None
        df['standard_unit'] = None
        
        mapped_count = 0
        
        for idx, row in df.iterrows():
            itemid = str(row['itemid'])
            
            if itemid in self.omop_mappings:
                mapping = self.omop_mappings[itemid]
                
                df.at[idx, 'concept_id'] = mapping['concept_id']
                df.at[idx, 'concept_name'] = mapping['concept_name']
                df.at[idx, 'concept_domain'] = mapping.get('domain_id', 'Measurement')
                df.at[idx, 'vocabulary_id'] = mapping.get('vocabulary_id', 'MIMIC-IV')
                df.at[idx, 'clinical_limits'] = [mapping.get('min_value'), mapping.get('max_value')]
                df.at[idx, 'standard_unit'] = mapping.get('standard_unit', row['valueuom'])
                
                mapped_count += 1
        
        self.processing_stats['mapped_records'] = mapped_count
        self.processing_stats['unmapped_records'] = len(df) - mapped_count
        
        self.logger.info(f"✅ Mapped {mapped_count:,} records ({mapped_count/len(df)*100:.1f}%)")
        
        return df

    def standardize_values_and_units(self, df):
        """Standardize values and units based on OMOP mappings"""
        self.logger.info("🔧 Standardizing values and units...")
        
        df['valuenum_std'] = df['valuenum'].copy()
        df['unit_std'] = df['valueuom'].copy()
        df['quality_flags'] = df.apply(lambda x: {}, axis=1)
        df['transformation_log'] = ''
        
        conversions_applied = 0
        
        for idx, row in df.iterrows():
            flags = {}
            log_entries = []
            
            # Apply unit conversions if needed
            if pd.notna(row['standard_unit']) and pd.notna(row['valueuom']):
                original_unit = str(row['valueuom']).lower().strip()
                target_unit = str(row['standard_unit']).lower().strip()
                
                if original_unit != target_unit:
                    # Apply common unit conversions
                    converted_value = self._convert_units(row['valuenum'], original_unit, target_unit)
                    if converted_value is not None:
                        df.at[idx, 'valuenum_std'] = converted_value
                        df.at[idx, 'unit_std'] = row['standard_unit']
                        flags['UNIT_CONVERTED'] = True
                        log_entries.append(f"Unit converted: {original_unit} → {target_unit}")
                        conversions_applied += 1
            
            # Flag outliers based on clinical limits
            if row['clinical_limits'] is not None and pd.notna(row['valuenum_std']):
                min_val, max_val = row['clinical_limits']
                if min_val is not None and max_val is not None:
                    if row['valuenum_std'] < min_val or row['valuenum_std'] > max_val:
                        flags['OUTLIER_DETECTED'] = True
                        log_entries.append(f"Outlier: {row['valuenum_std']} outside [{min_val}, {max_val}]")
            
            # Flag suspicious values from Bronze
            if row.get('is_suspicious', False):
                flags['SUSPICIOUS_VALUE'] = True
                log_entries.append("Flagged as suspicious in Bronze layer")
            
            # Store flags and log
            df.at[idx, 'quality_flags'] = flags
            df.at[idx, 'transformation_log'] = '; '.join(log_entries)
        
        self.processing_stats['units_converted'] = conversions_applied
        self.processing_stats['conversions_applied'] = conversions_applied
        self.processing_stats['outliers_flagged'] = df['quality_flags'].apply(
            lambda x: x.get('OUTLIER_DETECTED', False)
        ).sum()
        
        self.logger.info(f"✅ Applied {conversions_applied:,} unit conversions")
        self.logger.info(f"🚩 Flagged {self.processing_stats['outliers_flagged']:,} outliers")
        
        return df

    def _convert_units(self, value, from_unit, to_unit):
        """Convert between common medical units"""
        if pd.isna(value):
            return None
            
        # Common unit conversions
        conversions = {
            # Temperature
            ('celsius', 'fahrenheit'): lambda x: x * 9/5 + 32,
            ('fahrenheit', 'celsius'): lambda x: (x - 32) * 5/9,
            
            # Pressure
            ('mmhg', 'cmh2o'): lambda x: x * 1.36,
            ('cmh2o', 'mmhg'): lambda x: x / 1.36,
            
            # Volume
            ('ml', 'l'): lambda x: x / 1000,
            ('l', 'ml'): lambda x: x * 1000,
            
            # Weight/Mass
            ('kg', 'g'): lambda x: x * 1000,
            ('g', 'kg'): lambda x: x / 1000,
            ('lb', 'kg'): lambda x: x * 0.453592,
            ('kg', 'lb'): lambda x: x / 0.453592,
        }
        
        conversion_key = (from_unit, to_unit)
        if conversion_key in conversions:
            return conversions[conversion_key](value)
        
        return None

    def prepare_silver_data(self, df):
        """Prepare final DataFrame for Silver layer insertion"""
        self.logger.info("📝 Preparing data for Silver layer...")
        
        # Select columns for Silver schema
        silver_columns = [
            'bronze_id', 'subject_id', 'hadm_id', 'stay_id', 'charttime', 'storetime',
            'itemid', 'label', 'category', 'concept_id', 'concept_name', 'concept_domain',
            'vocabulary_id', 'sofa_system', 'search_term_matched', 'value', 'valuenum',
            'valueuom', 'valuenum_std', 'unit_std', 'source_table', 'source_fluid',
            'quality_flags', 'transformation_log'
        ]
        
        # Rename columns to match Silver schema
        column_mapping = {
            'value': 'value_original',
            'valuenum': 'valuenum_original', 
            'valueuom': 'valueuom_original',
            'search_term_matched': 'sofa_parameter_type'
        }
        
        df_prepared = df[silver_columns].rename(columns=column_mapping)
        
        # Convert quality_flags to JSON
        df_prepared['quality_flags'] = df_prepared['quality_flags'].apply(json.dumps)
        
        # Fill NaN values
        df_prepared = df_prepared.fillna({
            'transformation_log': '',
            'sofa_parameter_type': '',
            'concept_domain': 'Measurement',
            'vocabulary_id': 'MIMIC-IV'
        })
        
        return df_prepared

    def write_to_silver(self, df):
        """Write processed data to Silver layer"""
        self.logger.info("💾 Writing data to Silver layer...")
        
        # Write to database
        df.to_sql(
            'collection_disease_std',
            self.engine,
            schema='silver',
            if_exists='append',
            index=False,
            method='multi'
        )
        
        self.logger.info(f"✅ Successfully wrote {len(df):,} records to Silver layer")

    def generate_processing_report(self):
        """Generate comprehensive processing report"""
        self.logger.info("📋 Generating processing report...")
        
        duration = datetime.now() - self.processing_stats['start_time']
        
        report = []
        report.append("# Enhanced Silver Layer Processing Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Processing Duration: {duration}")
        report.append("")
        
        report.append("## Processing Summary")
        report.append(f"- **Total Bronze Records**: {self.processing_stats.get('total_bronze_records', 0):,}")
        report.append(f"- **Quality Filtered Out**: {self.processing_stats.get('filtered_records', 0):,}")
        report.append(f"- **Quality Records Processed**: {self.processing_stats.get('quality_records', 0):,}")
        report.append(f"- **Retention Rate**: {self.processing_stats.get('retention_rate', 0):.1f}%")
        report.append(f"- **Successfully Mapped**: {self.processing_stats['mapped_records']:,}")
        report.append(f"- **Unmapped Records**: {self.processing_stats['unmapped_records']:,}")
        report.append(f"- **Mapping Success Rate**: {self.processing_stats['mapped_records']/self.processing_stats.get('quality_records', 1)*100:.1f}%")
        report.append("")
        
        report.append("## Quality Processing")
        report.append(f"- **Unit Conversions Applied**: {self.processing_stats.get('units_converted', 0):,}")
        report.append(f"- **Outliers Flagged**: {self.processing_stats.get('outliers_flagged', 0):,}")
        report.append(f"- **Quality Flag Rate**: {self.processing_stats.get('outliers_flagged', 0)/self.processing_stats.get('quality_records', 1)*100:.1f}%")
        report.append("")
        
        # Get Silver layer statistics
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT sofa_system, COUNT(*) as record_count,
                   COUNT(DISTINCT subject_id) as unique_patients
            FROM silver.collection_disease_std
            GROUP BY sofa_system
            ORDER BY record_count DESC
        """)
        system_stats = cur.fetchall()
        
        report.append("## Records by SOFA System")
        for system, records, patients in system_stats:
            report.append(f"- **{system.upper()}**: {records:,} records, {patients} patients")
        report.append("")
        
        cur.execute("""
            SELECT concept_name, COUNT(*) as record_count
            FROM silver.collection_disease_std
            WHERE concept_name IS NOT NULL
            GROUP BY concept_name
            ORDER BY record_count DESC
            LIMIT 15
        """)
        concept_stats = cur.fetchall()
        
        report.append("## Top 15 OMOP Concepts")
        for concept, records in concept_stats:
            report.append(f"- **{concept}**: {records:,} records")
        
        # Write report
        with open('silver_processing_report.md', 'w') as f:
            f.write('\n'.join(report))
        
        self.logger.info("📋 Processing report saved: silver_processing_report.md")

    def build_silver_layer(self):
        """Build complete Silver layer with comprehensive OMOP mapping"""
        self.logger.info("🚀 Starting enhanced Silver layer build...")
        
        try:
            # Create schema and table
            self.create_silver_schema()
            
            # Load and process data
            df = self.load_bronze_data()
            df = self.apply_omop_mapping(df)
            df = self.standardize_values_and_units(df)
            df_silver = self.prepare_silver_data(df)
            
            # Write to Silver layer
            self.write_to_silver(df_silver)
            
            # Generate report
            self.generate_processing_report()
            
            self.logger.info(f"✅ Silver layer build completed successfully!")
            self.logger.info(f"📊 Total records: {len(df_silver):,}")
            
            return len(df_silver)
            
        except Exception as e:
            self.logger.error(f"❌ Silver layer build failed: {e}", exc_info=True)
            raise

def main():
    """Main execution function"""
    print("🚀 Starting Enhanced Silver Layer Build...")
    print("=" * 60)
    
    try:
        builder = EnhancedSilverBuilder()
        total_records = builder.build_silver_layer()
        
        print("✅ Silver layer build completed successfully!")
        print(f"📊 Results:")
        print(f"   - Total records processed: {total_records:,}")
        print(f"   - Comprehensive OMOP mapping applied")
        print(f"   - Quality flags and unit standardization completed")
        print("")
        print("📋 Reports generated:")
        print("   - silver_processing_report.md")
        print("   - silver_builder.log")
        
        return 0
        
    except Exception as e:
        print(f"❌ Silver layer build failed: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
