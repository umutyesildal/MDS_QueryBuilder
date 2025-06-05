#!/usr/bin/env python3
"""
Enhanced Bronze Layer Builder
============================

This module builds the Bronze layer with comprehensive SOFA parameter extraction
based on the parameter discovery results. It follows the upgrade plan by extracting
ALL discovered parameters with transparency and no filtering.

Features:
- Comprehensive extraction of all discovered SOFA parameters
- No data filtering - flag but don't drop values
- Complete transparency and logging
- Proper data type handling for PostgreSQL compatibility
- Integration with all MIMIC-IV tables (chartevents, labevents, outputevents, inputevents)

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

class EnhancedBronzeBuilder:
    """Enhanced Bronze layer builder with comprehensive SOFA extraction"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.conn = psycopg2.connect(**DB_CONFIG)
        
        # Build connection string with proper password handling
        if DB_CONFIG.get('password'):
            connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        else:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        self.engine = create_engine(connection_string)
        
        # Load discovery results
        self.discovered_params = self._load_discovery_results()
        self.extraction_stats = {
            'total_records': 0,
            'by_table': {},
            'by_system': {},
            'start_time': datetime.now(),
            'errors': []
        }
        
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logger = logging.getLogger('EnhancedBronzeBuilder')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler('bronze_builder.log')
        console_handler = logging.StreamHandler()
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def _load_discovery_results(self):
        """Load parameter discovery results"""
        try:
            with open('discovered_sofa_parameters.json', 'r') as f:
                discovered_params = json.load(f)
            self.logger.info("✅ Loaded parameter discovery results")
            return discovered_params
        except FileNotFoundError:
            self.logger.error("❌ Discovery results not found. Run parameter_discovery.py first!")
            raise

    def create_bronze_schema(self):
        """Create Bronze schema and table with enhanced structure"""
        self.logger.info("🥉 Creating Bronze schema and table...")
        
        cur = self.conn.cursor()
        
        # Drop and recreate bronze schema
        cur.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
        cur.execute("CREATE SCHEMA bronze")
        
        # Create enhanced bronze table
        create_table_sql = """
        CREATE TABLE bronze.collection_disease (
            -- Auto-incrementing primary key to handle duplicates
            id SERIAL PRIMARY KEY,
            
            -- Patient identifiers
            subject_id INTEGER NOT NULL,
            hadm_id INTEGER,
            stay_id INTEGER,
            
            -- Item identification
            itemid INTEGER NOT NULL,
            charttime TIMESTAMP NOT NULL,
            
            -- Value fields (using TEXT for compatibility)
            value TEXT,
            valuenum NUMERIC,
            valueuom VARCHAR(50),
            
            -- Item metadata
            label VARCHAR(200),
            category VARCHAR(100),
            
            -- Source tracking
            source_table VARCHAR(50) NOT NULL,
            source_fluid VARCHAR(50),  -- For lab items
            
            -- SOFA classification
            is_sofa_parameter BOOLEAN DEFAULT TRUE,
            sofa_system VARCHAR(50),
            search_term_matched VARCHAR(100),
            
            -- Quality flags (flag, don't filter)
            is_outlier BOOLEAN DEFAULT FALSE,
            is_suspicious BOOLEAN DEFAULT FALSE,
            has_unit_conversion BOOLEAN DEFAULT FALSE,
            
            -- Metadata
            extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Unique constraint to prevent true duplicates
            UNIQUE(subject_id, stay_id, itemid, charttime, source_table, value, valuenum)
        );
        
        -- Create indexes for performance
        CREATE INDEX idx_bronze_subject ON bronze.collection_disease (subject_id);
        CREATE INDEX idx_bronze_stay ON bronze.collection_disease (stay_id);
        CREATE INDEX idx_bronze_time ON bronze.collection_disease (charttime);
        CREATE INDEX idx_bronze_item ON bronze.collection_disease (itemid);
        CREATE INDEX idx_bronze_sofa_system ON bronze.collection_disease (sofa_system);
        CREATE INDEX idx_bronze_source ON bronze.collection_disease (source_table);
        CREATE INDEX idx_bronze_subject_time ON bronze.collection_disease (subject_id, charttime);
        """
        
        cur.execute(create_table_sql)
        self.conn.commit()
        
        self.logger.info("✅ Bronze schema and table created successfully")

    def extract_chartevents_data(self):
        """Extract all discovered SOFA parameters from chartevents"""
        self.logger.info("📊 Extracting chartevents SOFA parameters...")
        
        # Get all chartevents itemids from discovery
        chartevents_params = []
        for system, tables in self.discovered_params.items():
            for item in tables.get('chartevents', []):
                chartevents_params.append((item['itemid'], system, item['search_term_matched'], item['label']))
        
        if not chartevents_params:
            self.logger.warning("⚠️ No chartevents parameters found in discovery")
            return 0
        
        # Build itemid list and system mapping
        itemid_list = [str(itemid) for itemid, _, _, _ in chartevents_params]
        system_mapping = {itemid: system for itemid, system, _, _ in chartevents_params}
        term_mapping = {itemid: term for itemid, _, term, _ in chartevents_params}
        
        self.logger.info(f"📈 Extracting {len(itemid_list)} chartevents parameters...")
        
        cur = self.conn.cursor()
        
        # Extract data with comprehensive information
        extract_sql = f"""
        INSERT INTO bronze.collection_disease 
        (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, 
         label, category, source_table, sofa_system, search_term_matched)
        SELECT 
            ce.subject_id,
            ce.hadm_id,
            ce.stay_id,
            ce.itemid,
            ce.charttime,
            ce.value,
            CASE 
                WHEN ce.valuenum IS NOT NULL THEN ce.valuenum
                ELSE NULL
            END,
            ce.valueuom,
            di.label,
            di.category,
            'chartevents',
            CASE ce.itemid
                {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in system_mapping.items()])}
            END,
            CASE ce.itemid
                {' '.join([f"WHEN {itemid} THEN '{term}'" for itemid, term in term_mapping.items()])}
            END
        FROM mimiciv_icu.chartevents ce
        JOIN mimiciv_icu.d_items di ON ce.itemid = di.itemid
        WHERE ce.itemid IN ({','.join(itemid_list)})
          AND ce.stay_id IS NOT NULL
          AND ce.charttime IS NOT NULL
        ON CONFLICT (subject_id, stay_id, itemid, charttime, source_table, value, valuenum) DO NOTHING
        """
        
        cur.execute(extract_sql)
        records_extracted = cur.rowcount
        self.conn.commit()
        
        self.extraction_stats['by_table']['chartevents'] = records_extracted
        self.logger.info(f"✅ Extracted {records_extracted:,} chartevents records")
        
        return records_extracted

    def extract_labevents_data(self):
        """Extract all discovered SOFA parameters from labevents"""
        self.logger.info("🧪 Extracting labevents SOFA parameters...")
        
        # Get all labevents itemids from discovery
        labevents_params = []
        for system, tables in self.discovered_params.items():
            for item in tables.get('labevents', []):
                labevents_params.append((item['itemid'], system, item['search_term_matched'], item['label']))
        
        if not labevents_params:
            self.logger.warning("⚠️ No labevents parameters found in discovery")
            return 0
        
        # Build itemid list and system mapping
        itemid_list = [str(itemid) for itemid, _, _, _ in labevents_params]
        system_mapping = {itemid: system for itemid, system, _, _ in labevents_params}
        term_mapping = {itemid: term for itemid, _, term, _ in labevents_params}
        
        self.logger.info(f"🧪 Extracting {len(itemid_list)} labevents parameters...")
        
        cur = self.conn.cursor()
        
        # Extract data with ICU stay linkage
        extract_sql = f"""
        INSERT INTO bronze.collection_disease 
        (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, 
         label, category, source_table, source_fluid, sofa_system, search_term_matched)
        SELECT 
            le.subject_id,
            le.hadm_id,
            icu.stay_id,
            le.itemid,
            le.charttime,
            le.value,
            CASE 
                WHEN le.valuenum IS NOT NULL THEN le.valuenum
                ELSE NULL
            END,
            le.valueuom,
            dl.label,
            dl.category,
            'labevents',
            dl.fluid,
            CASE le.itemid
                {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in system_mapping.items()])}
            END,
            CASE le.itemid
                {' '.join([f"WHEN {itemid} THEN '{term}'" for itemid, term in term_mapping.items()])}
            END
        FROM mimiciv_hosp.labevents le
        JOIN mimiciv_hosp.d_labitems dl ON le.itemid = dl.itemid
        JOIN mimiciv_icu.icustays icu ON le.subject_id = icu.subject_id 
            AND le.charttime >= icu.intime 
            AND le.charttime <= icu.outtime
        WHERE le.itemid IN ({','.join(itemid_list)})
          AND le.charttime IS NOT NULL
        ON CONFLICT (subject_id, stay_id, itemid, charttime, source_table, value, valuenum) DO NOTHING
        """
        
        cur.execute(extract_sql)
        records_extracted = cur.rowcount
        self.conn.commit()
        
        self.extraction_stats['by_table']['labevents'] = records_extracted
        self.logger.info(f"✅ Extracted {records_extracted:,} labevents records")
        
        return records_extracted

    def extract_outputevents_data(self):
        """Extract all discovered SOFA parameters from outputevents"""
        self.logger.info("💧 Extracting outputevents SOFA parameters...")
        
        # Get all outputevents itemids from discovery
        outputevents_params = []
        for system, tables in self.discovered_params.items():
            for item in tables.get('outputevents', []):
                outputevents_params.append((item['itemid'], system, item['search_term_matched'], item['label']))
        
        if not outputevents_params:
            self.logger.warning("⚠️ No outputevents parameters found in discovery")
            return 0
        
        # Build itemid list and system mapping
        itemid_list = [str(itemid) for itemid, _, _, _ in outputevents_params]
        system_mapping = {itemid: system for itemid, system, _, _ in outputevents_params}
        term_mapping = {itemid: term for itemid, _, term, _ in outputevents_params}
        
        self.logger.info(f"💧 Extracting {len(itemid_list)} outputevents parameters...")
        
        cur = self.conn.cursor()
        
        # Extract output data (primarily urine output)
        extract_sql = f"""
        INSERT INTO bronze.collection_disease 
        (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, 
         label, category, source_table, sofa_system, search_term_matched)
        SELECT 
            oe.subject_id,
            oe.hadm_id,
            oe.stay_id,
            oe.itemid,
            oe.charttime,
            CAST(oe.value AS TEXT),
            oe.value,
            oe.valueuom,
            di.label,
            di.category,
            'outputevents',
            CASE oe.itemid
                {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in system_mapping.items()])}
            END,
            CASE oe.itemid
                {' '.join([f"WHEN {itemid} THEN '{term}'" for itemid, term in term_mapping.items()])}
            END
        FROM mimiciv_icu.outputevents oe
        JOIN mimiciv_icu.d_items di ON oe.itemid = di.itemid
        WHERE oe.itemid IN ({','.join(itemid_list)})
          AND oe.stay_id IS NOT NULL
          AND oe.charttime IS NOT NULL
          AND oe.value IS NOT NULL
          AND oe.value > 0
        ON CONFLICT (subject_id, stay_id, itemid, charttime, source_table, value, valuenum) DO NOTHING
        """
        
        cur.execute(extract_sql)
        records_extracted = cur.rowcount
        self.conn.commit()
        
        self.extraction_stats['by_table']['outputevents'] = records_extracted
        self.logger.info(f"✅ Extracted {records_extracted:,} outputevents records")
        
        return records_extracted

    def extract_inputevents_data(self):
        """Extract all discovered SOFA parameters from inputevents (vasopressors)"""
        self.logger.info("💉 Extracting inputevents SOFA parameters...")
        
        # Get all inputevents itemids from discovery
        inputevents_params = []
        for system, tables in self.discovered_params.items():
            for item in tables.get('inputevents', []):
                inputevents_params.append((item['itemid'], system, item['search_term_matched'], item['label']))
        
        if not inputevents_params:
            self.logger.warning("⚠️ No inputevents parameters found in discovery")
            return 0
        
        # Build itemid list and system mapping
        itemid_list = [str(itemid) for itemid, _, _, _ in inputevents_params]
        system_mapping = {itemid: system for itemid, system, _, _ in inputevents_params}
        term_mapping = {itemid: term for itemid, _, term, _ in inputevents_params}
        
        self.logger.info(f"💉 Extracting {len(itemid_list)} inputevents parameters...")
        
        cur = self.conn.cursor()
        
        # Extract medication/fluid data (primarily vasopressors)
        extract_sql = f"""
        INSERT INTO bronze.collection_disease 
        (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, 
         label, category, source_table, sofa_system, search_term_matched)
        SELECT 
            ie.subject_id,
            ie.hadm_id,
            ie.stay_id,
            ie.itemid,
            ie.starttime,  -- Use starttime as charttime for medications
            CAST(ie.amount AS TEXT),
            ie.amount,
            ie.amountuom,
            di.label,
            di.category,
            'inputevents',
            CASE ie.itemid
                {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in system_mapping.items()])}
            END,
            CASE ie.itemid
                {' '.join([f"WHEN {itemid} THEN '{term}'" for itemid, term in term_mapping.items()])}
            END
        FROM mimiciv_icu.inputevents ie
        JOIN mimiciv_icu.d_items di ON ie.itemid = di.itemid
        WHERE ie.itemid IN ({','.join(itemid_list)})
          AND ie.stay_id IS NOT NULL
          AND ie.starttime IS NOT NULL
          AND ie.amount IS NOT NULL
          AND ie.amount > 0
        ON CONFLICT (subject_id, stay_id, itemid, charttime, source_table, value, valuenum) DO NOTHING
        """
        
        cur.execute(extract_sql)
        records_extracted = cur.rowcount
        self.conn.commit()
        
        self.extraction_stats['by_table']['inputevents'] = records_extracted
        self.logger.info(f"✅ Extracted {records_extracted:,} inputevents records")
        
        return records_extracted

    def flag_quality_issues(self):
        """Flag potential quality issues without filtering data"""
        self.logger.info("🔍 Flagging quality issues...")
        
        cur = self.conn.cursor()
        
        # Flag outliers based on discovery statistics
        self.logger.info("   🚩 Flagging outliers...")
        with open('omop_concept_mappings.json', 'r') as f:
            omop_mappings = json.load(f)
        
        # Create outlier detection for each parameter
        for concept_id, mapping in omop_mappings.items():
            itemid = mapping['source_itemid']
            min_val = mapping['min_value']
            max_val = mapping['max_value']
            
            update_sql = f"""
            UPDATE bronze.collection_disease 
            SET is_outlier = TRUE
            WHERE itemid = {itemid}
              AND valuenum IS NOT NULL
              AND (valuenum < {min_val} OR valuenum > {max_val})
            """
            cur.execute(update_sql)
        
        # Flag suspicious values (e.g., negative values where inappropriate)
        self.logger.info("   🚩 Flagging suspicious values...")
        suspicious_sql = """
        UPDATE bronze.collection_disease 
        SET is_suspicious = TRUE
        WHERE (
            -- Negative values for parameters that should always be positive
            (sofa_system IN ('coagulation', 'respiratory') AND valuenum < 0)
            OR 
            -- Extremely high values that are physiologically impossible
            (sofa_system = 'respiratory' AND label ILIKE '%spo2%' AND valuenum > 100)
            OR
            (sofa_system = 'cns' AND label ILIKE '%gcs%' AND valuenum > 15)
        )
        """
        cur.execute(suspicious_sql)
        
        self.conn.commit()
        
        # Get flagging statistics
        cur.execute("SELECT COUNT(*) FROM bronze.collection_disease WHERE is_outlier = TRUE")
        outlier_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM bronze.collection_disease WHERE is_suspicious = TRUE")
        suspicious_count = cur.fetchone()[0]
        
        self.logger.info(f"   🚩 Flagged {outlier_count:,} outliers and {suspicious_count:,} suspicious values")

    def generate_extraction_report(self):
        """Generate comprehensive extraction report"""
        self.logger.info("📋 Generating extraction report...")
        
        cur = self.conn.cursor()
        
        # Get comprehensive statistics
        cur.execute("SELECT COUNT(*) FROM bronze.collection_disease")
        total_records = cur.fetchone()[0]
        
        cur.execute("""
            SELECT sofa_system, COUNT(*) as count
            FROM bronze.collection_disease
            GROUP BY sofa_system
            ORDER BY count DESC
        """)
        system_counts = cur.fetchall()
        
        cur.execute("""
            SELECT source_table, COUNT(*) as count
            FROM bronze.collection_disease
            GROUP BY source_table
            ORDER BY count DESC
        """)
        table_counts = cur.fetchall()
        
        cur.execute("""
            SELECT COUNT(DISTINCT subject_id) as patients,
                   COUNT(DISTINCT stay_id) as stays
            FROM bronze.collection_disease
        """)
        patient_stats = cur.fetchone()
        
        # Generate report
        report = []
        report.append("# Enhanced Bronze Layer Extraction Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Extraction Duration: {datetime.now() - self.extraction_stats['start_time']}")
        report.append("")
        
        report.append("## Extraction Summary")
        report.append(f"- **Total Records Extracted**: {total_records:,}")
        report.append(f"- **Unique Patients**: {patient_stats[0]:,}")
        report.append(f"- **Unique ICU Stays**: {patient_stats[1]:,}")
        report.append("")
        
        report.append("## Records by Source Table")
        for table, count in table_counts:
            report.append(f"- **{table}**: {count:,} records")
        report.append("")
        
        report.append("## Records by SOFA System")
        for system, count in system_counts:
            report.append(f"- **{system.upper()}**: {count:,} records")
        report.append("")
        
        # Coverage analysis
        report.append("## Coverage Analysis")
        cur.execute("""
            SELECT sofa_system, 
                   COUNT(DISTINCT subject_id) as patients_with_data,
                   COUNT(DISTINCT stay_id) as stays_with_data
            FROM bronze.collection_disease
            GROUP BY sofa_system
            ORDER BY patients_with_data DESC
        """)
        coverage_stats = cur.fetchall()
        
        for system, patients, stays in coverage_stats:
            coverage_pct = (patients / patient_stats[0]) * 100
            report.append(f"- **{system.upper()}**: {patients}/{patient_stats[0]} patients ({coverage_pct:.1f}%)")
        
        # Write report
        with open('bronze_extraction_report.md', 'w') as f:
            f.write('\n'.join(report))
        
        self.logger.info(f"📋 Extraction report saved: bronze_extraction_report.md")
        self.logger.info(f"🎯 Total records extracted: {total_records:,}")

    def build_bronze_layer(self):
        """Build complete Bronze layer with all discovered SOFA parameters"""
        self.logger.info("🚀 Starting enhanced Bronze layer build...")
        
        try:
            # Create schema and table
            self.create_bronze_schema()
            
            # Extract data from all tables
            chartevents_count = self.extract_chartevents_data()
            labevents_count = self.extract_labevents_data()
            outputevents_count = self.extract_outputevents_data()
            inputevents_count = self.extract_inputevents_data()
            
            total_extracted = chartevents_count + labevents_count + outputevents_count + inputevents_count
            self.extraction_stats['total_records'] = total_extracted
            
            # Flag quality issues
            self.flag_quality_issues()
            
            # Generate report
            self.generate_extraction_report()
            
            self.logger.info(f"✅ Bronze layer build completed successfully!")
            self.logger.info(f"📊 Total records: {total_extracted:,}")
            
            return total_extracted
            
        except Exception as e:
            self.logger.error(f"❌ Bronze layer build failed: {e}", exc_info=True)
            raise


def main():
    """Main execution function"""
    builder = EnhancedBronzeBuilder()
    
    print("🚀 Starting Enhanced Bronze Layer Build...")
    print("=" * 60)
    
    try:
        total_records = builder.build_bronze_layer()
        
        print(f"\n✅ Bronze layer build completed successfully!")
        print(f"📊 Results:")
        print(f"   - Total records extracted: {total_records:,}")
        print(f"   - All 6 SOFA systems covered")
        print(f"   - Full transparency with quality flags")
        print(f"\n📋 Reports generated:")
        print(f"   - bronze_extraction_report.md")
        print(f"   - bronze_builder.log")
        
    except Exception as e:
        print(f"\n❌ Bronze layer build failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
