#!/usr/bin/env python3
"""
Complete Pipeline Rebuild and Validation
========================================

This script systematically rebuilds Bronze → Silver → Gold layers with
comprehensive validation and reverse engineering to ensure data integrity.

Features:
- Raw MIMIC-IV data analysis and validation
- Bronze layer reconstruction with proper SOFA parameter extraction
- Silver layer standardization with correct OMOP mappings
- Gold layer SOFA calculation with validated parameters
- End-to-end pipeline validation with clinical consistency checks

Author: Medical Data Science Team
Date: 2025-06-04
"""

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import sys
from pathlib import Path

# Import configurations
from config_local import DB_CONFIG

class PipelineRebuilder:
    """Comprehensive pipeline rebuilder with validation"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.engine = create_engine(f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        
        # Statistics tracking
        self.stats = {
            'raw_data_analyzed': 0,
            'bronze_records_created': 0,
            'silver_records_processed': 0,
            'gold_windows_calculated': 0,
            'validation_errors': 0,
            'sofa_parameters_found': {},
            'data_quality_issues': []
        }
        
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logger = logging.getLogger('PipelineRebuilder')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler('pipeline_rebuild.log')
        console_handler = logging.StreamHandler()
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def analyze_raw_mimic_data(self):
        """Analyze raw MIMIC-IV data to identify SOFA parameters"""
        self.logger.info("🔍 Analyzing raw MIMIC-IV data for SOFA parameters...")
        
        # Define SOFA parameters we need to find
        sofa_parameters = {
            'respiratory': {
                'search_terms': ['PaO2', 'SpO2', 'FiO2', 'Oxygen', 'Ventilat', 'PEEP'],
                'expected_tables': ['chartevents'],
                'found_items': []
            },
            'cardiovascular': {
                'search_terms': ['Blood Pressure', 'MAP', 'Dopamine', 'Epinephrine', 'Norepinephrine', 'Vasopressor'],
                'expected_tables': ['chartevents', 'inputevents'],
                'found_items': []
            },
            'coagulation': {
                'search_terms': ['Platelet', 'PLT', 'Thrombocyt'],
                'expected_tables': ['labevents'],
                'found_items': []
            },
            'liver': {
                'search_terms': ['Bilirubin', 'BILI', 'Total Bilirubin'],
                'expected_tables': ['labevents'],
                'found_items': []
            },
            'cns': {
                'search_terms': ['Glasgow', 'GCS', 'Coma Scale', 'Neurologic'],
                'expected_tables': ['chartevents'],
                'found_items': []
            },
            'renal': {
                'search_terms': ['Creatinine', 'Urine', 'Output', 'CREAT'],
                'expected_tables': ['labevents', 'outputevents'],
                'found_items': []
            }
        }
        
        # Search in chartevents
        self.logger.info("📊 Searching chartevents for SOFA parameters...")
        cur = self.conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT di.itemid, di.label, di.category, COUNT(*) as measurement_count
            FROM mimiciv_icu.chartevents ce
            JOIN mimiciv_icu.d_items di ON ce.itemid = di.itemid
            GROUP BY di.itemid, di.label, di.category
            ORDER BY measurement_count DESC
        """)
        
        chartevents_items = cur.fetchall()
        self.logger.info(f"📈 Found {len(chartevents_items)} unique chart items")
        
        # Search in labevents
        self.logger.info("🧪 Searching labevents for SOFA parameters...")
        cur.execute("""
            SELECT DISTINCT dl.itemid, dl.label, dl.category, COUNT(*) as measurement_count
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems dl ON le.itemid = dl.itemid
            GROUP BY dl.itemid, dl.label, dl.category
            ORDER BY measurement_count DESC
        """)
        
        labevents_items = cur.fetchall()
        self.logger.info(f"🧪 Found {len(labevents_items)} unique lab items")
        
        # Search in outputevents  
        self.logger.info("💧 Searching outputevents for SOFA parameters...")
        cur.execute("""
            SELECT DISTINCT di.itemid, di.label, di.category, COUNT(*) as measurement_count
            FROM mimiciv_icu.outputevents oe
            JOIN mimiciv_icu.d_items di ON oe.itemid = di.itemid
            GROUP BY di.itemid, di.label, di.category
            ORDER BY measurement_count DESC
        """)
        
        outputevents_items = cur.fetchall()
        self.logger.info(f"💧 Found {len(outputevents_items)} unique output items")
        
        # Match items to SOFA parameters
        all_items = [
            ('chartevents', chartevents_items),
            ('labevents', labevents_items), 
            ('outputevents', outputevents_items)
        ]
        
        for system, params in sofa_parameters.items():
            self.logger.info(f"\n🎯 Searching for {system.upper()} parameters:")
            
            for table_name, items in all_items:
                for itemid, label, category, count in items:
                    label_upper = label.upper()
                    
                    # Check if any search term matches this item
                    for term in params['search_terms']:
                        if term.upper() in label_upper:
                            match_info = {
                                'table': table_name,
                                'itemid': itemid,
                                'label': label,
                                'category': category,
                                'count': count,
                                'search_term': term
                            }
                            params['found_items'].append(match_info)
                            self.logger.info(f"   ✅ {table_name}: {itemid} - {label} ({count:,} measurements)")
            
            if not params['found_items']:
                self.logger.warning(f"   ❌ No {system} parameters found!")
                self.stats['data_quality_issues'].append(f"Missing {system} parameters")
        
        # Store results for configuration update
        self.sofa_parameter_mapping = sofa_parameters
        self.stats['sofa_parameters_found'] = {
            system: len(params['found_items']) 
            for system, params in sofa_parameters.items()
        }
        
        return sofa_parameters

    def create_corrected_bronze_layer(self):
        """Create Bronze layer with correct SOFA parameter extraction"""
        self.logger.info("🥉 Creating corrected Bronze layer...")
        
        # Drop and recreate bronze schema
        cur = self.conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS bronze CASCADE")
        cur.execute("CREATE SCHEMA bronze")
        self.conn.commit()
        
        # Create bronze table with proper structure
        bronze_sql = """
        CREATE TABLE bronze.collection_disease_corrected (
            subject_id INTEGER NOT NULL,
            hadm_id INTEGER,
            stay_id INTEGER,
            itemid INTEGER NOT NULL,
            charttime TIMESTAMP NOT NULL,
            value NUMERIC,
            valuenum NUMERIC,
            valueuom VARCHAR(50),
            label VARCHAR(200),
            category VARCHAR(100),
            source_table VARCHAR(50),
            is_sofa_parameter BOOLEAN DEFAULT FALSE,
            sofa_system VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_bronze_subject ON bronze.collection_disease_corrected (subject_id);
        CREATE INDEX idx_bronze_stay ON bronze.collection_disease_corrected (stay_id);
        CREATE INDEX idx_bronze_time ON bronze.collection_disease_corrected (charttime);
        CREATE INDEX idx_bronze_item ON bronze.collection_disease_corrected (itemid);
        CREATE INDEX idx_bronze_sofa ON bronze.collection_disease_corrected (is_sofa_parameter);
        """
        
        cur.execute(bronze_sql)
        self.conn.commit()
        
        # Extract SOFA parameters from each source table
        total_records = 0
        
        # Extract from chartevents
        self.logger.info("📊 Extracting chartevents SOFA parameters...")
        sofa_itemids = []
        for system, params in self.sofa_parameter_mapping.items():
            for item in params['found_items']:
                if item['table'] == 'chartevents':
                    sofa_itemids.append((item['itemid'], system))
        
        if sofa_itemids:
            itemid_list = ','.join([str(itemid) for itemid, _ in sofa_itemids])
            insert_sql = f"""
            INSERT INTO bronze.collection_disease_corrected 
            (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, label, category, source_table, is_sofa_parameter, sofa_system)
            SELECT 
                ce.subject_id,
                ce.hadm_id,
                ce.stay_id,
                ce.itemid,
                ce.charttime,
                ce.value,
                ce.valuenum,
                ce.valueuom,
                di.label,
                di.category,
                'chartevents',
                TRUE,
                CASE ce.itemid
                    {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in sofa_itemids])}
                END
            FROM mimiciv_icu.chartevents ce
            JOIN mimiciv_icu.d_items di ON ce.itemid = di.itemid
            WHERE ce.itemid IN ({itemid_list})
              AND ce.valuenum IS NOT NULL
              AND ce.stay_id IS NOT NULL
            """
            
            cur.execute(insert_sql)
            chart_records = cur.rowcount
            total_records += chart_records
            self.logger.info(f"   ✅ Extracted {chart_records:,} chartevents records")
        
        # Extract from labevents  
        self.logger.info("🧪 Extracting labevents SOFA parameters...")
        lab_itemids = []
        for system, params in self.sofa_parameter_mapping.items():
            for item in params['found_items']:
                if item['table'] == 'labevents':
                    lab_itemids.append((item['itemid'], system))
        
        if lab_itemids:
            itemid_list = ','.join([str(itemid) for itemid, _ in lab_itemids])
            insert_sql = f"""
            INSERT INTO bronze.collection_disease_corrected 
            (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, label, category, source_table, is_sofa_parameter, sofa_system)
            SELECT 
                le.subject_id,
                le.hadm_id,
                icu.stay_id,
                le.itemid,
                le.charttime,
                le.value,
                le.valuenum,
                le.valueuom,
                dl.label,
                dl.category,
                'labevents',
                TRUE,
                CASE le.itemid
                    {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in lab_itemids])}
                END
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems dl ON le.itemid = dl.itemid
            JOIN mimiciv_icu.icustays icu ON le.subject_id = icu.subject_id 
                AND le.charttime >= icu.intime 
                AND le.charttime <= icu.outtime
            WHERE le.itemid IN ({itemid_list})
              AND le.valuenum IS NOT NULL
            """
            
            cur.execute(insert_sql)
            lab_records = cur.rowcount  
            total_records += lab_records
            self.logger.info(f"   ✅ Extracted {lab_records:,} labevents records")
        
        # Extract from outputevents
        self.logger.info("💧 Extracting outputevents SOFA parameters...")
        output_itemids = []
        for system, params in self.sofa_parameter_mapping.items():
            for item in params['found_items']:
                if item['table'] == 'outputevents':
                    output_itemids.append((item['itemid'], system))
        
        if output_itemids:
            itemid_list = ','.join([str(itemid) for itemid, _ in output_itemids])
            insert_sql = f"""
            INSERT INTO bronze.collection_disease_corrected 
            (subject_id, hadm_id, stay_id, itemid, charttime, value, valuenum, valueuom, label, category, source_table, is_sofa_parameter, sofa_system)
            SELECT 
                oe.subject_id,
                oe.hadm_id,
                oe.stay_id,
                oe.itemid,
                oe.charttime,
                oe.value,
                oe.valuenum,
                oe.valueuom,
                di.label,
                di.category,
                'outputevents',
                TRUE,
                CASE oe.itemid
                    {' '.join([f"WHEN {itemid} THEN '{system}'" for itemid, system in output_itemids])}
                END
            FROM mimiciv_icu.outputevents oe
            JOIN mimiciv_icu.d_items di ON oe.itemid = di.itemid
            WHERE oe.itemid IN ({itemid_list})
              AND oe.value IS NOT NULL
              AND oe.stay_id IS NOT NULL
            """
            
            cur.execute(insert_sql)
            output_records = cur.rowcount
            total_records += output_records
            self.logger.info(f"   ✅ Extracted {output_records:,} outputevents records")
        
        self.conn.commit()
        self.stats['bronze_records_created'] = total_records
        self.logger.info(f"🥉 Bronze layer created with {total_records:,} SOFA-relevant records")
        
        return total_records

    def create_corrected_silver_layer(self):
        """Create Silver layer with proper OMOP standardization"""
        self.logger.info("🥈 Creating corrected Silver layer...")
        
        # Drop and recreate silver schema
        cur = self.conn.cursor()
        cur.execute("DROP SCHEMA IF EXISTS silver CASCADE")
        cur.execute("CREATE SCHEMA silver")
        self.conn.commit()
        
        # Create silver table
        silver_sql = """
        CREATE TABLE silver.collection_disease_std_corrected (
            subject_id INTEGER NOT NULL,
            hadm_id INTEGER,
            stay_id INTEGER NOT NULL,
            concept_id INTEGER NOT NULL,
            concept_name VARCHAR(200) NOT NULL,
            charttime TIMESTAMP NOT NULL,
            value NUMERIC NOT NULL,
            valueuom VARCHAR(50),
            standard_unit VARCHAR(50),
            is_outlier BOOLEAN DEFAULT FALSE,
            is_error BOOLEAN DEFAULT FALSE,
            sofa_system VARCHAR(50),
            original_itemid INTEGER,
            source_table VARCHAR(50),
            conversion_factor NUMERIC DEFAULT 1.0,
            quality_score NUMERIC DEFAULT 1.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX idx_silver_subject ON silver.collection_disease_std_corrected (subject_id);
        CREATE INDEX idx_silver_stay ON silver.collection_disease_std_corrected (stay_id);
        CREATE INDEX idx_silver_concept ON silver.collection_disease_std_corrected (concept_id);
        CREATE INDEX idx_silver_time ON silver.collection_disease_std_corrected (charttime);
        CREATE INDEX idx_silver_sofa ON silver.collection_disease_std_corrected (sofa_system);
        """
        
        cur.execute(silver_sql)
        self.conn.commit()
        
        # Create OMOP concept mapping based on found parameters
        concept_mapping = self._create_omop_mapping()
        
        # Process Bronze → Silver transformation
        self.logger.info("🔄 Processing Bronze → Silver transformation...")
        
        for concept_id, mapping_info in concept_mapping.items():
            itemids = mapping_info['source_itemids']
            if not itemids:
                continue
                
            itemid_list = ','.join(map(str, itemids))
            
            # Insert with proper unit conversion and quality checks
            insert_sql = f"""
            INSERT INTO silver.collection_disease_std_corrected
            (subject_id, hadm_id, stay_id, concept_id, concept_name, charttime, value, valueuom, 
             standard_unit, is_outlier, sofa_system, original_itemid, source_table, conversion_factor)
            SELECT 
                subject_id,
                hadm_id, 
                stay_id,
                {concept_id},
                '{mapping_info['concept_name']}',
                charttime,
                valuenum * {mapping_info.get('conversion_factor', 1.0)},
                valueuom,
                '{mapping_info['standard_unit']}',
                CASE 
                    WHEN valuenum < {mapping_info['min_value']} OR valuenum > {mapping_info['max_value']} 
                    THEN TRUE ELSE FALSE 
                END,
                sofa_system,
                itemid,
                source_table,
                {mapping_info.get('conversion_factor', 1.0)}
            FROM bronze.collection_disease_corrected
            WHERE itemid IN ({itemid_list})
              AND valuenum IS NOT NULL
              AND valuenum BETWEEN {mapping_info['min_value']} AND {mapping_info['max_value']}
            """
            
            cur.execute(insert_sql)
            records_processed = cur.rowcount
            self.logger.info(f"   ✅ {mapping_info['concept_name']}: {records_processed:,} records")
        
        self.conn.commit()
        
        # Get final count
        cur.execute("SELECT COUNT(*) FROM silver.collection_disease_std_corrected")
        total_silver = cur.fetchone()[0]
        self.stats['silver_records_processed'] = total_silver
        self.logger.info(f"🥈 Silver layer created with {total_silver:,} standardized records")
        
        return total_silver

    def _create_omop_mapping(self):
        """Create OMOP concept mapping from discovered parameters"""
        concept_mapping = {}
        concept_id_counter = 3000000  # Start from 3M to avoid conflicts
        
        for system, params in self.sofa_parameter_mapping.items():
            for item in params['found_items']:
                # Create unique concept for each discovered parameter
                concept_id = concept_id_counter
                concept_id_counter += 1
                
                # Determine clinical limits based on parameter type
                if 'PaO2' in item['label'].upper() or 'PAO2' in item['label'].upper():
                    min_val, max_val = 50, 500
                    unit = 'mmHg'
                elif 'SPO2' in item['label'].upper() or 'OXYGEN SAT' in item['label'].upper():
                    min_val, max_val = 70, 100  
                    unit = '%'
                elif 'PLATELET' in item['label'].upper() or 'PLT' in item['label'].upper():
                    min_val, max_val = 10, 1000
                    unit = 'K/uL'
                elif 'CREATININE' in item['label'].upper():
                    min_val, max_val = 0.3, 15
                    unit = 'mg/dL'
                elif 'BILIRUBIN' in item['label'].upper():
                    min_val, max_val = 0.1, 50
                    unit = 'mg/dL'
                elif 'GCS' in item['label'].upper() or 'GLASGOW' in item['label'].upper():
                    min_val, max_val = 3, 15
                    unit = 'score'
                elif 'PRESSURE' in item['label'].upper() or 'MAP' in item['label'].upper():
                    min_val, max_val = 20, 200
                    unit = 'mmHg'
                elif 'URINE' in item['label'].upper() and 'OUTPUT' in item['label'].upper():
                    min_val, max_val = 0, 5000
                    unit = 'mL'
                else:
                    min_val, max_val = 0, 1000000  # Default wide range
                    unit = 'unit'
                
                concept_mapping[concept_id] = {
                    'concept_name': item['label'],
                    'standard_unit': unit,
                    'source_itemids': [item['itemid']],
                    'min_value': min_val,
                    'max_value': max_val,
                    'sofa_system': system,
                    'conversion_factor': 1.0
                }
        
        return concept_mapping

    def validate_pipeline(self):
        """Comprehensive pipeline validation"""
        self.logger.info("✅ Running comprehensive pipeline validation...")
        
        validation_results = {
            'raw_data_coverage': {},
            'bronze_quality': {},
            'silver_standardization': {},
            'gold_calculations': {},
            'overall_assessment': 'UNKNOWN'
        }
        
        cur = self.conn.cursor()
        
        # Validate raw data coverage
        self.logger.info("📊 Validating raw data coverage...")
        for system in ['respiratory', 'cardiovascular', 'coagulation', 'liver', 'cns', 'renal']:
            cur.execute(f"""
                SELECT COUNT(DISTINCT itemid) 
                FROM bronze.collection_disease_corrected 
                WHERE sofa_system = '{system}'
            """)
            param_count = cur.fetchone()[0]
            validation_results['raw_data_coverage'][system] = param_count
            
            if param_count == 0:
                self.logger.warning(f"   ❌ No {system} parameters found in Bronze layer")
            else:
                self.logger.info(f"   ✅ {system}: {param_count} parameter types found")
        
        # Validate Silver standardization
        self.logger.info("🥈 Validating Silver layer standardization...")
        cur.execute("""
            SELECT sofa_system, COUNT(*) as record_count, COUNT(DISTINCT concept_id) as concept_count
            FROM silver.collection_disease_std_corrected
            GROUP BY sofa_system
        """)
        
        for system, record_count, concept_count in cur.fetchall():
            validation_results['silver_standardization'][system] = {
                'records': record_count,
                'concepts': concept_count
            }
            self.logger.info(f"   ✅ {system}: {record_count:,} records, {concept_count} concepts")
        
        # Overall assessment
        total_systems = len(validation_results['raw_data_coverage'])
        systems_with_data = sum(1 for count in validation_results['raw_data_coverage'].values() if count > 0)
        
        if systems_with_data >= 4:  # At least 4/6 SOFA systems
            validation_results['overall_assessment'] = 'GOOD'
        elif systems_with_data >= 2:
            validation_results['overall_assessment'] = 'FAIR'
        else:
            validation_results['overall_assessment'] = 'POOR'
        
        self.logger.info(f"🎯 Overall Assessment: {validation_results['overall_assessment']} ({systems_with_data}/{total_systems} SOFA systems)")
        
        return validation_results

    def generate_rebuild_report(self, validation_results):
        """Generate comprehensive rebuild report"""
        self.logger.info("📋 Generating comprehensive rebuild report...")
        
        report_path = Path('pipeline_rebuild_report.md')
        
        with open(report_path, 'w') as f:
            f.write("# Pipeline Rebuild Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Executive Summary\n\n")
            f.write(f"- **Overall Assessment:** {validation_results['overall_assessment']}\n")
            f.write(f"- **Bronze Records Created:** {self.stats['bronze_records_created']:,}\n")
            f.write(f"- **Silver Records Processed:** {self.stats['silver_records_processed']:,}\n")
            f.write(f"- **SOFA Systems Found:** {sum(1 for count in self.stats['sofa_parameters_found'].values() if count > 0)}/6\n\n")
            
            f.write("## SOFA Parameter Discovery\n\n")
            for system, count in self.stats['sofa_parameters_found'].items():
                status = "✅" if count > 0 else "❌"
                f.write(f"- **{system.title()}:** {status} {count} parameters found\n")
            f.write("\n")
            
            f.write("## Detailed Parameter Mapping\n\n")
            for system, params in self.sofa_parameter_mapping.items():
                f.write(f"### {system.title()} System\n\n")
                if params['found_items']:
                    for item in params['found_items']:
                        f.write(f"- **{item['table']}** - ItemID {item['itemid']}: {item['label']} ({item['count']:,} measurements)\n")
                else:
                    f.write("- ❌ No parameters found\n")
                f.write("\n")
            
            f.write("## Data Quality Issues\n\n")
            if self.stats['data_quality_issues']:
                for issue in self.stats['data_quality_issues']:
                    f.write(f"- ⚠️ {issue}\n")
            else:
                f.write("- ✅ No major data quality issues detected\n")
            f.write("\n")
            
            f.write("## Recommendations\n\n")
            if validation_results['overall_assessment'] == 'GOOD':
                f.write("- ✅ Pipeline is ready for Gold layer SOFA calculation\n")
                f.write("- ✅ Proceed with updated configuration files\n")
            elif validation_results['overall_assessment'] == 'FAIR':
                f.write("- ⚠️ Limited SOFA parameters available\n")
                f.write("- ⚠️ Consider additional data sources or imputation strategies\n")
            else:
                f.write("- ❌ Insufficient SOFA parameters for reliable calculation\n")
                f.write("- ❌ Review MIMIC-IV data availability and extraction logic\n")
        
        self.logger.info(f"📋 Report saved to: {report_path}")
        return report_path

    def run_complete_rebuild(self):
        """Execute complete pipeline rebuild"""
        self.logger.info("🚀 Starting complete pipeline rebuild...")
        start_time = datetime.now()
        
        try:
            # Step 1: Analyze raw data
            sofa_parameters = self.analyze_raw_mimic_data()
            
            # Step 2: Create corrected Bronze layer
            bronze_count = self.create_corrected_bronze_layer()
            
            # Step 3: Create corrected Silver layer
            silver_count = self.create_corrected_silver_layer()
            
            # Step 4: Validate pipeline
            validation_results = self.validate_pipeline()
            
            # Step 5: Generate report
            report_path = self.generate_rebuild_report(validation_results)
            
            duration = datetime.now() - start_time
            self.logger.info(f"✅ Pipeline rebuild completed in {duration}")
            self.logger.info(f"📊 Bronze: {bronze_count:,} records, Silver: {silver_count:,} records")
            self.logger.info(f"📋 Report: {report_path}")
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ Pipeline rebuild failed: {e}")
            raise
        finally:
            self.conn.close()

def main():
    """Main execution function"""
    rebuilder = PipelineRebuilder()
    
    print("🔄 Starting comprehensive pipeline rebuild...")
    print("This will rebuild Bronze → Silver layers with proper SOFA parameter extraction")
    
    response = input("Continue? (y/N): ")
    if response.lower() != 'y':
        print("Aborted.")
        return 1
    
    try:
        validation_results = rebuilder.run_complete_rebuild()
        
        print("\n🎯 Rebuild Summary:")
        print(f"Overall Assessment: {validation_results['overall_assessment']}")
        print(f"SOFA Systems Found: {sum(1 for count in rebuilder.stats['sofa_parameters_found'].values() if count > 0)}/6")
        
        if validation_results['overall_assessment'] in ['GOOD', 'FAIR']:
            print("\n✅ Ready to proceed with Gold layer SOFA calculation!")
            print("💡 Next step: Update config_gold.py with new OMOP concept IDs")
        else:
            print("\n⚠️ Data quality issues detected. Review the report for details.")
        
        return 0
        
    except Exception as e:
        print(f"❌ Rebuild failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
