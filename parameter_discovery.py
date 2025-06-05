#!/usr/bin/env python3
"""
MIMIC-IV Parameter Discovery Engine
==================================

This module implements comprehensive parameter discovery for SOFA scoring
following the upgrade plan. It analyzes all MIMIC-IV tables to identify
every possible SOFA-relevant parameter and creates dynamic mappings.

Features:
- Dynamic discovery of all SOFA parameters across MIMIC-IV tables
- Comprehensive OMOP concept mapping generation
- Unit variation analysis and harmonization planning
- Coverage assessment and quality reporting
- Transparent logging and traceability

Author: Medical Data Science Team
Date: 2025-06-05
"""

import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Import configurations
from config_local import DB_CONFIG

class SOFAParameterDiscovery:
    """Comprehensive SOFA parameter discovery and mapping engine"""
    
    def __init__(self):
        self.logger = self._setup_logging()
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )
        
        # Discovery results storage
        self.discovered_parameters = {}
        self.omop_mappings = {}
        self.unit_variations = {}
        self.coverage_stats = {}
        
        # Define comprehensive SOFA search terms based on clinical guidelines
        self.sofa_search_terms = {
            'respiratory': {
                'search_terms': [
                    'PaO2', 'PAO2', 'pao2', 'arterial oxygen',
                    'SpO2', 'SPO2', 'spo2', 'oxygen saturation', 'pulse ox',
                    'FiO2', 'FIO2', 'fio2', 'inspired oxygen', 'oxygen concentration',
                    'PEEP', 'peep', 'positive end expiratory',
                    'ventilat', 'mechanical vent', 'vent mode',
                    'respiratory rate', 'resp rate', 'breathing'
                ],
                'expected_tables': ['chartevents', 'labevents'],
                'clinical_ranges': {'pao2': (40, 500), 'spo2': (70, 100), 'fio2': (0.21, 1.0)},
                'standard_units': {'pao2': 'mmHg', 'spo2': '%', 'fio2': 'fraction'},
                'found_items': []
            },
            
            'cardiovascular': {
                'search_terms': [
                    'mean arterial pressure', 'MAP', 'map', 'arterial pressure mean',
                    'blood pressure', 'systolic', 'diastolic', 'BP',
                    'norepinephrine', 'noradrenaline', 'levophed',
                    'epinephrine', 'adrenaline', 'epi',
                    'dopamine', 'dobutamine', 'vasopressin',
                    'phenylephrine', 'neo-synephrine',
                    'vasopressor', 'inotrope', 'pressor'
                ],
                'expected_tables': ['chartevents', 'inputevents'],
                'clinical_ranges': {'map': (40, 150), 'sbp': (70, 250), 'dbp': (30, 150)},
                'standard_units': {'map': 'mmHg', 'vasopressor': 'mcg/kg/min'},
                'found_items': []
            },
            
            'coagulation': {
                'search_terms': [
                    'platelet', 'PLT', 'plt', 'thrombocyte',
                    'platelet count', 'platelets'
                ],
                'expected_tables': ['labevents'],
                'clinical_ranges': {'platelets': (10, 1000)},
                'standard_units': {'platelets': 'K/uL'},
                'found_items': []
            },
            
            'liver': {
                'search_terms': [
                    'bilirubin', 'BILI', 'bili', 'total bilirubin',
                    'direct bilirubin', 'indirect bilirubin',
                    'conjugated bilirubin', 'unconjugated bilirubin'
                ],
                'expected_tables': ['labevents'],
                'clinical_ranges': {'bilirubin': (0.1, 30)},
                'standard_units': {'bilirubin': 'mg/dL'},
                'found_items': []
            },
            
            'cns': {
                'search_terms': [
                    'glasgow coma scale', 'GCS', 'gcs',
                    'glasgow total', 'coma scale',
                    'eye opening', 'verbal response', 'motor response',
                    'neurologic', 'consciousness', 'mental status'
                ],
                'expected_tables': ['chartevents'],
                'clinical_ranges': {'gcs': (3, 15)},
                'standard_units': {'gcs': 'points'},
                'found_items': []
            },
            
            'renal': {
                'search_terms': [
                    'creatinine', 'CREAT', 'creat', 'serum creatinine',
                    'urine output', 'urine volume', 'foley',
                    'urinary output', 'diuresis', 'UO',
                    'kidney function', 'renal function'
                ],
                'expected_tables': ['labevents', 'outputevents'],
                'clinical_ranges': {'creatinine': (0.3, 15), 'urine_output': (0, 500)},
                'standard_units': {'creatinine': 'mg/dL', 'urine_output': 'mL/hr'},
                'found_items': []
            }
        }
        
    def _setup_logging(self):
        """Setup comprehensive logging"""
        logger = logging.getLogger('SOFAParameterDiscovery')
        logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler('parameter_discovery.log')
        console_handler = logging.StreamHandler()
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def analyze_database_structure(self):
        """Analyze MIMIC-IV database structure and table availability"""
        self.logger.info("🔍 Analyzing MIMIC-IV database structure...")
        
        cur = self.conn.cursor()
        
        # Get all schemas and tables
        cur.execute("""
            SELECT schemaname, tablename, tableowner 
            FROM pg_tables 
            WHERE schemaname IN ('mimiciv_icu', 'mimiciv_hosp', 'mimiciv_ed', 'bronze', 'silver', 'gold')
            ORDER BY schemaname, tablename
        """)
        
        tables = cur.fetchall()
        self.logger.info(f"📊 Found {len(tables)} tables across MIMIC-IV schemas")
        
        # Analyze key tables for SOFA parameters
        key_tables = {
            'mimiciv_icu.chartevents': 'ICU vital signs and measurements',
            'mimiciv_hosp.labevents': 'Laboratory test results',
            'mimiciv_icu.outputevents': 'Patient output measurements (urine, etc.)',
            'mimiciv_icu.inputevents': 'Medication and fluid inputs',
            'mimiciv_icu.icustays': 'ICU admission records'
        }
        
        table_stats = {}
        for table_name, description in key_tables.items():
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                table_stats[table_name] = count
                self.logger.info(f"   ✅ {table_name}: {count:,} records - {description}")
            except Exception as e:
                self.logger.warning(f"   ❌ {table_name}: Not accessible - {e}")
                table_stats[table_name] = 0
        
        return table_stats

    def discover_chartevents_parameters(self):
        """Discover all SOFA-relevant parameters in chartevents"""
        self.logger.info("📊 Discovering chartevents SOFA parameters...")
        
        cur = self.conn.cursor()
        
        # Get all unique items with counts and sample values
        discovery_query = """
            SELECT 
                di.itemid,
                di.label,
                di.category,
                di.unitname,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT ce.subject_id) as patient_count,
                COUNT(DISTINCT ce.stay_id) as stay_count,
                MIN(ce.valuenum) as min_value,
                MAX(ce.valuenum) as max_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ce.valuenum) as median_value
            FROM mimiciv_icu.chartevents ce
            JOIN mimiciv_icu.d_items di ON ce.itemid = di.itemid
            WHERE ce.valuenum IS NOT NULL
            GROUP BY di.itemid, di.label, di.category, di.unitname
            HAVING COUNT(*) >= 10  -- Only items with sufficient data
            ORDER BY measurement_count DESC
        """
        
        cur.execute(discovery_query)
        chartevents_items = cur.fetchall()
        
        self.logger.info(f"📈 Found {len(chartevents_items)} unique chartevents items with data")
        
        # Match to SOFA systems
        sofa_matches = {'respiratory': [], 'cardiovascular': [], 'cns': [], 'renal': []}
        
        for item in chartevents_items:
            itemid, label, category, unit, count, patients, stays, min_val, max_val, median = item
            label_upper = label.upper()
            
            # Check each SOFA system
            for system_name, system_config in self.sofa_search_terms.items():
                if system_name in ['coagulation', 'liver']:  # These are primarily in labevents
                    continue
                    
                for search_term in system_config['search_terms']:
                    if search_term.upper() in label_upper:
                        match_info = {
                            'itemid': itemid,
                            'label': label,
                            'category': category,
                            'unit': unit,
                            'table': 'chartevents',
                            'measurement_count': count,
                            'patient_count': patients,
                            'stay_count': stays,
                            'min_value': min_val,
                            'max_value': max_val,
                            'median_value': median,
                            'search_term_matched': search_term,
                            'system': system_name
                        }
                        sofa_matches[system_name].append(match_info)
                        self.logger.info(f"   ✅ {system_name.upper()}: {itemid} - {label} ({count:,} measurements)")
                        break  # Only match once per item
        
        return sofa_matches

    def discover_labevents_parameters(self):
        """Discover all SOFA-relevant parameters in labevents"""
        self.logger.info("🧪 Discovering labevents SOFA parameters...")
        
        cur = self.conn.cursor()
        
        # Get all unique lab items with counts and sample values
        discovery_query = """
            SELECT 
                dl.itemid,
                dl.label,
                dl.category,
                dl.fluid,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT le.subject_id) as patient_count,
                MIN(le.valuenum) as min_value,
                MAX(le.valuenum) as max_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY le.valuenum) as median_value,
                -- Get most common unit
                MODE() WITHIN GROUP (ORDER BY le.valueuom) as common_unit
            FROM mimiciv_hosp.labevents le
            JOIN mimiciv_hosp.d_labitems dl ON le.itemid = dl.itemid
            WHERE le.valuenum IS NOT NULL
            GROUP BY dl.itemid, dl.label, dl.category, dl.fluid
            HAVING COUNT(*) >= 10  -- Only items with sufficient data
            ORDER BY measurement_count DESC
        """
        
        cur.execute(discovery_query)
        labevents_items = cur.fetchall()
        
        self.logger.info(f"🧪 Found {len(labevents_items)} unique labevents items with data")
        
        # Match to SOFA systems (focus on coagulation, liver, respiratory, renal)
        sofa_matches = {'respiratory': [], 'coagulation': [], 'liver': [], 'renal': []}
        
        for item in labevents_items:
            itemid, label, category, fluid, count, patients, min_val, max_val, median, unit = item
            label_upper = label.upper()
            
            # Check each SOFA system
            for system_name in ['respiratory', 'coagulation', 'liver', 'renal']:
                system_config = self.sofa_search_terms[system_name]
                
                for search_term in system_config['search_terms']:
                    if search_term.upper() in label_upper:
                        match_info = {
                            'itemid': itemid,
                            'label': label,
                            'category': category,
                            'fluid': fluid,
                            'unit': unit,
                            'table': 'labevents',
                            'measurement_count': count,
                            'patient_count': patients,
                            'min_value': min_val,
                            'max_value': max_val,
                            'median_value': median,
                            'search_term_matched': search_term,
                            'system': system_name
                        }
                        sofa_matches[system_name].append(match_info)
                        self.logger.info(f"   ✅ {system_name.upper()}: {itemid} - {label} ({count:,} measurements)")
                        break  # Only match once per item
        
        return sofa_matches

    def discover_outputevents_parameters(self):
        """Discover all SOFA-relevant parameters in outputevents (primarily urine output)"""
        self.logger.info("💧 Discovering outputevents SOFA parameters...")
        
        cur = self.conn.cursor()
        
        # Get all unique output items with counts
        discovery_query = """
            SELECT 
                di.itemid,
                di.label,
                di.category,
                di.unitname,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT oe.subject_id) as patient_count,
                COUNT(DISTINCT oe.stay_id) as stay_count,
                MIN(oe.value) as min_value,
                MAX(oe.value) as max_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY oe.value) as median_value
            FROM mimiciv_icu.outputevents oe
            JOIN mimiciv_icu.d_items di ON oe.itemid = di.itemid
            WHERE oe.value IS NOT NULL AND oe.value > 0
            GROUP BY di.itemid, di.label, di.category, di.unitname
            HAVING COUNT(*) >= 10
            ORDER BY measurement_count DESC
        """
        
        cur.execute(discovery_query)
        outputevents_items = cur.fetchall()
        
        self.logger.info(f"💧 Found {len(outputevents_items)} unique outputevents items with data")
        
        # Match to renal SOFA system (urine output)
        sofa_matches = {'renal': []}
        
        for item in outputevents_items:
            itemid, label, category, unit, count, patients, stays, min_val, max_val, median = item
            label_upper = label.upper()
            
            # Check for urine output related terms
            for search_term in self.sofa_search_terms['renal']['search_terms']:
                if search_term.upper() in label_upper:
                    match_info = {
                        'itemid': itemid,
                        'label': label,
                        'category': category,
                        'unit': unit,
                        'table': 'outputevents',
                        'measurement_count': count,
                        'patient_count': patients,
                        'stay_count': stays,
                        'min_value': min_val,
                        'max_value': max_val,
                        'median_value': median,
                        'search_term_matched': search_term,
                        'system': 'renal'
                    }
                    sofa_matches['renal'].append(match_info)
                    self.logger.info(f"   ✅ RENAL: {itemid} - {label} ({count:,} measurements)")
                    break
        
        return sofa_matches

    def discover_inputevents_parameters(self):
        """Discover vasopressor parameters in inputevents for cardiovascular SOFA"""
        self.logger.info("💉 Discovering inputevents SOFA parameters...")
        
        cur = self.conn.cursor()
        
        # Get all unique input items (medications/fluids) with counts
        discovery_query = """
            SELECT 
                di.itemid,
                di.label,
                di.category,
                di.unitname,
                COUNT(*) as measurement_count,
                COUNT(DISTINCT ie.subject_id) as patient_count,
                COUNT(DISTINCT ie.stay_id) as stay_count,
                MIN(ie.amount) as min_amount,
                MAX(ie.amount) as max_amount,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ie.amount) as median_amount
            FROM mimiciv_icu.inputevents ie
            JOIN mimiciv_icu.d_items di ON ie.itemid = di.itemid
            WHERE ie.amount IS NOT NULL AND ie.amount > 0
            GROUP BY di.itemid, di.label, di.category, di.unitname
            HAVING COUNT(*) >= 5  -- Lower threshold for medications
            ORDER BY measurement_count DESC
        """
        
        cur.execute(discovery_query)
        inputevents_items = cur.fetchall()
        
        self.logger.info(f"💉 Found {len(inputevents_items)} unique inputevents items with data")
        
        # Match to cardiovascular SOFA system (vasopressors)
        sofa_matches = {'cardiovascular': []}
        
        for item in inputevents_items:
            itemid, label, category, unit, count, patients, stays, min_amt, max_amt, median = item
            label_upper = label.upper()
            
            # Check for vasopressor medications
            for search_term in self.sofa_search_terms['cardiovascular']['search_terms']:
                if search_term.upper() in label_upper:
                    match_info = {
                        'itemid': itemid,
                        'label': label,
                        'category': category,
                        'unit': unit,
                        'table': 'inputevents',
                        'measurement_count': count,
                        'patient_count': patients,
                        'stay_count': stays,
                        'min_value': min_amt,
                        'max_value': max_amt,
                        'median_value': median,
                        'search_term_matched': search_term,
                        'system': 'cardiovascular'
                    }
                    sofa_matches['cardiovascular'].append(match_info)
                    self.logger.info(f"   ✅ CARDIOVASCULAR: {itemid} - {label} ({count:,} measurements)")
                    break
        
        return sofa_matches

    def run_comprehensive_discovery(self):
        """Run complete parameter discovery across all MIMIC-IV tables"""
        self.logger.info("🚀 Starting comprehensive SOFA parameter discovery...")
        
        # Analyze database structure
        table_stats = self.analyze_database_structure()
        
        # Discover parameters in each table
        chartevents_matches = self.discover_chartevents_parameters()
        labevents_matches = self.discover_labevents_parameters()
        outputevents_matches = self.discover_outputevents_parameters()
        inputevents_matches = self.discover_inputevents_parameters()
        
        # Consolidate results
        all_discovered = {}
        for system in self.sofa_search_terms.keys():
            all_discovered[system] = {
                'chartevents': chartevents_matches.get(system, []),
                'labevents': labevents_matches.get(system, []),
                'outputevents': outputevents_matches.get(system, []),
                'inputevents': inputevents_matches.get(system, [])
            }
        
        # Generate discovery report
        self.generate_discovery_report(all_discovered, table_stats)
        
        # Create OMOP mappings
        self.create_omop_mappings(all_discovered)
        
        # Save results
        self.save_discovery_results(all_discovered)
        
        return all_discovered

    def generate_discovery_report(self, discovered_params: Dict, table_stats: Dict):
        """Generate comprehensive discovery report"""
        self.logger.info("📋 Generating discovery report...")
        
        report = []
        report.append("# MIMIC-IV SOFA Parameter Discovery Report")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Database overview
        report.append("## Database Overview")
        for table, count in table_stats.items():
            report.append(f"- {table}: {count:,} records")
        report.append("")
        
        # SOFA system coverage
        report.append("## SOFA Parameter Coverage by System")
        total_parameters = 0
        
        for system, tables in discovered_params.items():
            system_total = sum(len(items) for items in tables.values())
            total_parameters += system_total
            
            report.append(f"### {system.upper()} ({system_total} parameters found)")
            
            for table, items in tables.items():
                if items:
                    report.append(f"#### {table} ({len(items)} items)")
                    for item in items:
                        report.append(f"- **{item['itemid']}**: {item['label']} "
                                    f"({item['measurement_count']:,} measurements, "
                                    f"{item['patient_count']} patients)")
            report.append("")
        
        report.append(f"## Summary")
        report.append(f"- **Total SOFA parameters discovered**: {total_parameters}")
        report.append(f"- **Systems with parameters**: {len([s for s, t in discovered_params.items() if any(t.values())])}/6")
        
        # Write report
        with open('parameter_discovery_report.md', 'w') as f:
            f.write('\n'.join(report))
        
        self.logger.info(f"📋 Discovery report saved: parameter_discovery_report.md")
        self.logger.info(f"🎯 Total SOFA parameters discovered: {total_parameters}")

    def create_omop_mappings(self, discovered_params: Dict):
        """Create OMOP concept mappings for discovered parameters"""
        self.logger.info("🗂️ Creating OMOP concept mappings...")
        
        concept_id_counter = 3000000  # Start from 3M to avoid conflicts
        mappings = {}
        
        for system, tables in discovered_params.items():
            for table, items in tables.items():
                for item in items:
                    concept_id = concept_id_counter
                    concept_id_counter += 1
                    
                    # Determine standard unit and clinical limits
                    standard_unit, min_val, max_val = self._get_clinical_limits(item, system)
                    
                    mapping = {
                        'concept_id': concept_id,
                        'concept_name': item['label'],
                        'source_itemid': item['itemid'],
                        'source_table': table,
                        'sofa_system': system,
                        'standard_unit': standard_unit,
                        'min_value': min_val,
                        'max_value': max_val,
                        'conversion_factor': 1.0,  # To be determined during harmonization
                        'original_unit': item.get('unit', ''),
                        'measurement_count': item['measurement_count'],
                        'patient_count': item['patient_count']
                    }
                    
                    mappings[concept_id] = mapping
        
        self.omop_mappings = mappings
        self.logger.info(f"🗂️ Created {len(mappings)} OMOP concept mappings")
        
        return mappings

    def _get_clinical_limits(self, item: Dict, system: str) -> Tuple[str, float, float]:
        """Determine clinical limits and standard units for a parameter"""
        label_upper = item['label'].upper()
        
        # Respiratory system
        if 'PAO2' in label_upper or 'PO2' in label_upper:
            return 'mmHg', 40, 500
        elif 'SPO2' in label_upper or 'OXYGEN SAT' in label_upper:
            return '%', 70, 100
        elif 'FIO2' in label_upper:
            return 'fraction', 0.21, 1.0
        
        # Cardiovascular system
        elif 'MAP' in label_upper or 'MEAN ARTERIAL' in label_upper:
            return 'mmHg', 40, 150
        elif 'SYSTOLIC' in label_upper:
            return 'mmHg', 70, 250
        elif 'DIASTOLIC' in label_upper:
            return 'mmHg', 30, 150
        elif any(vaso in label_upper for vaso in ['NOREPINEPHRINE', 'EPINEPHRINE', 'DOPAMINE']):
            return 'mcg/kg/min', 0, 100
        
        # Coagulation
        elif 'PLATELET' in label_upper:
            return 'K/uL', 10, 1000
        
        # Liver
        elif 'BILIRUBIN' in label_upper:
            return 'mg/dL', 0.1, 30
        
        # CNS
        elif 'GCS' in label_upper or 'GLASGOW' in label_upper:
            return 'points', 3, 15
        
        # Renal
        elif 'CREATININE' in label_upper:
            return 'mg/dL', 0.3, 15
        elif 'URINE' in label_upper and 'OUTPUT' in label_upper:
            return 'mL/hr', 0, 500
        
        # Default ranges based on observed data
        else:
            min_val = float(item.get('min_value', 0))
            max_val = float(item.get('max_value', 1000))
            return item.get('unit', ''), min_val, max_val

    def save_discovery_results(self, discovered_params: Dict):
        """Save discovery results to JSON files for pipeline use"""
        self.logger.info("💾 Saving discovery results...")
        
        # Save discovered parameters
        with open('discovered_sofa_parameters.json', 'w') as f:
            # Convert numpy types to native Python types for JSON serialization
            serializable_params = self._convert_for_json(discovered_params)
            json.dump(serializable_params, f, indent=2)
        
        # Save OMOP mappings
        with open('omop_concept_mappings.json', 'w') as f:
            serializable_mappings = self._convert_for_json(self.omop_mappings)
            json.dump(serializable_mappings, f, indent=2)
        
        self.logger.info("💾 Discovery results saved to JSON files")

    def _convert_for_json(self, obj):
        """Convert numpy types to JSON-serializable types"""
        if isinstance(obj, dict):
            return {key: self._convert_for_json(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_for_json(item) for item in obj]
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif pd.isna(obj):
            return None
        else:
            return obj


def main():
    """Main execution function"""
    discovery = SOFAParameterDiscovery()
    
    print("🚀 Starting MIMIC-IV SOFA Parameter Discovery...")
    print("=" * 60)
    
    try:
        # Run comprehensive discovery
        discovered_params = discovery.run_comprehensive_discovery()
        
        print("\n✅ Parameter discovery completed successfully!")
        print(f"📊 Results saved to:")
        print(f"   - parameter_discovery_report.md")
        print(f"   - discovered_sofa_parameters.json")
        print(f"   - omop_concept_mappings.json")
        print(f"   - parameter_discovery.log")
        
    except Exception as e:
        print(f"\n❌ Discovery failed: {e}")
        discovery.logger.error(f"Discovery failed: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
