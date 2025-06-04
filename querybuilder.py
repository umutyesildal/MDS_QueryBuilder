#!/usr/bin/env python3
"""
QueryBuilder for Medical Data Science (Übungsblatt 3.2)
===============================================

Dynamic SQL query generator for extracting clinical parameters 
associated with Acute Respiratory Failure (ARI) from MIMIC-IV database
into a structured Bronze-level schema.

Author: Medical Data Science Team
Date: May 2025
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import traceback

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
import psycopg2


class QueryBuilder:
    """
    QueryBuilder class for extracting medical data from MIMIC-IV database
    and inserting into Bronze layer schema for Acute Respiratory Failure analysis.
    """
    
    def __init__(self, connection_string: str):
        """
        Initialize QueryBuilder with database connection.
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.engine = None
        self.logger = self._setup_logging()
        
        # OMOP Concept IDs mapping from Übung 2
        self.omop_concepts = {
            'PaO2_FiO2_Ratio': 40762499,
            'SpO2_FiO2_Ratio': 40764520,
            'Respiratory_Rate': 3027018,
            'Heart_Rate': 3027017,
            'PaCO2': 3020656,
            'Tidal_Volume': 3024289,
            'Minute_Ventilation': 3024328,
            'Creatinine': 3016723,
            'pH': 3014605,
            'Albumin': 3013705,
            'Uric_Acid': 3013466,
            'NT_proBNP': 3016728,
            'D_Dimer': 3003737,
            'Homocysteine': 3002304,
            'Procalcitonin': 3013682,
            'IL_6': 3015039,
            'IL_8': 3015040,
            'IL_10': 3015037,
            'ST2': 43013695,
            'Pentraxin_3': 4172961,
            'Fraktalkin': 4273690,
            'sRAGE': 3022415,
            'KL_6': 43531668,
            'PAI_1': 4314768,
            'VEGF': 43013099
        }
        
        # Parameter definitions for different tables
        self.chart_parameters = [
            'spo2', 'respiratory rate', 'heart rate', 'tidal volume', 
            'minute ventilation', 'oxygen saturation'
        ]
        
        self.lab_parameters = [
            'ph', 'paco2', 'creatinine', 'albumin', 'd-dimer', 'procalcitonin',
            'il-6', 'il-8', 'il-10', 'nt-probnp', 'homocysteine', 'harnsäure',
            'srage', 'kl-6', 'pai-1', 'vegf', 'lactate', 'uric acid'
        ]
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging configuration."""
        logger = logging.getLogger('QueryBuilder')
        logger.setLevel(logging.INFO)
        
        # Create file handler
        file_handler = logging.FileHandler('querybuilder.log', mode='w')
        file_handler.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
        
    def connect(self) -> bool:
        """
        Establish database connection and verify schemas.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.engine = create_engine(self.connection_string)
            
            # Test connection and verify schemas
            with self.engine.connect() as conn:
                schemas_query = text("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('mimiciv_hosp', 'mimiciv_icu', 'mimiciv_derived')
                """)
                schemas = conn.execute(schemas_query).fetchall()
                
                if len(schemas) < 2:
                    self.logger.error("Required MIMIC-IV schemas not found")
                    return False
                    
                self.logger.info(f"✅ Connected to database. Found schemas: {[s[0] for s in schemas]}")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Database connection failed: {str(e)}")
            return False
            
    def get_chart_itemids(self) -> List[int]:
        """
        Retrieve itemids for chart events parameters.
        
        Returns:
            List[int]: List of itemids for chartevents table
        """
        try:
            with self.engine.connect() as conn:
                # Build dynamic WHERE clause for chart parameters
                conditions = []
                for param in self.chart_parameters:
                    conditions.append(f"LOWER(label) LIKE '%{param}%'")
                
                where_clause = " OR ".join(conditions)
                
                query = text(f"""
                    SELECT itemid, label, unitname, param_type
                    FROM mimiciv_icu.d_items 
                    WHERE {where_clause}
                    ORDER BY label
                """)
                
                result = conn.execute(query).fetchall()
                itemids = [row[0] for row in result]
                
                self.logger.info(f"📊 Found {len(itemids)} chart itemids:")
                for row in result:
                    self.logger.info(f"  - {row[0]}: {row[1]} ({row[2]}, {row[3]})")
                    
                return itemids
                
        except Exception as e:
            self.logger.error(f"❌ Error retrieving chart itemids: {str(e)}")
            return []
            
    def get_lab_itemids(self) -> List[int]:
        """
        Retrieve itemids for lab events parameters.
        
        Returns:
            List[int]: List of itemids for labevents table
        """
        try:
            with self.engine.connect() as conn:
                # Build dynamic WHERE clause for lab parameters
                conditions = []
                for param in self.lab_parameters:
                    conditions.append(f"LOWER(label) LIKE '%{param}%'")
                
                where_clause = " OR ".join(conditions)
                
                query = text(f"""
                    SELECT itemid, label, category, fluid
                    FROM mimiciv_hosp.d_labitems 
                    WHERE {where_clause}
                    ORDER BY label
                """)
                
                result = conn.execute(query).fetchall()
                itemids = [row[0] for row in result]
                
                self.logger.info(f"🧪 Found {len(itemids)} lab itemids:")
                for row in result:
                    self.logger.info(f"  - {row[0]}: {row[1]} ({row[2]}, {row[3]})")
                    
                return itemids
                
        except Exception as e:
            self.logger.error(f"❌ Error retrieving lab itemids: {str(e)}")
            return []
            
    def create_bronze_schema(self) -> bool:
        """
        Create bronze schema and collection_disease table if they don't exist.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # Create bronze schema
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                
                # Create collection_disease table
                create_table_query = text("""
                    CREATE TABLE IF NOT EXISTS bronze.collection_disease (
                        id SERIAL PRIMARY KEY,
                        subject_id INTEGER NOT NULL,
                        hadm_id INTEGER,
                        stay_id INTEGER,
                        charttime TIMESTAMP,
                        storetime TIMESTAMP,
                        itemid INTEGER NOT NULL,
                        value TEXT,
                        valuenum NUMERIC,
                        valueuom TEXT,
                        source_table VARCHAR(50) NOT NULL,
                        omop_concept_id INTEGER,
                        extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        CONSTRAINT fk_source_table CHECK (source_table IN ('chartevents', 'labevents'))
                    )
                """)
                
                conn.execute(create_table_query)
                conn.commit()
                
                self.logger.info("✅ Bronze schema and collection_disease table created/verified")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Error creating bronze schema: {str(e)}")
            return False
            
    def insert_chart_data(self, itemids: List[int]) -> int:
        """
        Insert chart events data into bronze table.
        
        Args:
            itemids: List of itemids to extract
            
        Returns:
            int: Number of rows inserted
        """
        if not itemids:
            self.logger.warning("⚠️ No chart itemids provided, skipping chart data insertion")
            return 0
            
        try:
            with self.engine.connect() as conn:
                # Convert itemids to tuple for SQL IN clause
                itemids_tuple = tuple(itemids)
                
                insert_query = text("""
                    INSERT INTO bronze.collection_disease 
                    (subject_id, hadm_id, stay_id, charttime, storetime, itemid, value, valuenum, valueuom, source_table)
                    SELECT 
                        subject_id, 
                        hadm_id, 
                        stay_id, 
                        charttime, 
                        storetime, 
                        itemid, 
                        value, 
                        valuenum, 
                        valueuom, 
                        'chartevents'
                    FROM mimiciv_icu.chartevents
                    WHERE itemid = ANY(:itemids)
                    AND valuenum IS NOT NULL
                    AND (warning IS NULL OR warning = 0)
                """)
                
                result = conn.execute(insert_query, {'itemids': list(itemids_tuple)})
                rows_inserted = result.rowcount
                conn.commit()
                
                self.logger.info(f"✅ Inserted {rows_inserted} rows from chartevents with itemids: {itemids}")
                return rows_inserted
                
        except Exception as e:
            self.logger.error(f"❌ Error inserting chart data: {str(e)}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return 0
            
    def insert_lab_data(self, itemids: List[int]) -> int:
        """
        Insert lab events data into bronze table.
        
        Args:
            itemids: List of itemids to extract
            
        Returns:
            int: Number of rows inserted
        """
        if not itemids:
            self.logger.warning("⚠️ No lab itemids provided, skipping lab data insertion")
            return 0
            
        try:
            with self.engine.connect() as conn:
                # Convert itemids to tuple for SQL IN clause
                itemids_tuple = tuple(itemids)
                
                insert_query = text("""
                    INSERT INTO bronze.collection_disease 
                    (subject_id, hadm_id, stay_id, charttime, storetime, itemid, value, valuenum, valueuom, source_table)
                    SELECT 
                        subject_id, 
                        hadm_id, 
                        NULL as stay_id, 
                        charttime, 
                        storetime, 
                        itemid, 
                        value, 
                        valuenum, 
                        valueuom, 
                        'labevents'
                    FROM mimiciv_hosp.labevents
                    WHERE itemid = ANY(:itemids)
                    AND valuenum IS NOT NULL
                    AND (flag IS NULL OR flag NOT IN ('abnormal', 'error'))
                """)
                
                result = conn.execute(insert_query, {'itemids': list(itemids_tuple)})
                rows_inserted = result.rowcount
                conn.commit()
                
                self.logger.info(f"✅ Inserted {rows_inserted} rows from labevents with itemids: {itemids}")
                return rows_inserted
                
        except Exception as e:
            self.logger.error(f"❌ Error inserting lab data: {str(e)}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return 0
            
    def validate_results(self) -> bool:
        """
        Validate the inserted data and provide summary statistics.
        
        Returns:
            bool: True if validation successful, False otherwise
        """
        try:
            with self.engine.connect() as conn:
                # Count total records
                total_query = text("SELECT COUNT(*) FROM bronze.collection_disease")
                total_count = conn.execute(total_query).scalar()
                
                # Count by source table
                source_query = text("""
                    SELECT source_table, COUNT(*) as count 
                    FROM bronze.collection_disease 
                    GROUP BY source_table
                """)
                source_counts = conn.execute(source_query).fetchall()
                
                # Sample records
                sample_query = text("""
                    SELECT subject_id, hadm_id, itemid, value, valuenum, source_table
                    FROM bronze.collection_disease 
                    LIMIT 10
                """)
                sample_records = conn.execute(sample_query).fetchall()
                
                # Log validation results
                self.logger.info(f"📈 VALIDATION RESULTS:")
                self.logger.info(f"  Total records inserted: {total_count}")
                
                for source, count in source_counts:
                    self.logger.info(f"  {source}: {count} records")
                    
                self.logger.info(f"📋 Sample records:")
                for record in sample_records:
                    self.logger.info(f"  Subject: {record[0]}, HADM: {record[1]}, Item: {record[2]}, Value: {record[3]}, Source: {record[5]}")
                    
                return total_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ Error during validation: {str(e)}")
            return False
            
    def run_extraction(self) -> bool:
        """
        Main method to run the complete data extraction process.
        
        Returns:
            bool: True if extraction successful, False otherwise
        """
        start_time = datetime.now()
        self.logger.info("🚀 Starting QueryBuilder extraction process...")
        
        try:
            # Step 1: Connect to database
            if not self.connect():
                return False
                
            # Step 2: Create bronze schema
            if not self.create_bronze_schema():
                return False
                
            # Step 3: Get itemids
            self.logger.info("📋 Step 3: Retrieving itemids...")
            chart_itemids = self.get_chart_itemids()
            lab_itemids = self.get_lab_itemids()
            
            if not chart_itemids and not lab_itemids:
                self.logger.error("❌ No itemids found, aborting extraction")
                return False
                
            # Step 4: Insert data
            self.logger.info("💾 Step 4: Inserting data...")
            chart_rows = self.insert_chart_data(chart_itemids)
            lab_rows = self.insert_lab_data(lab_itemids)
            
            total_rows = chart_rows + lab_rows
            
            # Step 5: Validate results
            self.logger.info("✅ Step 5: Validating results...")
            validation_success = self.validate_results()
            
            # Summary
            end_time = datetime.now()
            duration = end_time - start_time
            
            self.logger.info(f"🎯 EXTRACTION SUMMARY:")
            self.logger.info(f"  Duration: {duration}")
            self.logger.info(f"  Total rows inserted: {total_rows}")
            self.logger.info(f"  Chart events: {chart_rows}")
            self.logger.info(f"  Lab events: {lab_rows}")
            self.logger.info(f"  Validation: {'✅ PASSED' if validation_success else '❌ FAILED'}")
            
            return validation_success and total_rows > 0
            
        except Exception as e:
            self.logger.error(f"❌ Fatal error during extraction: {str(e)}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False


def main():
    """Main execution function."""
    # Database connection string
    connection_string = "postgresql://bernazehraural@localhost:5432/mimiciv"
    
    # Initialize and run QueryBuilder
    qb = QueryBuilder(connection_string)
    
    success = qb.run_extraction()
    
    if success:
        print("\n🎉 QueryBuilder execution completed successfully!")
        print("📄 Check 'querybuilder.log' for detailed logs")
        print("🔍 Query the bronze.collection_disease table to explore results")
    else:
        print("\n❌ QueryBuilder execution failed!")
        print("📄 Check 'querybuilder.log' for error details")
        sys.exit(1)


if __name__ == "__main__":
    main()
