#!/usr/bin/env python3
"""
Test SQL Queries on Extracted Medical Data
QueryBuilder for Medical Data Science (Übungsblatt 3.2)
"""

import psycopg2
import pandas as pd
from config import DB_CONFIG
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class QueryTester:
    def __init__(self):
        self.connection = None
        
    def connect(self):
        """Connect to PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(**DB_CONFIG)
            logger.info("✅ Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def test_basic_queries(self):
        """Test basic analysis queries on the extracted data."""
        if not self.connection:
            logger.error("No database connection")
            return
            
        queries = {
            "Total Records": """
                SELECT COUNT(*) as total_records 
                FROM bronze.collection_disease;
            """,
            
            "Records by Source": """
                SELECT 
                    source_table,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT subject_id) as unique_patients,
                    COUNT(DISTINCT itemid) as unique_parameters
                FROM bronze.collection_disease 
                GROUP BY source_table;
            """,
            
            "Data Time Range": """
                SELECT 
                    MIN(charttime) as earliest_record,
                    MAX(charttime) as latest_record,
                    (MAX(charttime) - MIN(charttime)) as time_span
                FROM bronze.collection_disease
                WHERE charttime IS NOT NULL;
            """,
            
            "Top 10 Chart Parameters": """
                SELECT 
                    cd.itemid,
                    COUNT(*) as frequency
                FROM bronze.collection_disease cd
                WHERE cd.source_table = 'chartevents'
                GROUP BY cd.itemid
                ORDER BY frequency DESC
                LIMIT 10;
            """,
            
            "Top 10 Lab Parameters": """
                SELECT 
                    cd.itemid,
                    COUNT(*) as frequency
                FROM bronze.collection_disease cd
                WHERE cd.source_table = 'labevents'
                GROUP BY cd.itemid
                ORDER BY frequency DESC
                LIMIT 10;
            """,
            
            "Patient Coverage": """
                SELECT 
                    COUNT(DISTINCT subject_id) as unique_patients,
                    COUNT(DISTINCT hadm_id) as unique_admissions,
                    COUNT(DISTINCT stay_id) as unique_icu_stays
                FROM bronze.collection_disease;
            """,
            
            "Value Distribution Sample": """
                SELECT 
                    itemid,
                    source_table,
                    MIN(valuenum) as min_value,
                    MAX(valuenum) as max_value,
                    AVG(valuenum) as avg_value,
                    COUNT(*) as count
                FROM bronze.collection_disease
                WHERE valuenum IS NOT NULL
                GROUP BY itemid, source_table
                ORDER BY count DESC
                LIMIT 10;
            """
        }
        
        logger.info("🧪 Testing SQL queries on extracted data...")
        
        for query_name, query in queries.items():
            try:
                logger.info(f"\n📊 Running: {query_name}")
                df = pd.read_sql_query(query, self.connection)
                print(f"\n{query_name}:")
                print(df.to_string(index=False))
                print("-" * 50)
                
            except Exception as e:
                logger.error(f"❌ Query '{query_name}' failed: {e}")
    
    def test_clinical_analysis(self):
        """Test clinical analysis queries."""
        if not self.connection:
            logger.error("No database connection")
            return
            
        logger.info("\n🏥 Testing clinical analysis queries...")
        
        # Respiratory parameters analysis
        respiratory_query = """
            SELECT 
                subject_id,
                hadm_id,
                charttime,
                itemid,
                valuenum,
                valueuom
            FROM bronze.collection_disease
            WHERE itemid IN (220210, 224688, 224689, 224690)  -- Respiratory rate parameters
            AND valuenum IS NOT NULL
            ORDER BY subject_id, charttime
            LIMIT 20;
        """
        
        try:
            logger.info("📊 Respiratory parameters sample:")
            df = pd.read_sql_query(respiratory_query, self.connection)
            print(df.to_string(index=False))
            
        except Exception as e:
            logger.error(f"❌ Respiratory analysis failed: {e}")
    
    def close(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Database connection closed")

def main():
    """Main execution function."""
    tester = QueryTester()
    
    if tester.connect():
        tester.test_basic_queries()
        tester.test_clinical_analysis()
        tester.close()
    else:
        logger.error("Cannot proceed without database connection")

if __name__ == "__main__":
    main()
