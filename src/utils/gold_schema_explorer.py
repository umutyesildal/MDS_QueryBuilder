#!/usr/bin/env python3
"""
MIMIC-IV Medallion Pipeline - Gold Layer Schema Explorer
Complete documentation of Gold layer tables and structure
"""

import psycopg2
import json
from datetime import datetime
from typing import Dict, List, Any

def get_db_connection():
    """Create database connection with proper error handling"""
    try:
        return psycopg2.connect(
            host="localhost",
            database="mimiciv",
            user="postgres",
            password="password"
        )
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def explore_gold_layer():
    """Explore and document Gold layer tables"""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor() as cursor:
            print("=" * 80)
            print("🏥 MIMIC-IV MEDALLION PIPELINE - GOLD LAYER SCHEMA")
            print("=" * 80)
            print(f"🕐 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🔗 Database: mimiciv")
            print()
            
            # Get Gold layer tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gold'
                ORDER BY table_name
            """)
            gold_tables = [row[0] for row in cursor.fetchall()]
            
            print(f"📁 GOLD LAYER TABLES: {len(gold_tables)} table(s)")
            for table in gold_tables:
                print(f"   📄 {table}")
            print()
            
            # Document each Gold table
            for table_name in gold_tables:
                document_table(cursor, 'gold', table_name)
                print("─" * 80)
                print()
    
    except Exception as e:
        print(f"❌ Error exploring Gold layer: {e}")
    finally:
        conn.close()

def document_table(cursor, schema: str, table_name: str):
    """Document a specific table with full details"""
    print(f"📋 TABLE: {schema}.{table_name}")
    print("=" * 50)
    print()
    
    # Get column information
    cursor.execute(f"""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    if columns:
        print("📝 COLUMNS:")
        print("-" * 40)
        for col in columns:
            col_name, data_type, nullable, default, max_length = col
            nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
            
            # Format data type with length if applicable
            if max_length and data_type == 'character varying':
                data_type = f"{data_type}({max_length})"
            
            default_str = f", DEFAULT: {default}" if default else ""
            print(f"   🔸 {col_name:<25} | {data_type:<20} | {nullable_str}{default_str}")
    
    print()
    
    # Get sample data
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}")
        total_count = cursor.fetchone()[0]
        
        print(f"📊 SAMPLE DATA FROM {schema}.{table_name}:")
        print("-" * 50)
        print(f"📈 Total Records: {total_count:,}")
        
        if total_count > 0:
            # Get sample record
            cursor.execute(f"SELECT * FROM {schema}.{table_name} LIMIT 1")
            sample_record = cursor.fetchone()
            col_names = [desc[0] for desc in cursor.description]
            
            print()
            print("🔍 SAMPLE RECORD:")
            print("-" * 30)
            for i, value in enumerate(sample_record):
                # Format different data types appropriately
                if isinstance(value, dict):
                    formatted_value = json.dumps(value, indent=2)
                else:
                    formatted_value = str(value)
                
                print(f"   {col_names[i]:<25}: {formatted_value}")
        
        print()
        
        # Get key statistics for SOFA scores table
        if table_name == 'sofa_scores':
            get_sofa_scores_stats(cursor, schema, table_name)
        elif table_name == 'patient_sofa_summary':
            get_patient_summary_stats(cursor, schema, table_name)
        elif table_name == 'daily_sofa_trends':
            get_daily_trends_stats(cursor, schema, table_name)
    
    except Exception as e:
        print(f"❌ Error getting sample data: {e}")

def get_sofa_scores_stats(cursor, schema: str, table_name: str):
    """Get specific statistics for SOFA scores table"""
    print("📊 SOFA SCORES ANALYSIS:")
    print("-" * 30)
    
    try:
        # Score distribution
        cursor.execute(f"""
            SELECT 
                CASE 
                    WHEN total_sofa_score = 0 THEN '0 (No dysfunction)'
                    WHEN total_sofa_score BETWEEN 1 AND 6 THEN '1-6 (Mild)'
                    WHEN total_sofa_score BETWEEN 7 AND 9 THEN '7-9 (Moderate)'
                    WHEN total_sofa_score BETWEEN 10 AND 12 THEN '10-12 (Severe)'
                    ELSE '13+ (Critical)'
                END as severity_category,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
            FROM {schema}.{table_name}
            GROUP BY 
                CASE 
                    WHEN total_sofa_score = 0 THEN '0 (No dysfunction)'
                    WHEN total_sofa_score BETWEEN 1 AND 6 THEN '1-6 (Mild)'
                    WHEN total_sofa_score BETWEEN 7 AND 9 THEN '7-9 (Moderate)'
                    WHEN total_sofa_score BETWEEN 10 AND 12 THEN '10-12 (Severe)'
                    ELSE '13+ (Critical)'
                END
            ORDER BY count DESC
        """)
        
        print("🎯 SOFA Score Distribution:")
        for row in cursor.fetchall():
            category, count, percentage = row
            print(f"   {category}: {count} ({percentage}%)")
        
        # Score range
        cursor.execute(f"""
            SELECT 
                MIN(total_sofa_score) as min_score,
                MAX(total_sofa_score) as max_score,
                ROUND(AVG(total_sofa_score), 2) as avg_score,
                COUNT(DISTINCT subject_id) as unique_patients,
                COUNT(DISTINCT stay_id) as unique_stays
            FROM {schema}.{table_name}
        """)
        
        result = cursor.fetchone()
        min_score, max_score, avg_score, unique_patients, unique_stays = result
        
        print(f"📈 Score Range: {min_score} - {max_score} (Average: {avg_score})")
        print(f"👤 Unique Patients: {unique_patients}")
        print(f"🏥 Unique ICU Stays: {unique_stays}")
        
    except Exception as e:
        print(f"❌ Error getting SOFA stats: {e}")
    
    print()

def get_patient_summary_stats(cursor, schema: str, table_name: str):
    """Get specific statistics for patient summary table"""
    print("📊 PATIENT SUMMARY ANALYSIS:")
    print("-" * 30)
    
    try:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_patients,
                ROUND(AVG(total_measurements), 0) as avg_measurements,
                ROUND(AVG(avg_sofa_score), 2) as avg_sofa_score,
                COUNT(CASE WHEN max_sofa_score >= 10 THEN 1 END) as high_severity_patients
            FROM {schema}.{table_name}
        """)
        
        result = cursor.fetchone()
        total_patients, avg_measurements, avg_sofa_score, high_severity = result
        
        print(f"👥 Total Patients: {total_patients}")
        print(f"📊 Average Measurements per Patient: {avg_measurements}")
        print(f"🎯 Average SOFA Score: {avg_sofa_score}")
        print(f"🚨 High Severity Patients (SOFA ≥10): {high_severity}")
        
    except Exception as e:
        print(f"❌ Error getting patient summary stats: {e}")
    
    print()

def get_daily_trends_stats(cursor, schema: str, table_name: str):
    """Get specific statistics for daily trends table"""
    print("📊 DAILY TRENDS ANALYSIS:")
    print("-" * 30)
    
    try:
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_daily_scores,
                COUNT(DISTINCT subject_id) as unique_patients,
                ROUND(AVG(EXTRACT(EPOCH FROM (window_end - window_start))/3600), 1) as avg_window_hours,
                ROUND(AVG(total_sofa_score), 2) as avg_daily_sofa
            FROM {schema}.{table_name}
        """)
        
        result = cursor.fetchone()
        total_scores, unique_patients, avg_window_hours, avg_daily_sofa = result
        
        print(f"📅 Total Daily Scores: {total_scores}")
        print(f"👤 Unique Patients: {unique_patients}")
        print(f"⏰ Average Window Duration: {avg_window_hours} hours")
        print(f"📈 Average Daily SOFA: {avg_daily_sofa}")
        
    except Exception as e:
        print(f"❌ Error getting daily trends stats: {e}")
    
    print()

if __name__ == "__main__":
    explore_gold_layer()
    print("=" * 80)
    print("✅ Gold Layer Schema Documentation Complete")
    print("=" * 80)
