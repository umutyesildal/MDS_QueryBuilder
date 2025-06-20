#!/usr/bin/env python3
"""
Complete Gold Layer Schema Documentation
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def document_gold_layer():
    """Document Gold layer with robust error handling"""
    
    conn = psycopg2.connect(
        host="localhost",
        database="mimiciv", 
        user="postgres",
        password="password"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    print("=" * 80)
    print("🏥 MIMIC-IV MEDALLION PIPELINE - GOLD LAYER COMPLETE SCHEMA")
    print("=" * 80)
    print()
    
    with conn.cursor() as cursor:
        # Document each Gold table individually
        tables = ['sofa_scores']
        
        for table_name in tables:
            try:
                print(f"📋 TABLE: gold.{table_name}")
                print("=" * 50)
                
                # Get columns
                cursor.execute(f"""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'gold' AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                
                columns = cursor.fetchall()
                if columns:
                    print("📝 COLUMNS:")
                    print("-" * 30)
                    for col_name, data_type, nullable, default in columns:
                        nullable_str = "NULL" if nullable == "YES" else "NOT NULL"
                        default_str = f", DEFAULT: {default}" if default else ""
                        print(f"   🔸 {col_name:<25} | {data_type:<20} | {nullable_str}{default_str}")
                
                # Get record count
                cursor.execute(f"SELECT COUNT(*) FROM gold.{table_name}")
                count = cursor.fetchone()[0]
                print(f"\n📊 Total Records: {count:,}")
                
                # Table-specific analysis
                if table_name == 'sofa_scores' and count > 0:
                    print("\n🎯 SOFA SCORES ANALYSIS:")
                    
                    # Score distribution
                    cursor.execute("""
                        SELECT 
                            CASE 
                                WHEN total_sofa_score = 0 THEN 'No dysfunction (0)'
                                WHEN total_sofa_score BETWEEN 1 AND 6 THEN 'Mild (1-6)'
                                WHEN total_sofa_score BETWEEN 7 AND 9 THEN 'Moderate (7-9)'
                                WHEN total_sofa_score BETWEEN 10 AND 12 THEN 'Severe (10-12)'
                                ELSE 'Critical (13+)'
                            END as severity,
                            COUNT(*) as count,
                            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
                        FROM gold.sofa_scores
                        GROUP BY 1
                        ORDER BY count DESC
                    """)
                    
                    for severity, count, percentage in cursor.fetchall():
                        print(f"   {severity}: {count} ({percentage}%)")
                
                elif table_name == 'patient_sofa_summary' and count > 0:
                    print("\n👥 PATIENT SUMMARY ANALYSIS:")
                    
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as patients,
                            ROUND(AVG(total_measurements), 0) as avg_measurements,
                            ROUND(AVG(avg_sofa_score), 2) as avg_sofa
                        FROM gold.patient_sofa_summary
                    """)
                    
                    patients, avg_measurements, avg_sofa = cursor.fetchone()
                    print(f"   Total Patients: {patients}")
                    print(f"   Avg Measurements: {avg_measurements}")
                    print(f"   Avg SOFA Score: {avg_sofa}")
                
                print("\n" + "─" * 80 + "\n")
                
            except Exception as e:
                print(f"❌ Error documenting {table_name}: {e}")
                print("─" * 80 + "\n")
                continue
    
    # Pipeline summary
    print("📊 PIPELINE SUMMARY:")
    print("-" * 30)
    
    try:
        with conn.cursor() as cursor:
            # Bronze layer
            cursor.execute("SELECT COUNT(*) FROM bronze.collection_disease")
            bronze_count = cursor.fetchone()[0]
            
            # Silver layer  
            cursor.execute("SELECT COUNT(*) FROM silver.collection_disease_std")
            silver_count = cursor.fetchone()[0]
            
            # Gold layer totals
            cursor.execute("SELECT COUNT(*) FROM gold.sofa_scores")
            gold_scores = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM gold.patient_sofa_summary")
            gold_patients = cursor.fetchone()[0]
            
            print(f"🥉 Bronze Layer: {bronze_count:,} raw measurements")
            print(f"🥈 Silver Layer: {silver_count:,} standardized measurements")
            print(f"🥇 Gold Layer: {gold_scores:,} SOFA scores for {gold_patients} patients")
            
    except Exception as e:
        print(f"❌ Error getting pipeline summary: {e}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ GOLD LAYER DOCUMENTATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    document_gold_layer()
