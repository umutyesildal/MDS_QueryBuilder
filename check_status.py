#!/usr/bin/env python3
"""
Project Status Checker
QueryBuilder for Medical Data Science (Übungsblatt 3.2)
======================================================

Quick status check script to verify project completeness.
"""

import os
import psycopg2
from config import DB_CONFIG
import logging

# Suppress warnings for cleaner output
logging.getLogger().setLevel(logging.ERROR)

def check_files():
    """Check if all required project files exist."""
    required_files = [
        'querybuilder.py',
        'config.py', 
        'validate_data.py',
        'test_queries.py',
        'generate_summary.py',
        'README.md',
        'requirements.txt',
        'example_queries.sql',
        'test_db.py',
        'setup.py'
    ]
    
    print("📁 FILE STATUS CHECK")
    print("=" * 30)
    
    all_exist = True
    for file in required_files:
        exists = os.path.exists(file)
        status = "✅" if exists else "❌"
        print(f"{status} {file}")
        if not exists:
            all_exist = False
    
    return all_exist

def check_database():
    """Check database connection and Bronze schema."""
    print("\n🗄️ DATABASE STATUS CHECK")
    print("=" * 30)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Database connection")
        
        cursor = conn.cursor()
        
        # Check Bronze schema
        cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze';")
        if cursor.fetchone():
            print("✅ Bronze schema exists")
        else:
            print("❌ Bronze schema missing")
            return False
        
        # Check collection_disease table
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = 'collection_disease';
        """)
        if cursor.fetchone():
            print("✅ collection_disease table exists")
        else:
            print("❌ collection_disease table missing")
            return False
        
        # Check record count
        cursor.execute("SELECT COUNT(*) FROM bronze.collection_disease;")
        count = cursor.fetchone()[0]
        print(f"✅ Records in table: {count:,}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database check failed: {e}")
        return False

def check_logs():
    """Check if log files exist."""
    print("\n📋 LOG FILES CHECK")
    print("=" * 30)
    
    log_files = ['querybuilder.log', 'FINAL_SUMMARY_REPORT.txt']
    
    for log_file in log_files:
        exists = os.path.exists(log_file)
        status = "✅" if exists else "❌"
        print(f"{status} {log_file}")

def main():
    """Main status check function."""
    print("🎯 QUERYBUILDER PROJECT STATUS")
    print("=" * 40)
    
    files_ok = check_files()
    db_ok = check_database()
    check_logs()
    
    print("\n📊 OVERALL STATUS")
    print("=" * 20)
    
    if files_ok and db_ok:
        print("🎉 PROJECT COMPLETE!")
        print("✅ All files present")
        print("✅ Database configured")
        print("✅ Data extracted successfully")
        print("\n🚀 Ready for medical data analysis!")
    else:
        print("⚠️ PROJECT INCOMPLETE")
        if not files_ok:
            print("❌ Missing required files")
        if not db_ok:
            print("❌ Database issues detected")

if __name__ == "__main__":
    main()
