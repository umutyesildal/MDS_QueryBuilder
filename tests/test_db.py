#!/usr/bin/env python3
"""
Database Connection Test Script
==============================

Quick test to verify database connectivity and schema availability
before running the main QueryBuilder.
"""

from sqlalchemy import create_engine, text
import sys


def test_connection():
    """Test database connection and schema availability."""
    connection_string = "postgresql://umutyesildal@localhost:5432/mimiciv"
    
    try:
        print("🔗 Testing database connection...")
        engine = create_engine(connection_string)
        
        with engine.connect() as conn:
            # Test basic connection
            result = conn.execute(text("SELECT version()"))
            version = result.scalar()
            print(f"✅ Connected to PostgreSQL: {version[:50]}...")
            
            # Check available schemas
            schemas_query = text("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name IN ('mimiciv_hosp', 'mimiciv_icu', 'mimiciv_derived', 'public')
                ORDER BY schema_name
            """)
            schemas = conn.execute(schemas_query).fetchall()
            
            print(f"📁 Available schemas: {[s[0] for s in schemas]}")
            
            if len(schemas) < 2:
                print("⚠️  Warning: Expected MIMIC-IV schemas not found")
                return False
                
            # Test table access
            try:
                tables_query = text("""
                    SELECT table_schema, table_name 
                    FROM information_schema.tables 
                    WHERE table_schema IN ('mimiciv_hosp', 'mimiciv_icu')
                    AND table_name IN ('chartevents', 'labevents', 'd_items', 'd_labitems')
                    ORDER BY table_schema, table_name
                """)
                tables = conn.execute(tables_query).fetchall()
                
                print(f"📊 Key tables found:")
                for schema, table in tables:
                    print(f"  - {schema}.{table}")
                
                if len(tables) < 4:
                    print("⚠️  Warning: Some expected tables not found")
                    
                print("✅ Database test completed successfully!")
                return True
                
            except Exception as e:
                print(f"❌ Error accessing tables: {e}")
                return False
                
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check:")
        print("  - PostgreSQL service is running")
        print("  - Database 'mimiciv' exists")
        print("  - User has proper permissions") 
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
