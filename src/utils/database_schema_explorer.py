#!/usr/bin/env python3
"""
Database Schema Explorer for MIMIC-IV Medallion Pipeline
========================================================

This script provides comprehensive documentation of the entire database schema
including all tables, columns, data types, relationships, and sample data.

Features:
- Complete schema documentation for Bronze, Silver, and Gold layers
- Column descriptions with data types and constraints
- Sample data from each table (LIMIT 1)
- Relationships and foreign key mappings
- Index and performance information
- Data quality and completeness metrics

Author: Medical Data Science Team
Date: 2025-06-05
"""

import psycopg2
import json
from datetime import datetime
from config_local import DB_CONFIG

class DatabaseSchemaExplorer:
    """Comprehensive database schema explorer and documenter"""
    
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cursor = self.conn.cursor()
        
    def print_header(self, title, level=1):
        """Print formatted headers"""
        if level == 1:
            print("\n" + "=" * 80)
            print(f"🏥 {title}")
            print("=" * 80)
        elif level == 2:
            print("\n" + "-" * 60)
            print(f"📊 {title}")
            print("-" * 60)
        elif level == 3:
            print(f"\n🔹 {title}")
            print("." * 40)
    
    def get_all_schemas(self):
        """Get all custom schemas in the database"""
        print("\n🗂️  DATABASE SCHEMAS:")
        print("-" * 40)
        
        self.cursor.execute("""
            SELECT table_schema, 
                   COUNT(table_name) as table_count
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            GROUP BY table_schema
            ORDER BY table_schema
        """)
        
        schemas = self.cursor.fetchall()
        for schema, table_count in schemas:
            print(f"📁 {schema}: {table_count} table(s)")
        
        return [schema[0] for schema in schemas]
    
    def get_table_structure(self, schema_name, table_name):
        """Get detailed table structure"""
        print(f"\n📋 TABLE: {schema_name}.{table_name}")
        print("=" * 50)
        
        # Get column information
        self.cursor.execute("""
            SELECT 
                c.column_name,
                c.data_type,
                c.character_maximum_length,
                c.is_nullable,
                c.column_default,
                pgd.description
            FROM information_schema.columns c
            LEFT JOIN pg_catalog.pg_statio_all_tables st ON c.table_schema = st.schemaname 
                AND c.table_name = st.relname
            LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = st.relid 
                AND pgd.objsubid = c.ordinal_position
            WHERE c.table_schema = %s AND c.table_name = %s
            ORDER BY c.ordinal_position
        """, (schema_name, table_name))
        
        columns = self.cursor.fetchall()
        
        print("\n📝 COLUMNS:")
        print("-" * 40)
        
        for col in columns:
            col_name, data_type, max_length, nullable, default, description = col
            
            # Format data type
            type_info = data_type
            if max_length:
                type_info += f"({max_length})"
            
            # Format nullable
            null_info = "NULL" if nullable == "YES" else "NOT NULL"
            
            # Format default
            default_info = f", DEFAULT: {default}" if default else ""
            
            print(f"   🔸 {col_name:<25} | {type_info:<20} | {null_info}{default_info}")
            if description:
                print(f"      📖 {description}")
        
        # Get indexes
        self.cursor.execute("""
            SELECT 
                i.relname as index_name,
                a.attname as column_name,
                ix.indisunique as is_unique,
                ix.indisprimary as is_primary
            FROM pg_class t, pg_class i, pg_index ix, pg_attribute a
            WHERE t.oid = ix.indrelid
                AND i.oid = ix.indexrelid
                AND a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                AND t.relname = %s
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
            ORDER BY i.relname, a.attname
        """, (table_name, schema_name))
        
        indexes = self.cursor.fetchall()
        if indexes:
            print("\n🔍 INDEXES:")
            print("-" * 40)
            current_index = None
            for idx_name, col_name, is_unique, is_primary in indexes:
                if idx_name != current_index:
                    current_index = idx_name
                    idx_type = "PRIMARY KEY" if is_primary else ("UNIQUE" if is_unique else "INDEX")
                    print(f"   🔑 {idx_name} ({idx_type})")
                print(f"      ↳ {col_name}")
    
    def get_sample_data(self, schema_name, table_name):
        """Get sample data from table"""
        print(f"\n📊 SAMPLE DATA FROM {schema_name}.{table_name}:")
        print("-" * 50)
        
        try:
            # Get row count first
            self.cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            row_count = self.cursor.fetchone()[0]
            print(f"📈 Total Records: {row_count:,}")
            
            if row_count == 0:
                print("⚠️  No data available in this table")
                return
            
            # Get column names
            self.cursor.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT 1")
            sample_data = self.cursor.fetchone()
            column_names = [desc[0] for desc in self.cursor.description]
            
            print("\n🔍 SAMPLE RECORD:")
            print("-" * 30)
            
            for i, (col_name, value) in enumerate(zip(column_names, sample_data)):
                # Format value for display
                if value is None:
                    display_value = "NULL"
                elif isinstance(value, str) and len(value) > 50:
                    display_value = value[:47] + "..."
                else:
                    display_value = str(value)
                
                print(f"   {col_name:<25}: {display_value}")
        
        except Exception as e:
            print(f"❌ Error retrieving sample data: {e}")
    
    def get_table_relationships(self, schema_name, table_name):
        """Get foreign key relationships"""
        print(f"\n🔗 RELATIONSHIPS FOR {schema_name}.{table_name}:")
        print("-" * 50)
        
        # Foreign keys FROM this table
        self.cursor.execute("""
            SELECT 
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = %s 
                AND tc.table_name = %s
        """, (schema_name, table_name))
        
        foreign_keys = self.cursor.fetchall()
        
        if foreign_keys:
            print("📤 REFERENCES (Foreign Keys):")
            for col, f_schema, f_table, f_col in foreign_keys:
                print(f"   {col} → {f_schema}.{f_table}.{f_col}")
        
        # Foreign keys TO this table
        self.cursor.execute("""
            SELECT 
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.column_name AS referenced_column
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND ccu.table_schema = %s 
                AND ccu.table_name = %s
        """, (schema_name, table_name))
        
        referenced_by = self.cursor.fetchall()
        
        if referenced_by:
            print("📥 REFERENCED BY:")
            for r_schema, r_table, r_col, ref_col in referenced_by:
                print(f"   {r_schema}.{r_table}.{r_col} → {ref_col}")
        
        if not foreign_keys and not referenced_by:
            print("📭 No foreign key relationships found")
    
    def analyze_data_quality(self, schema_name, table_name):
        """Analyze data quality metrics"""
        print(f"\n📊 DATA QUALITY ANALYSIS FOR {schema_name}.{table_name}:")
        print("-" * 50)
        
        try:
            # Get column nullability stats
            self.cursor.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT CASE WHEN table_schema = '{schema_name}' THEN 'subject_id' END) as unique_patients
                FROM information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
            """)
            
            # Get basic stats
            self.cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name}")
            total_rows = self.cursor.fetchone()[0]
            
            if total_rows > 0:
                # Check for common ID columns
                self.cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema_name}' 
                        AND table_name = '{table_name}'
                        AND column_name IN ('subject_id', 'stay_id', 'hadm_id', 'patient_id')
                """)
                id_columns = [row[0] for row in self.cursor.fetchall()]
                
                print(f"📈 Total Records: {total_rows:,}")
                
                # Get unique counts for ID columns
                for col in id_columns:
                    self.cursor.execute(f"SELECT COUNT(DISTINCT {col}) FROM {schema_name}.{table_name} WHERE {col} IS NOT NULL")
                    unique_count = self.cursor.fetchone()[0]
                    print(f"👤 Unique {col}: {unique_count:,}")
                
                # Check for quality flags if they exist
                quality_columns = ['is_outlier', 'is_suspicious', 'quality_flags']
                for col in quality_columns:
                    try:
                        self.cursor.execute(f"""
                            SELECT column_name FROM information_schema.columns 
                            WHERE table_schema = '{schema_name}' 
                                AND table_name = '{table_name}' 
                                AND column_name = '{col}'
                        """)
                        if self.cursor.fetchone():
                            self.cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {col} = TRUE")
                            flag_count = self.cursor.fetchone()[0]
                            if flag_count > 0:
                                print(f"🚩 {col}: {flag_count:,} records ({flag_count/total_rows*100:.1f}%)")
                    except:
                        continue
            
        except Exception as e:
            print(f"⚠️  Could not analyze data quality: {e}")
    
    def explore_mimic_schemas(self):
        """Explore MIMIC-IV source schemas"""
        print(self.print_header("MIMIC-IV SOURCE SCHEMAS", 2))
        
        mimic_schemas = ['mimiciv_icu', 'mimiciv_hosp', 'mimiciv_core']
        
        for schema in mimic_schemas:
            try:
                self.cursor.execute("""
                    SELECT table_name, 
                           (SELECT COUNT(*) FROM information_schema.columns 
                            WHERE table_schema = %s AND table_name = t.table_name) as column_count
                    FROM information_schema.tables t
                    WHERE table_schema = %s
                    ORDER BY table_name
                """, (schema, schema))
                
                tables = self.cursor.fetchall()
                if tables:
                    print(f"\n📂 {schema}:")
                    for table_name, col_count in tables:
                        print(f"   📄 {table_name} ({col_count} columns)")
                
            except:
                print(f"⚠️  Schema {schema} not accessible")
    
    def generate_pipeline_summary(self):
        """Generate pipeline data flow summary"""
        print(self.print_header("PIPELINE DATA FLOW SUMMARY", 2))
        
        pipeline_tables = [
            ('bronze', 'collection_disease'),
            ('silver', 'collection_disease_std'),
            ('gold', 'sofa_scores'),
            ('gold', 'patient_sofa_summary'),
            ('gold', 'daily_sofa_trends')
        ]
        
        for schema, table in pipeline_tables:
            try:
                self.cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                count = self.cursor.fetchone()[0]
                print(f"📊 {schema.upper()}.{table}: {count:,} records")
            except:
                print(f"❌ {schema.upper()}.{table}: Table not found")
    
    def explore_complete_schema(self):
        """Complete schema exploration"""
        print(self.print_header("MIMIC-IV MEDALLION PIPELINE - DATABASE SCHEMA EXPLORER"))
        print(f"🕐 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🔗 Database: {DB_CONFIG['database']}")
        
        try:
            # Get all schemas
            schemas = self.get_all_schemas()
            
            # Explore pipeline schemas in detail
            medallion_schemas = ['bronze', 'silver', 'gold']
            
            for schema in medallion_schemas:
                if schema in schemas:
                    try:
                        self.print_header(f"{schema.upper()} LAYER SCHEMA", 1)
                        
                        # Get tables in this schema
                        self.cursor.execute("""
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_schema = %s
                            ORDER BY table_name
                        """, (schema,))
                        
                        tables = [row[0] for row in self.cursor.fetchall()]
                        
                        for table in tables:
                            try:
                                # Table structure
                                self.get_table_structure(schema, table)
                                
                                # Sample data
                                self.get_sample_data(schema, table)
                                
                                # Relationships
                                self.get_table_relationships(schema, table)
                                
                                # Data quality
                                self.analyze_data_quality(schema, table)
                                
                                print("\n" + "─" * 80)
                                
                            except Exception as e:
                                print(f"❌ Error exploring table {schema}.{table}: {e}")
                                # Rollback and continue
                                self.conn.rollback()
                                continue
                                
                    except Exception as e:
                        print(f"❌ Error exploring schema {schema}: {e}")
                        # Rollback and continue
                        self.conn.rollback()
                        continue
            
            # MIMIC-IV source schemas summary
            try:
                self.explore_mimic_schemas()
            except Exception as e:
                print(f"❌ Error exploring MIMIC schemas: {e}")
                self.conn.rollback()
            
            # Pipeline summary
            try:
                self.generate_pipeline_summary()
            except Exception as e:
                print(f"❌ Error generating pipeline summary: {e}")
                self.conn.rollback()
            
            print(self.print_header("SCHEMA EXPLORATION COMPLETE", 1))
            print("✅ All database schemas, tables, and relationships documented")
            print("📋 Use this information for queries, analysis, and development")
            
        except Exception as e:
            print(f"❌ Schema exploration failed: {e}")
            self.conn.rollback()
        
    def close(self):
        """Close database connection"""
        self.cursor.close()
        self.conn.close()


def main():
    """Main execution function"""
    explorer = DatabaseSchemaExplorer()
    
    try:
        explorer.explore_complete_schema()
    except Exception as e:
        print(f"❌ Schema exploration failed: {e}")
    finally:
        explorer.close()


if __name__ == "__main__":
    main()
