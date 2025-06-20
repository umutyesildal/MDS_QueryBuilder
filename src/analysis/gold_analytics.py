#!/usr/bin/env python3
"""
Gold Layer Analytics Pipeline
============================

Creates business intelligence views and analytical aggregations
from Silver layer standardized data using PostgreSQL.

This module implements the Gold layer of the medallion architecture,
transforming Silver layer cleaned data into analytical views and KPIs.
"""

import logging
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

# Setup comprehensive logging with proper log directory
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from file_paths import get_log_path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(get_log_path('gold_analytics.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoldLayerAnalytics:
    """Gold layer analytics processor for PostgreSQL medallion architecture."""
    
    def __init__(self):
        """Initialize Gold layer processor."""
        self.engine = None
        self.stats = {
            'views_created': 0,
            'tables_created': 0,
            'processing_start': datetime.now(),
            'errors': []
        }
        self.connect_db()
    
    def connect_db(self):
        """Connect to PostgreSQL database."""
        try:
            # Build connection string from config
            if DB_CONFIG.get('password'):
                connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            else:
                connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            
            self.engine = create_engine(connection_string)
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("✅ Connected to PostgreSQL database successfully")
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def create_gold_schema(self):
        """Create Gold schema and supporting objects."""
        logger.info("🔧 Creating Gold schema infrastructure...")
        
        try:
            with self.engine.connect() as conn:
                # Create Gold schema
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
                
                # Add comments for documentation
                conn.execute(text("""
                    COMMENT ON SCHEMA gold IS 
                    'Gold layer - Business intelligence views and analytical aggregations from Silver layer data'
                """))
                
                conn.commit()
                logger.info("✅ Gold schema created/verified")
                
        except Exception as e:
            logger.error(f"❌ Error creating Gold schema: {e}")
            self.stats['errors'].append(f"Schema creation: {str(e)}")
            raise

    def create_patient_summaries(self):
        """Create comprehensive patient-level summary analytics."""
        logger.info("🏥 Creating patient summary analytics...")
        
        try:
            with self.engine.connect() as conn:
                # Patient summary view with comprehensive metrics
                summary_query = text("""
                    CREATE OR REPLACE VIEW gold.patient_summaries AS
                    SELECT 
                        s.subject_id,
                        COUNT(DISTINCT s.hadm_id) as total_admissions,
                        COUNT(*) as total_measurements,
                        MIN(s.charttime) as first_measurement,
                        MAX(s.charttime) as last_measurement,
                        EXTRACT(DAYS FROM (MAX(s.charttime) - MIN(s.charttime))) as measurement_span_days,
                        COUNT(DISTINCT s.itemid) as unique_parameters,
                        
                        -- Vital signs averages
                        ROUND(AVG(CASE WHEN s.concept_name = 'Heart Rate' THEN s.valuenum_std END), 1) as avg_heart_rate,
                        ROUND(AVG(CASE WHEN s.concept_name = 'Respiratory Rate' THEN s.valuenum_std END), 1) as avg_resp_rate,
                        ROUND(AVG(CASE WHEN s.concept_name = 'Oxygen Saturation' THEN s.valuenum_std END), 1) as avg_spo2,
                        
                        -- Lab values averages  
                        ROUND(AVG(CASE WHEN s.concept_name = 'Creatinine' THEN s.valuenum_std END), 2) as avg_creatinine,
                        ROUND(AVG(CASE WHEN s.concept_name = 'pH' THEN s.valuenum_std END), 2) as avg_ph,
                        ROUND(AVG(CASE WHEN s.concept_name = 'Lactate' THEN s.valuenum_std END), 2) as avg_lactate,
                        
                        -- Quality metrics
                        COUNT(CASE WHEN s.quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) as outlier_count,
                        COUNT(CASE WHEN s.quality_flags ? 'UNIT_CONVERTED' THEN 1 END) as unit_conversions,
                        ROUND(100.0 * COUNT(CASE WHEN s.quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) / COUNT(*), 2) as outlier_percentage,
                        
                        -- Source distribution
                        COUNT(CASE WHEN s.source_table = 'chartevents' THEN 1 END) as chart_measurements,
                        COUNT(CASE WHEN s.source_table = 'labevents' THEN 1 END) as lab_measurements
                        
                    FROM silver.collection_disease_std s
                    WHERE s.valuenum_std IS NOT NULL
                    GROUP BY s.subject_id
                    ORDER BY total_measurements DESC
                """)
                
                conn.execute(summary_query)
                
                # Add helpful comments
                conn.execute(text("""
                    COMMENT ON VIEW gold.patient_summaries IS 
                    'Comprehensive patient-level summary statistics including vital signs, lab values, and data quality metrics'
                """))
                
                conn.commit()
                self.stats['views_created'] += 1
                logger.info("✅ Patient summaries view created")
                
        except Exception as e:
            logger.error(f"❌ Error creating patient summaries: {e}")
            self.stats['errors'].append(f"Patient summaries: {str(e)}")
            raise

    def create_clinical_indicators(self):
        """Create clinical quality indicators and parameter statistics."""
        logger.info("📊 Creating clinical quality indicators...")
        
        try:
            with self.engine.connect() as conn:
                # Clinical quality indicators view
                indicators_query = text("""
                    CREATE OR REPLACE VIEW gold.clinical_indicators AS
                    SELECT 
                        s.concept_name,
                        s.unit_std as standard_unit,
                        COUNT(*) as total_measurements,
                        COUNT(DISTINCT s.subject_id) as unique_patients,
                        COUNT(DISTINCT s.hadm_id) as unique_admissions,
                        
                        -- Statistical measures
                        ROUND(AVG(s.valuenum_std), 3) as mean_value,
                        ROUND(STDDEV(s.valuenum_std), 3) as std_deviation,
                        ROUND(MIN(s.valuenum_std), 3) as min_value,
                        ROUND(MAX(s.valuenum_std), 3) as max_value,
                        
                        -- Percentiles
                        ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY s.valuenum_std), 3) as p05,
                        ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY s.valuenum_std), 3) as q25,
                        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.valuenum_std), 3) as median,
                        ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY s.valuenum_std), 3) as q75,
                        ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY s.valuenum_std), 3) as p95,
                        
                        -- Quality metrics
                        COUNT(CASE WHEN s.quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) as outlier_count,
                        COUNT(CASE WHEN s.quality_flags ? 'UNIT_CONVERTED' THEN 1 END) as unit_conversions,
                        COUNT(CASE WHEN s.quality_flags ? 'DUPLICATE_RESOLVED' THEN 1 END) as duplicates_resolved,
                        
                        -- Quality percentages
                        ROUND(100.0 * COUNT(CASE WHEN s.quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) / COUNT(*), 2) as outlier_percentage,
                        ROUND(100.0 * COUNT(CASE WHEN s.quality_flags ? 'UNIT_CONVERTED' THEN 1 END) / COUNT(*), 2) as conversion_percentage,
                        
                        -- Temporal coverage
                        MIN(s.charttime) as earliest_measurement,
                        MAX(s.charttime) as latest_measurement,
                        EXTRACT(DAYS FROM (MAX(s.charttime) - MIN(s.charttime))) as measurement_span_days
                        
                    FROM silver.collection_disease_std s
                    WHERE s.valuenum_std IS NOT NULL
                    GROUP BY s.concept_name, s.unit_std
                    ORDER BY s.concept_name, total_measurements DESC
                """)
                
                conn.execute(indicators_query)
                
                conn.execute(text("""
                    COMMENT ON VIEW gold.clinical_indicators IS 
                    'Clinical parameter statistics including distributions, quality metrics, and temporal coverage'
                """))
                
                conn.commit()
                self.stats['views_created'] += 1
                logger.info("✅ Clinical indicators view created")
                
        except Exception as e:
            logger.error(f"❌ Error creating clinical indicators: {e}")
            self.stats['errors'].append(f"Clinical indicators: {str(e)}")
            raise

    def create_temporal_analytics(self):
        """Create temporal trend analysis views."""
        logger.info("📈 Creating temporal analytics views...")
        
        try:
            with self.engine.connect() as conn:
                # Daily trends analysis
                daily_trends_query = text("""
                    CREATE OR REPLACE VIEW gold.daily_trends AS
                    SELECT 
                        DATE(s.charttime) as measurement_date,
                        s.concept_name,
                        COUNT(*) as daily_measurements,
                        COUNT(DISTINCT s.subject_id) as unique_patients,
                        ROUND(AVG(s.valuenum_std), 3) as daily_avg,
                        ROUND(STDDEV(s.valuenum_std), 3) as daily_std,
                        ROUND(MIN(s.valuenum_std), 3) as daily_min,
                        ROUND(MAX(s.valuenum_std), 3) as daily_max,
                        COUNT(CASE WHEN s.quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) as daily_outliers
                    FROM silver.collection_disease_std s
                    WHERE s.valuenum_std IS NOT NULL
                    GROUP BY DATE(s.charttime), s.concept_name
                    ORDER BY measurement_date DESC, s.concept_name
                """)
                
                conn.execute(daily_trends_query)
                
                # Hourly patterns analysis  
                hourly_patterns_query = text("""
                    CREATE OR REPLACE VIEW gold.hourly_patterns AS
                    SELECT 
                        EXTRACT(HOUR FROM s.charttime) as measurement_hour,
                        s.concept_name,
                        COUNT(*) as hourly_measurements,
                        ROUND(AVG(s.valuenum_std), 3) as hourly_avg,
                        ROUND(STDDEV(s.valuenum_std), 3) as hourly_std
                    FROM silver.collection_disease_std s
                    WHERE s.valuenum_std IS NOT NULL
                    GROUP BY EXTRACT(HOUR FROM s.charttime), s.concept_name
                    ORDER BY measurement_hour, s.concept_name
                """)
                
                conn.execute(hourly_patterns_query)
                
                # Add comments
                conn.execute(text("""
                    COMMENT ON VIEW gold.daily_trends IS 
                    'Daily aggregated trends for all clinical parameters with quality metrics'
                """))
                
                conn.execute(text("""
                    COMMENT ON VIEW gold.hourly_patterns IS 
                    'Hourly patterns showing circadian rhythms and measurement frequency by time of day'
                """))
                
                conn.commit()
                self.stats['views_created'] += 2
                logger.info("✅ Temporal analytics views created")
                
        except Exception as e:
            logger.error(f"❌ Error creating temporal analytics: {e}")
            self.stats['errors'].append(f"Temporal analytics: {str(e)}")
            raise

    def create_data_quality_dashboard(self):
        """Create comprehensive data quality dashboard views."""
        logger.info("🔍 Creating data quality dashboard...")
        
        try:
            with self.engine.connect() as conn:
                # Data quality summary
                quality_summary_query = text("""
                    CREATE OR REPLACE VIEW gold.data_quality_summary AS
                    SELECT 
                        'Silver Layer Overview' as metric_category,
                        'Total Records' as metric_name,
                        COUNT(*)::TEXT as metric_value,
                        'records' as unit
                    FROM silver.collection_disease_std
                    
                    UNION ALL
                    
                    SELECT 
                        'Silver Layer Overview',
                        'Unique Patients',
                        COUNT(DISTINCT subject_id)::TEXT,
                        'patients'
                    FROM silver.collection_disease_std
                    
                    UNION ALL
                    
                    SELECT 
                        'Silver Layer Overview',
                        'Unique Parameters',
                        COUNT(DISTINCT concept_name)::TEXT,
                        'parameters'
                    FROM silver.collection_disease_std
                    
                    UNION ALL
                    
                    SELECT 
                        'Data Quality',
                        'Outliers Detected',
                        COUNT(CASE WHEN quality_flags ? 'OUTLIER_DETECTED' THEN 1 END)::TEXT,
                        'records'
                    FROM silver.collection_disease_std
                    
                    UNION ALL
                    
                    SELECT 
                        'Data Quality',
                        'Unit Conversions',
                        COUNT(CASE WHEN quality_flags ? 'UNIT_CONVERTED' THEN 1 END)::TEXT,
                        'records'
                    FROM silver.collection_disease_std
                    
                    UNION ALL
                    
                    SELECT 
                        'Data Quality',
                        'Duplicates Resolved',
                        COUNT(CASE WHEN quality_flags ? 'DUPLICATE_RESOLVED' THEN 1 END)::TEXT,
                        'records'
                    FROM silver.collection_disease_std
                    
                    ORDER BY metric_category, metric_name
                """)
                
                conn.execute(quality_summary_query)
                
                conn.execute(text("""
                    COMMENT ON VIEW gold.data_quality_summary IS 
                    'High-level data quality metrics and summary statistics for the Silver layer'
                """))
                
                conn.commit()
                self.stats['views_created'] += 1
                logger.info("✅ Data quality dashboard created")
                
        except Exception as e:
            logger.error(f"❌ Error creating data quality dashboard: {e}")
            self.stats['errors'].append(f"Data quality dashboard: {str(e)}")
            raise

    def validate_gold_layer(self):
        """Validate Gold layer views and data integrity."""
        logger.info("🔍 Validating Gold layer integrity...")
        
        validation_results = {}
        
        try:
            with self.engine.connect() as conn:
                # Check each view exists and has data
                views_to_check = [
                    'gold.patient_summaries',
                    'gold.clinical_indicators', 
                    'gold.daily_trends',
                    'gold.hourly_patterns',
                    'gold.data_quality_summary'
                ]
                
                for view_name in views_to_check:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {view_name}")).scalar()
                        validation_results[view_name] = result
                        logger.info(f"✅ {view_name}: {result:,} records")
                    except Exception as e:
                        logger.error(f"❌ {view_name}: Validation failed - {e}")
                        validation_results[view_name] = f"ERROR: {e}"
                
                return validation_results
                
        except Exception as e:
            logger.error(f"❌ Gold layer validation failed: {e}")
            self.stats['errors'].append(f"Validation: {str(e)}")
            return {}

    def generate_analytics_report(self):
        """Generate comprehensive analytics report."""
        logger.info("📊 Generating Gold layer analytics report...")
        
        try:
            with self.engine.connect() as conn:
                # Get key metrics
                total_views = conn.execute(text("""
                    SELECT COUNT(*) FROM information_schema.views 
                    WHERE table_schema = 'gold'
                """)).scalar()
                
                # Create report
                report_lines = [
                    "=" * 70,
                    "GOLD LAYER ANALYTICS REPORT",
                    "=" * 70,
                    f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    f"Processing Duration: {datetime.now() - self.stats['processing_start']}",
                    "",
                    "📊 GOLD LAYER SUMMARY",
                    "-" * 30,
                    f"Views Created: {self.stats['views_created']}",
                    f"Tables Created: {self.stats['tables_created']}",
                    f"Total Gold Views: {total_views}",
                    "",
                    "🎯 AVAILABLE ANALYTICS VIEWS",
                    "-" * 30,
                    "• gold.patient_summaries - Patient-level aggregated metrics",
                    "• gold.clinical_indicators - Parameter statistics and quality metrics", 
                    "• gold.daily_trends - Daily aggregated trends",
                    "• gold.hourly_patterns - Hourly measurement patterns",
                    "• gold.data_quality_summary - Data quality dashboard",
                    "",
                    "📈 USAGE EXAMPLES",
                    "-" * 30,
                    "-- Top 10 patients by measurement count",
                    "SELECT subject_id, total_measurements FROM gold.patient_summaries LIMIT 10;",
                    "",
                    "-- Parameter quality overview", 
                    "SELECT concept_name, total_measurements, outlier_percentage FROM gold.clinical_indicators;",
                    "",
                    "-- Recent daily trends",
                    "SELECT * FROM gold.daily_trends WHERE measurement_date >= CURRENT_DATE - 7;",
                    "",
                ]
                
                if self.stats['errors']:
                    report_lines.extend([
                        "⚠️ ERRORS ENCOUNTERED",
                        "-" * 30,
                    ])
                    for error in self.stats['errors']:
                        report_lines.append(f"• {error}")
                    report_lines.append("")
                
                report_lines.extend([
                    "✅ Gold layer analytics processing completed!",
                    "=" * 70
                ])
                
                # Save report to docs/reports directory
                import sys
                import os
                sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
                from file_paths import get_report_path
                
                report_path = get_report_path('gold_analytics_report.txt')
                with open(report_path, 'w') as f:
                    f.write('\n'.join(report_lines))
                
                logger.info(f"✅ Analytics report saved to {report_path}")
                
        except Exception as e:
            logger.error(f"❌ Error generating analytics report: {e}")

    def run_analytics_pipeline(self):
        """Run complete Gold layer analytics pipeline."""
        logger.info("🚀 Starting comprehensive Gold layer analytics pipeline")
        
        try:
            # Execute all Gold layer components
            self.create_gold_schema()
            self.create_patient_summaries()
            self.create_clinical_indicators()
            self.create_temporal_analytics()
            self.create_data_quality_dashboard()
            
            # Validate results
            validation_results = self.validate_gold_layer()
            
            # Generate final report
            self.generate_analytics_report()
            
            # Log final statistics
            duration = datetime.now() - self.stats['processing_start']
            logger.info(f"✅ Gold layer analytics completed successfully!")
            logger.info(f"📊 Views created: {self.stats['views_created']}")
            logger.info(f"⏱️  Processing time: {duration}")
            
            if not self.stats['errors']:
                logger.info("🎉 No errors encountered during processing")
            else:
                logger.warning(f"⚠️ {len(self.stats['errors'])} errors encountered - check logs")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Gold layer analytics pipeline failed: {e}")
            return False

def main():
    """Main entry point for Gold layer analytics."""
    try:
        analytics = GoldLayerAnalytics()
        success = analytics.run_analytics_pipeline()
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"❌ Critical error in Gold layer analytics: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
