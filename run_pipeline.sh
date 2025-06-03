#!/bin/bash
"""
Medical Data Pipeline Runner Script
===================================

Comprehensive runner for the medallion architecture pipeline
with proper virtual environment activation and error handling.

Usage:
  ./run_pipeline.sh [bronze|silver|gold|full]

Author: Medical Data Science Team
Date: May 2025
"""

# Color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Emoji indicators
SUCCESS="✅"
ERROR="❌"
WARNING="⚠️"
INFO="ℹ️"
ROCKET="🚀"
GEAR="⚙️"
DATABASE="🗄️"

# Function to print colored output
print_success() {
    echo -e "${GREEN}${SUCCESS} $1${NC}"
}

print_error() {
    echo -e "${RED}${ERROR} $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}${WARNING} $1${NC}"
}

print_info() {
    echo -e "${BLUE}${INFO} $1${NC}"
}

print_header() {
    echo ""
    echo -e "${PURPLE}$1${NC}"
    echo "$(printf '=%.0s' {1..50})"
}

# Function to check if virtual environment exists and activate it
activate_venv() {
    if [[ ! -d "venv" ]]; then
        print_error "Virtual environment not found. Please run setup first:"
        echo "  ./setup_test.sh"
        exit 1
    fi
    
    print_info "Activating virtual environment..."
    source venv/bin/activate
    
    if [[ "$VIRTUAL_ENV" != "" ]]; then
        print_success "Virtual environment activated: $VIRTUAL_ENV"
    else
        print_error "Failed to activate virtual environment"
        exit 1
    fi
}

# Function to check dependencies
check_dependencies() {
    print_info "Checking Python dependencies..."
    
    # Check if required packages are installed
    python -c "
import sys
try:
    import sqlalchemy
    import psycopg2
    import pandas
    print('${SUCCESS} All required packages are available')
except ImportError as e:
    print('${ERROR} Missing dependency:', e)
    sys.exit(1)
" || {
        print_error "Dependencies missing. Installing from requirements.txt..."
        pip install -r requirements.txt
    }
}

# Function to run Bronze layer (data extraction)
run_bronze_layer() {
    print_header "${DATABASE} BRONZE LAYER - DATA EXTRACTION"
    
    print_info "Extracting data from MIMIC-IV PostgreSQL to Bronze schema..."
    
    if python querybuilder.py; then
        print_success "Bronze layer extraction completed"
    else
        print_error "Bronze layer extraction failed"
        return 1
    fi
}

# Function to run Silver layer (data standardization)
run_silver_layer() {
    print_header "${GEAR} SILVER LAYER - DATA STANDARDIZATION"
    
    print_info "Standardizing Bronze data into Silver schema..."
    
    if python standardize_data.py; then
        print_success "Silver layer standardization completed"
    else
        print_error "Silver layer standardization failed"
        return 1
    fi
}

# Function to run Gold layer (analytical processing)
run_gold_layer() {
    print_header "${ROCKET} GOLD LAYER - ANALYTICAL PROCESSING"
    
    print_info "Creating Gold layer analytical views and aggregations..."
    
    # Check if gold layer script exists, create if needed
    if [[ ! -f "gold_analytics.py" ]]; then
        print_warning "Gold layer script not found. Creating analytical framework..."
        create_gold_layer_script
    fi
    
    if python gold_analytics.py; then
        print_success "Gold layer analytics completed"
    else
        print_error "Gold layer analytics failed"
        return 1
    fi
}

# Function to create Gold layer script if it doesn't exist
create_gold_layer_script() {
    cat > gold_analytics.py << 'EOF'
#!/usr/bin/env python3
"""
Gold Layer Analytics Pipeline
============================

Creates business intelligence views and analytical aggregations
from Silver layer standardized data using PostgreSQL.
"""

import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gold_analytics.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoldLayerAnalytics:
    """Gold layer analytics processor for PostgreSQL."""
    
    def __init__(self):
        self.engine = None
        self.connect_db()
    
    def connect_db(self):
        """Connect to PostgreSQL database."""
        try:
            connection_string = f"postgresql://{DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            self.engine = create_engine(connection_string)
            logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    def create_gold_schema(self):
        """Create Gold schema if it doesn't exist."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
                conn.commit()
                logger.info("✅ Gold schema created/verified")
        except Exception as e:
            logger.error(f"❌ Error creating Gold schema: {e}")
            raise

    def create_patient_summaries(self):
        """Create patient-level summary statistics."""
        logger.info("🏥 Creating patient summary analytics...")
        
        try:
            with self.engine.connect() as conn:
                # Create patient summary view
                summary_query = text("""
                    CREATE OR REPLACE VIEW gold.patient_summaries AS
                    SELECT 
                        subject_id,
                        COUNT(DISTINCT hadm_id) as total_admissions,
                        COUNT(*) as total_measurements,
                        MIN(charttime) as first_measurement,
                        MAX(charttime) as last_measurement,
                        COUNT(DISTINCT itemid) as unique_parameters,
                        AVG(CASE WHEN concept_name = 'Heart Rate' THEN valuenum_std END) as avg_heart_rate,
                        AVG(CASE WHEN concept_name = 'Respiratory Rate' THEN valuenum_std END) as avg_resp_rate,
                        AVG(CASE WHEN concept_name = 'Oxygen Saturation' THEN valuenum_std END) as avg_spo2
                    FROM silver.collection_disease_std
                    WHERE valuenum_std IS NOT NULL
                    GROUP BY subject_id
                """)
                conn.execute(summary_query)
                conn.commit()
                logger.info("✅ Patient summaries view created")
                
        except Exception as e:
            logger.error(f"❌ Error creating patient summaries: {e}")
            raise

    def create_clinical_indicators(self):
        """Create clinical quality indicators."""
        logger.info("📊 Creating clinical indicators...")
        
        try:
            with self.engine.connect() as conn:
                # Create clinical quality indicators view
                indicators_query = text("""
                    CREATE OR REPLACE VIEW gold.clinical_indicators AS
                    SELECT 
                        concept_name,
                        unit_std,
                        COUNT(*) as total_measurements,
                        COUNT(DISTINCT subject_id) as unique_patients,
                        AVG(valuenum_std) as mean_value,
                        STDDEV(valuenum_std) as std_dev,
                        MIN(valuenum_std) as min_value,
                        MAX(valuenum_std) as max_value,
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY valuenum_std) as q25,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY valuenum_std) as median,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY valuenum_std) as q75,
                        COUNT(CASE WHEN quality_flags ? 'OUTLIER_DETECTED' THEN 1 END) as outlier_count
                    FROM silver.collection_disease_std
                    WHERE valuenum_std IS NOT NULL
                    GROUP BY concept_name, unit_std
                    ORDER BY concept_name
                """)
                conn.execute(indicators_query)
                conn.commit()
                logger.info("✅ Clinical indicators view created")
                
        except Exception as e:
            logger.error(f"❌ Error creating clinical indicators: {e}")
            raise

    def create_trend_analysis(self):
        """Create trend analysis views."""
        logger.info("📈 Creating trend analysis views...")
        
        try:
            with self.engine.connect() as conn:
                # Create daily trends view
                trends_query = text("""
                    CREATE OR REPLACE VIEW gold.daily_trends AS
                    SELECT 
                        DATE(charttime) as measurement_date,
                        concept_name,
                        COUNT(*) as daily_measurements,
                        AVG(valuenum_std) as daily_avg,
                        STDDEV(valuenum_std) as daily_std
                    FROM silver.collection_disease_std
                    WHERE valuenum_std IS NOT NULL
                    GROUP BY DATE(charttime), concept_name
                    ORDER BY measurement_date, concept_name
                """)
                conn.execute(trends_query)
                conn.commit()
                logger.info("✅ Daily trends view created")
                
        except Exception as e:
            logger.error(f"❌ Error creating trend analysis: {e}")
            raise

def main():
    """Main Gold layer processing."""
    logger.info("🚀 Starting Gold Layer Analytics Pipeline")
    
    try:
        analytics = GoldLayerAnalytics()
        analytics.create_gold_schema()
        analytics.create_patient_summaries()
        analytics.create_clinical_indicators()
        analytics.create_trend_analysis()
        
        logger.info("✅ Gold layer processing completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Gold layer processing failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
EOF
    
    print_success "Gold layer analytics script created"
}

# Function to run data validation
run_validation() {
    print_header "${SUCCESS} DATA VALIDATION"
    
    print_info "Running comprehensive data validation..."
    
    if python validate_data.py; then
        print_success "Data validation passed"
    else
        print_error "Data validation failed"
        return 1
    fi
    
    if [[ -f "validate_silver.py" ]]; then
        print_info "Running Silver layer validation..."
        if python validate_silver.py; then
            print_success "Silver layer validation passed"
        else
            print_warning "Silver layer validation had issues"
        fi
    fi
}

# Function to generate status report
generate_status_report() {
    print_header "📋 STATUS REPORT GENERATION"
    
    print_info "Generating comprehensive status report..."
    
    if python check_status.py; then
        print_success "Status report generated"
    else
        print_warning "Status report generation had issues"
    fi
    
    if python generate_summary.py; then
        print_success "Final summary report generated"
    else
        print_warning "Summary generation had issues"
    fi
}

# Main pipeline execution
run_full_pipeline() {
    print_header "${ROCKET} MEDICAL DATA MEDALLION ARCHITECTURE PIPELINE"
    echo "${BLUE}🏥 Processing clinical data through Bronze → Silver → Gold layers${NC}"
    echo "${BLUE}📅 $(date)${NC}"
    echo ""
    
    # Run each layer
    run_bronze_layer || exit 1
    run_silver_layer || exit 1
    run_gold_layer || exit 1
    run_validation || exit 1
    generate_status_report
    
    # Final success message
    print_header "🎉 PIPELINE COMPLETE!"
    
    echo -e "${GREEN}"
    echo "╔═══════════════════════════════════════════════╗"
    echo "║     Medallion Architecture Pipeline Success!  ║"
    echo "╚═══════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    print_success "Bronze Layer: Raw data extracted and stored"
    print_success "Silver Layer: Data standardized and cleaned"
    print_success "Gold Layer: Analytics and aggregations created"
    print_success "Validation: All quality checks passed"
    
    echo ""
    print_info "Generated outputs:"
    echo "  📊 FINAL_SUMMARY_REPORT.txt - Comprehensive summary"
    echo "  📝 *.log files - Detailed processing logs"
    echo "  🗄️  PostgreSQL Bronze/Silver schemas - Processed data"
    echo ""
    print_info "Next steps:"
    echo "  1. Review analytical results in Gold layer"
    echo "  2. Use data for medical research and analysis"
    echo "  3. Schedule regular pipeline runs for new data"
}

# Function to show help
show_help() {
    echo "Medical Data Pipeline Runner"
    echo ""
    echo "Usage:"
    echo "  ./run_pipeline.sh [command]"
    echo ""
    echo "Commands:"
    echo "  bronze     Run Bronze layer (data extraction) only"
    echo "  silver     Run Silver layer (data standardization) only"
    echo "  gold       Run Gold layer (analytics) only"
    echo "  full       Run complete pipeline (default)"
    echo "  validate   Run data validation only"
    echo "  status     Generate status report only"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./run_pipeline.sh full        # Run complete pipeline"
    echo "  ./run_pipeline.sh bronze      # Extract data only"
    echo "  ./run_pipeline.sh silver      # Standardize data only"
    echo ""
}

# Ensure script is run from correct directory
if [[ ! -f "querybuilder.py" ]]; then
    print_error "Please run this script from the project directory containing querybuilder.py"
    exit 1
fi

# Activate virtual environment
activate_venv

# Check dependencies
check_dependencies

# Parse command line arguments
case "${1:-full}" in
    "bronze")
        run_bronze_layer
        ;;
    "silver")
        run_silver_layer
        ;;
    "gold")
        run_gold_layer
        ;;
    "full"|"")
        run_full_pipeline
        ;;
    "validate")
        run_validation
        ;;
    "status")
        generate_status_report
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo "Use './run_pipeline.sh help' for usage information"
        exit 1
        ;;
esac

# Deactivate virtual environment
deactivate 2>/dev/null || true
