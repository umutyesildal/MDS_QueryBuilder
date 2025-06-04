#!/bin/bash
# =============================================================================
# Medallion Architecture Healthcare Data Pipeline - Complete Setup Script
# =============================================================================
# Medical Data Science - Übungsblatt 3.2
# 
# This script sets up the complete medallion architecture pipeline:
# 1. Bronze Layer (Raw Data Extraction)
# 2. Silver Layer (Data Standardization) 
# 3. Validates the entire pipeline
#
# Usage: ./setup.sh [option]
# Options:
#   full     - Complete setup (Bronze + Silver) [DEFAULT]
#   bronze   - Bronze layer only
#   silver   - Silver layer only (requires Bronze)
#   test     - Quick test of existing setup
#   help     - Show this help message
# =============================================================================

set -e  # Exit on any error

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
DATABASE="🗄️"
GEAR="⚙️"
MEDAL="🏆"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

print_header() {
    echo ""
    echo -e "${PURPLE}================================${NC}"
    echo -e "${PURPLE}$1${NC}"
    echo -e "${PURPLE}================================${NC}"
}

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

print_step() {
    echo -e "${CYAN}${GEAR} $1${NC}"
}

# Function to activate virtual environment
activate_venv() {
    if [[ ! -d "venv" ]]; then
        print_error "Virtual environment not found. Creating..."
        python3 -m venv venv
        print_success "Virtual environment created"
    fi
    
    print_info "Activating virtual environment..."
    source venv/bin/activate
    
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        print_error "Failed to activate virtual environment"
        exit 1
    fi
    
    print_success "Virtual environment activated: $VIRTUAL_ENV"
}

# Function to install dependencies
install_dependencies() {
    print_step "Installing Python dependencies..."
    pip install -q -r requirements.txt
    print_success "Dependencies installed"
}

# Function to check database configuration
check_db_config() {
    print_step "Checking database configuration..."
    
    if [[ ! -f "config_local.py" ]]; then
        print_warning "config_local.py not found"
        
        if [[ -f "config_template.py" ]]; then
            print_info "Creating config_local.py from template..."
            cp config_template.py config_local.py
            print_warning "Please edit config_local.py with your database credentials"
            
            # Try to open with common editors
            if command -v code &> /dev/null; then
                code config_local.py
            elif command -v nano &> /dev/null; then
                nano config_local.py
            else
                print_info "Please manually edit config_local.py"
            fi
            
            echo ""
            read -p "Press Enter after updating config_local.py with your database credentials..."
        else
            print_error "config_template.py not found. Cannot create local configuration."
            exit 1
        fi
    else
        print_success "config_local.py found"
    fi
    
    # Test database connection
    print_step "Testing database connection..."
    if python test_db.py; then
        print_success "Database connection successful"
    else
        print_error "Database connection failed"
        print_warning "Please check your config_local.py settings"
        exit 1
    fi
}

# =============================================================================
# BRONZE LAYER SETUP
# =============================================================================

setup_bronze_layer() {
    print_header "${DATABASE} BRONZE LAYER SETUP"
    
    print_step "Checking if Bronze data exists..."
    
    bronze_exists=false
    record_count=0
    
    if python -c "
import sys
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
        count = result.fetchone()[0]
        print(count)
        sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        record_count=$(python -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
    count = result.fetchone()[0]
    print(count)
")
        bronze_exists=true
        print_success "Found $record_count existing Bronze records"
    else
        print_info "No Bronze data found"
    fi
    
    if [[ $bronze_exists == true && $record_count -gt 0 ]]; then
        echo ""
        read -p "Bronze data exists ($record_count records). Re-extract? (y/N): " re_extract
        if [[ ! $re_extract =~ ^[Yy]$ ]]; then
            print_info "Skipping Bronze extraction (existing data preserved)"
            return 0
        fi
    fi
    
    print_step "Starting Bronze layer data extraction..."
    if python querybuilder.py; then
        print_success "Bronze layer extraction completed successfully"
        
        # Validate Bronze data
        print_step "Validating Bronze layer data..."
        if python validate_data.py; then
            print_success "Bronze layer validation passed"
        else
            print_warning "Bronze layer validation had issues"
        fi
    else
        print_error "Bronze layer extraction failed"
        exit 1
    fi
}

# =============================================================================
# SILVER LAYER SETUP
# =============================================================================

setup_silver_layer() {
    print_header "${GEAR} SILVER LAYER SETUP"
    
    # Check if Bronze data exists first
    print_step "Checking Bronze layer prerequisite..."
    
    if ! python -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
    count = result.fetchone()[0]
    if count > 0:
        print(f'✅ Bronze layer ready: {count} records')
        exit(0)
    else:
        exit(1)
" 2>/dev/null; then
        print_error "Bronze layer data not found. Cannot proceed with Silver layer."
        print_info "Please run Bronze layer setup first: ./setup.sh bronze"
        exit 1
    fi
    
    # Check if Silver data exists
    print_step "Checking if Silver data exists..."
    
    silver_exists=false
    silver_count=0
    
    if python -c "
import sys
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
        count = result.fetchone()[0]
        print(count)
        sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        silver_count=$(python -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
    count = result.fetchone()[0]
    print(count)
")
        silver_exists=true
        print_success "Found $silver_count existing Silver records"
    else
        print_info "No Silver data found"
    fi
    
    if [[ $silver_exists == true && $silver_count -gt 0 ]]; then
        echo ""
        read -p "Silver data exists ($silver_count records). Re-process? (y/N): " re_process
        if [[ ! $re_process =~ ^[Yy]$ ]]; then
            print_info "Skipping Silver processing (existing data preserved)"
            return 0
        fi
    fi
    
    print_step "Starting Silver layer data standardization..."
    if python standardize_data.py; then
        print_success "Silver layer processing completed successfully"
        
        # Validate Silver data
        print_step "Validating Silver layer data..."
        if python validate_silver.py; then
            print_success "Silver layer validation passed"
        else
            print_warning "Silver layer validation had issues"
        fi
    else
        print_error "Silver layer processing failed"
        exit 1
    fi
}

# =============================================================================
# PIPELINE VALIDATION
# =============================================================================

validate_pipeline() {
    print_header "${MEDAL} PIPELINE VALIDATION"
    
    print_step "Running comprehensive pipeline validation..."
    
    # Check Bronze layer
    bronze_count=$(python -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
    print(result.fetchone()[0])
" 2>/dev/null || echo "0")
    
    # Check Silver layer
    silver_count=$(python -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
    print(result.fetchone()[0])
" 2>/dev/null || echo "0")
    
    print_info "Pipeline Status:"
    print_info "  Bronze Layer: $bronze_count records"
    print_info "  Silver Layer: $silver_count records"
    
    if [[ $bronze_count -gt 0 && $silver_count -gt 0 ]]; then
        print_success "Medallion architecture pipeline operational"
        return 0
    else
        print_error "Pipeline validation failed"
        return 1
    fi
}

# =============================================================================
# MAIN SETUP FUNCTIONS
# =============================================================================

full_setup() {
    print_header "${ROCKET} MEDALLION ARCHITECTURE COMPLETE SETUP"
    echo "🏥 Healthcare Data Pipeline - Bronze → Silver"
    echo "📅 $(date)"
    echo ""
    
    # Check if we're in the right directory
    if [[ ! -f "querybuilder.py" || ! -f "standardize_data.py" ]]; then
        print_error "Required files not found. Please run from the project directory."
        exit 1
    fi
    
    print_success "Project directory validated"
    
    # Setup environment
    activate_venv
    install_dependencies
    check_db_config
    
    # Setup Bronze and Silver layers
    setup_bronze_layer
    setup_silver_layer
    
    # Final validation
    if validate_pipeline; then
        print_header "${MEDAL} SETUP COMPLETE!"
        
        echo -e "${GREEN}"
        echo "╔══════════════════════════════════════════════╗"
        echo "║   Medallion Architecture Setup Successful!  ║"
        echo "╚══════════════════════════════════════════════╝"
        echo -e "${NC}"
        
        print_success "Virtual environment: $(pwd)/venv"
        print_success "Configuration: config_local.py"
        print_success "Bronze layer: Operational ($bronze_count records)"
        print_success "Silver layer: Operational ($silver_count records)"
        
        echo ""
        print_info "Next steps:"
        echo "  1. Review logs: standardize.log, querybuilder.log"
        echo "  2. Explore silver_analysis_queries.sql for analysis examples"
        echo "  3. Use the standardized data for medical research"
        echo ""
        print_info "To reactivate the environment later:"
        echo "  source venv/bin/activate"
        echo ""
    else
        print_error "Setup completed with validation issues"
        exit 1
    fi
    
    # Deactivate virtual environment
    deactivate 2>/dev/null || true
}

bronze_only_setup() {
    print_header "${DATABASE} BRONZE LAYER ONLY SETUP"
    
    if [[ ! -f "querybuilder.py" ]]; then
        print_error "querybuilder.py not found. Please run from the project directory."
        exit 1
    fi
    
    activate_venv
    install_dependencies
    check_db_config
    setup_bronze_layer
    
    print_success "Bronze layer setup completed"
    deactivate 2>/dev/null || true
}

silver_only_setup() {
    print_header "${GEAR} SILVER LAYER ONLY SETUP"
    
    if [[ ! -f "standardize_data.py" ]]; then
        print_error "standardize_data.py not found. Please run from the project directory."
        exit 1
    fi
    
    activate_venv
    install_dependencies
    check_db_config
    setup_silver_layer
    
    print_success "Silver layer setup completed"
    deactivate 2>/dev/null || true
}

quick_test() {
    print_header "${INFO} QUICK PIPELINE TEST"
    
    if [[ ! -f "config_local.py" ]]; then
        print_error "config_local.py not found. Run full setup first."
        exit 1
    fi
    
    if [[ ! -d "venv" ]]; then
        print_error "Virtual environment not found. Run full setup first."
        exit 1
    fi
    
    activate_venv
    
    print_step "Testing database connection..."
    python test_db.py
    
    if validate_pipeline; then
        print_success "Pipeline test successful"
    else
        print_error "Pipeline test failed"
        exit 1
    fi
    
    deactivate 2>/dev/null || true
}

show_help() {
    echo "Medallion Architecture Healthcare Data Pipeline Setup"
    echo ""
    echo "Usage:"
    echo "  ./setup.sh [option]"
    echo ""
    echo "Options:"
    echo "  full     Complete setup (Bronze + Silver) [DEFAULT]"
    echo "  bronze   Bronze layer only (raw data extraction)"
    echo "  silver   Silver layer only (data standardization)"
    echo "  test     Quick test of existing pipeline"
    echo "  help     Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./setup.sh           # Complete medallion architecture setup"
    echo "  ./setup.sh bronze    # Extract raw data only"
    echo "  ./setup.sh silver    # Standardize existing Bronze data"
    echo "  ./setup.sh test      # Test existing setup"
    echo ""
    echo "Pipeline Flow:"
    echo "  Bronze Layer  → Raw MIMIC-IV data extraction"
    echo "  Silver Layer  → OMOP standardization + quality validation"
    echo ""
}

# =============================================================================
# MAIN SCRIPT LOGIC
# =============================================================================

case "${1:-full}" in
    "full"|"")
        full_setup
        ;;
    "bronze")
        bronze_only_setup
        ;;
    "silver")
        silver_only_setup
        ;;
    "test")
        quick_test
        ;;
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        print_error "Unknown option: $1"
        echo "Use './setup.sh help' for usage information"
        exit 1
        ;;
esac
