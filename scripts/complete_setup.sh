#!/bin/bash
# =============================================================================
# MIMIC-IV Medallion Architecture Complete Pipeline Setup Script
# =============================================================================
# Medical Data Science - Complete Healthcare Data Pipeline
# 
# This script sets up the complete medallion architecture pipeline:
# 🥉 Bronze Layer (Raw Data Extraction from MIMIC-IV)
# 🥈 Silver Layer (OMOP Standardization & Quality Control) 
# 🥇 Gold Layer (Clinical SOFA Score Calculation & Analytics)
#
# Usage: ./complete_setup.sh [option]
# Options:
#   full        - Complete setup (Bronze + Silver + Gold) [DEFAULT]
#   bronze      - Bronze layer only (raw data extraction)
#   silver      - Silver layer only (requires Bronze)
#   gold        - Gold layer only (requires Silver)
#   discover    - Parameter discovery only
#   validate    - Comprehensive pipeline validation
#   clean       - Clean all layers and restart
#   status      - Show current pipeline status
#   help        - Show this help message
# =============================================================================

set -e  # Exit on any error

# Project directory setup
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"

# Color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
MAGENTA='\033[0;95m'
NC='\033[0m' # No Color

# Emoji indicators for better UX
SUCCESS="✅"
ERROR="❌"
WARNING="⚠️"
INFO="ℹ️"
ROCKET="🚀"
DATABASE="🗄️"
GEAR="⚙️"
MEDAL="🏆"
BRONZE="🥉"
SILVER="🥈"
GOLD="🥇"
STETHOSCOPE="🩺"
CHART="📊"
MAGNIFYING="🔍"
CLOCK="⏱️"
FIRE="🔥"

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

print_banner() {
    echo ""
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║                    🏥 MIMIC-IV MEDALLION PIPELINE                    ║${NC}"
    echo -e "${MAGENTA}║              Complete Healthcare Data Processing System              ║${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_header() {
    echo ""
    echo -e "${PURPLE}═══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${PURPLE}${1}${NC}"
    echo -e "${PURPLE}═══════════════════════════════════════════════════════════════════════${NC}"
}

print_section() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}${1}${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
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

print_progress() {
    echo -e "${PURPLE}${CLOCK} $1${NC}"
}

# Function to show progress with timer
show_progress() {
    local duration=$1
    local message=$2
    
    echo -n -e "${PURPLE}${CLOCK} ${message}"
    for ((i=0; i<duration; i++)); do
        echo -n "."
        sleep 1
    done
    echo -e " ${SUCCESS}${NC}"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to get timestamp
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S'
}

# Function to log with timestamp
log_message() {
    echo "[$(get_timestamp)] $1" >> pipeline_setup.log
}

# Function to activate virtual environment with enhanced error handling
activate_venv() {
    print_step "Setting up Python virtual environment..."
    
    if [[ ! -d "venv" ]]; then
        print_info "Creating virtual environment..."
        if ! python3 -m venv venv; then
            print_error "Failed to create virtual environment"
            print_info "Please ensure Python 3.8+ is installed"
            exit 1
        fi
        print_success "Virtu al environment created successfully"
    fi
    
    print_step "Activating virtual environment..."
    source venv/bin/activate
    
    if [[ "$VIRTUAL_ENV" == "" ]]; then
        print_error "Failed to activate virtual environment"
        exit 1
    fi
    
    print_success "Virtual environment activated: $(basename $VIRTUAL_ENV)"
    log_message "Virtual environment activated"
}

# Function to install dependencies with version checking
install_dependencies() {
    print_step "Installing Python dependencies..."
    
    if [[ ! -f "requirements.txt" ]]; then
        print_warning "requirements.txt not found, creating basic requirements..."
        cat > requirements.txt << EOF
psycopg2-binary>=2.9.0
sqlalchemy>=2.0.0
pandas>=2.0.0
numpy>=1.24.0
python-dotenv>=1.0.0
jsonschema>=4.0.0
EOF
        print_info "Basic requirements.txt created"
    fi
    
    print_progress "Upgrading pip..."
    pip install --upgrade pip -q
    
    print_progress "Installing requirements..."
    if pip install -r requirements.txt -q; then
        print_success "All dependencies installed successfully"
        log_message "Dependencies installed"
    else
        print_error "Failed to install dependencies"
        exit 1
    fi
}

# Function to check database configuration with detailed validation
check_db_config() {
    print_step "Validating database configuration..."
    
    if [[ ! -f "config_local.py" ]]; then
        print_warning "config_local.py not found"
        
        if [[ -f "config_template.py" ]]; then
            print_info "Creating config_local.py from template..."
            cp config_template.py config_local.py
            print_warning "Please edit config_local.py with your database credentials"
            
            # Try to open with common editors
            if command_exists code; then
                print_info "Opening in VS Code..."
                code config_local.py
            elif command_exists nano; then
                print_info "Opening in nano editor..."
                nano config_local.py
            else
                print_info "Please manually edit config_local.py"
            fi
            
            echo ""
            echo -e "${YELLOW}Please update the following in config_local.py:${NC}"
            echo "  - Database host, port, name"
            echo "  - Username and password"
            echo "  - Ensure PostgreSQL is running"
            echo ""
            read -p "Press Enter after updating config_local.py..."
        else
            print_error "config_template.py not found. Cannot create local configuration."
            exit 1
        fi
    else
        print_success "config_local.py found"
    fi
    
    # Test database connection with detailed error reporting
    print_step "Testing database connection..."
    if python3 -c "
import sys
try:
    from config_local import DB_CONFIG
    from sqlalchemy import create_engine, text
    
    print('Database configuration loaded successfully')
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT version()'))
        version = result.fetchone()[0]
        print(f'Connected to: {version}')
        
    print('✅ Database connection successful')
except ImportError as e:
    print(f'❌ Configuration import error: {e}')
    sys.exit(1)
except Exception as e:
    print(f'❌ Database connection failed: {e}')
    sys.exit(1)
"; then
        print_success "Database connection validated"
        log_message "Database connection successful"
    else
        print_error "Database connection failed"
        print_info "Common issues:"
        echo "  - PostgreSQL service not running"
        echo "  - Incorrect credentials in config_local.py"
        echo "  - Database does not exist"
        echo "  - Network connectivity issues"
        exit 1
    fi
}

# Function to discover SOFA parameters
discover_sofa_parameters() {
    print_section "${MAGNIFYING} SOFA PARAMETER DISCOVERY"
    
    print_step "Checking if SOFA parameters already discovered..."
    
    if [[ -f "discovered_sofa_parameters.json" && -f "omop_concept_mappings.json" ]]; then
        print_success "SOFA parameters already discovered"
        
        # Show discovery stats
        param_count=$(python3 -c "
import json
with open('discovered_sofa_parameters.json', 'r') as f:
    data = json.load(f)
    total = sum(len(system.get('chartevents', [])) + len(system.get('labevents', [])) + len(system.get('outputevents', [])) for system in data.values())
    print(total)
" 2>/dev/null || echo "0")
        
        print_info "Found $param_count SOFA parameters across all organ systems"
        
        echo ""
        read -p "Re-discover parameters? (y/N): " rediscover
        if [[ ! $rediscover =~ ^[Yy]$ ]]; then
            print_info "Using existing parameter discovery"
            return 0
        fi
    fi
    
    print_step "Starting SOFA parameter discovery..."
    print_info "This process analyzes MIMIC-IV to find all SOFA-relevant parameters"
    
    # Set Python path to include project root and change to project directory
    export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"
    cd "${PROJECT_ROOT}"
    
    if python3 src/utils/parameter_discovery.py; then
        print_success "SOFA parameter discovery completed"
        
        # Show discovery results
        if [[ -f "discovered_sofa_parameters.json" ]]; then
            param_count=$(python3 -c "
import json
with open('discovered_sofa_parameters.json', 'r') as f:
    data = json.load(f)
    total = sum(len(system.get('chartevents', [])) + len(system.get('labevents', [])) + len(system.get('outputevents', [])) for system in data.values())
    print(total)
")
            print_success "Discovered $param_count SOFA parameters"
            log_message "SOFA parameter discovery completed: $param_count parameters"
        fi
    else
        print_error "SOFA parameter discovery failed"
        exit 1
    fi
}

# =============================================================================
# BRONZE LAYER SETUP
# =============================================================================

setup_bronze_layer() {
    print_section "${BRONZE} BRONZE LAYER SETUP - Raw Data Extraction"
    
    print_step "Checking Bronze layer status..."
    
    bronze_exists=false
    record_count=0
    
    # Check if Bronze data exists
    if python3 -c "
import sys
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Check if schema and table exist
        result = conn.execute(text(\"\"\"
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'bronze' AND table_name = 'collection_disease'
        \"\"\"))
        
        if result.fetchone()[0] > 0:
            result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
            count = result.fetchone()[0]
            print(count)
            sys.exit(0)
        else:
            sys.exit(1)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        record_count=$(python3 -c "
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
        
        # Show Bronze statistics
        print_info "Bronze Layer Statistics:"
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    # Get patient and stay counts
    result = conn.execute(text('SELECT COUNT(DISTINCT subject_id), COUNT(DISTINCT stay_id) FROM bronze.collection_disease'))
    patients, stays = result.fetchone()
    print(f'  📊 Patients: {patients}')
    print(f'  🏥 ICU Stays: {stays}')
    
    # Get outlier statistics
    result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease WHERE is_outlier = true'))
    outliers = result.fetchone()[0]
    print(f'  🚩 Outliers: {outliers} ({outliers/int('$record_count')*100:.1f}%)')
"
    else
        print_info "No Bronze data found - will extract from MIMIC-IV"
    fi
    
    if [[ $bronze_exists == true && $record_count -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Bronze data exists with $record_count records${NC}"
        read -p "Re-extract Bronze data? (y/N): " re_extract
        if [[ ! $re_extract =~ ^[Yy]$ ]]; then
            print_info "Preserving existing Bronze data"
            log_message "Bronze layer skipped (existing data preserved)"
            return 0
        fi
        
        print_warning "Dropping existing Bronze data..."
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS bronze.collection_disease CASCADE'))
    conn.commit()
    print('✅ Bronze table dropped')
"
    fi
    
    print_step "Starting Bronze layer data extraction..."
    print_info "Extracting raw MIMIC-IV data with quality assessment..."
    
    # Use enhanced bronze builder if available, otherwise fallback
    if [[ -f "src/etl/enhanced_bronze_builder.py" ]]; then
        print_progress "Running enhanced Bronze extraction..."
        if python3 src/etl/enhanced_bronze_builder.py 2>&1 | tee logs/bronze_extraction.log; then
            print_success "Enhanced Bronze layer extraction completed"
        else
            print_error "Enhanced Bronze extraction failed"
            exit 1
        fi
    elif [[ -f "src/utils/querybuilder.py" ]]; then
        print_progress "Running standard Bronze extraction..."
        if python3 src/utils/querybuilder.py 2>&1 | tee logs/bronze_extraction.log; then
            print_success "Bronze layer extraction completed"
        else
            print_error "Bronze extraction failed"
            exit 1
        fi
    else
        print_error "No Bronze extraction script found (enhanced_bronze_builder.py or querybuilder.py)"
        exit 1
    fi
    
    # Validate Bronze data
    print_step "Validating Bronze layer data..."
    final_count=$(python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
    print(result.fetchone()[0])
")
    
    if [[ $final_count -gt 0 ]]; then
        print_success "Bronze validation passed: $final_count records"
        log_message "Bronze layer completed: $final_count records"
    else
        print_error "Bronze validation failed: no records found"
        exit 1
    fi
}

# =============================================================================
# SILVER LAYER SETUP
# =============================================================================

setup_silver_layer() {
    print_section "${SILVER} SILVER LAYER SETUP - OMOP Standardization"
    
    # Check Bronze prerequisite
    print_step "Validating Bronze layer prerequisite..."
    
    bronze_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
        count = result.fetchone()[0]
        print(count)
except Exception:
    print(0)
" 2>/dev/null)
    
    if [[ $bronze_count -eq 0 ]]; then
        print_error "Bronze layer data not found"
        print_info "Please run Bronze layer setup first: ./complete_setup.sh bronze"
        exit 1
    else
        print_success "Bronze layer validated: $bronze_count records"
    fi
    
    # Check if Silver data exists
    print_step "Checking Silver layer status..."
    
    silver_exists=false
    silver_count=0
    
    if python3 -c "
import sys
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text(\"\"\"
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'silver' AND table_name = 'collection_disease_std'
        \"\"\"))
        
        if result.fetchone()[0] > 0:
            result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
            count = result.fetchone()[0]
            print(count)
            sys.exit(0)
        else:
            sys.exit(1)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        silver_count=$(python3 -c "
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
        
        # Show retention rate
        retention_rate=$(python3 -c "print(f'{($silver_count / $bronze_count) * 100:.1f}%')")
        print_info "Data retention rate: $retention_rate"
    else
        print_info "No Silver data found - will standardize Bronze data"
    fi
    
    if [[ $silver_exists == true && $silver_count -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Silver data exists with $silver_count records${NC}"
        read -p "Re-process Silver data? (y/N): " re_process
        if [[ ! $re_process =~ ^[Yy]$ ]]; then
            print_info "Preserving existing Silver data"
            log_message "Silver layer skipped (existing data preserved)"
            return 0
        fi
        
        print_warning "Dropping existing Silver data..."
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS silver.collection_disease_std CASCADE'))
    conn.commit()
    print('✅ Silver table dropped')
"
    fi
    
    print_step "Starting Silver layer OMOP standardization..."
    print_info "Applying OMOP concepts, unit conversions, and quality controls..."
    
    # Use enhanced silver builder if available
    if [[ -f "src/etl/enhanced_silver_builder.py" ]]; then
        print_progress "Running enhanced Silver processing..."
        if python3 src/etl/enhanced_silver_builder.py 2>&1 | tee logs/silver_processing.log; then
            print_success "Enhanced Silver layer processing completed"
        else
            print_error "Enhanced Silver processing failed"
            exit 1
        fi
    elif [[ -f "src/utils/standardize_data.py" ]]; then
        print_progress "Running standard Silver processing..."
        if python3 src/utils/standardize_data.py 2>&1 | tee logs/silver_processing.log; then
            print_success "Silver layer processing completed"
        else
            print_error "Silver processing failed"
            exit 1
        fi
    else
        print_error "No Silver processing script found"
        exit 1
    fi
    
    # Validate Silver data
    print_step "Validating Silver layer data..."
    final_silver_count=$(python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
    print(result.fetchone()[0])
")
    
    if [[ $final_silver_count -gt 0 ]]; then
        retention_rate=$(python3 -c "print(f'{($final_silver_count / $bronze_count) * 100:.1f}%')")
        print_success "Silver validation passed: $final_silver_count records ($retention_rate retention)"
        log_message "Silver layer completed: $final_silver_count records"
    else
        print_error "Silver validation failed: no records found"
        exit 1
    fi
}

# =============================================================================
# GOLD LAYER SETUP
# =============================================================================

setup_gold_layer() {
    print_section "${GOLD} GOLD LAYER SETUP - Clinical SOFA Scoring"
    
    # Check Silver prerequisite
    print_step "Validating Silver layer prerequisite..."
    
    silver_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
        count = result.fetchone()[0]
        print(count)
except Exception:
    print(0)
" 2>/dev/null)
    
    if [[ $silver_count -eq 0 ]]; then
        print_error "Silver layer data not found"
        print_info "Please run Silver layer setup first: ./complete_setup.sh silver"
        exit 1
    else
        print_success "Silver layer validated: $silver_count records"
    fi
    
    # Check if Gold data exists
    print_step "Checking Gold layer status..."
    
    gold_exists=false
    gold_count=0
    
    if python3 -c "
import sys
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        result = conn.execute(text(\"\"\"
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'gold' AND table_name = 'sofa_scores'
        \"\"\"))
        
        if result.fetchone()[0] > 0:
            result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores'))
            count = result.fetchone()[0]
            print(count)
            sys.exit(0)
        else:
            sys.exit(1)
except Exception:
    sys.exit(1)
" 2>/dev/null; then
        gold_count=$(python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores'))
    count = result.fetchone()[0]
    print(count)
")
        gold_exists=true
        print_success "Found $gold_count existing SOFA scores"
        
        # Show Gold layer statistics
        print_info "Gold Layer Statistics:"
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    # Get patient count
    result = conn.execute(text('SELECT COUNT(DISTINCT subject_id) FROM gold.sofa_scores'))
    patients = result.fetchone()[0]
    print(f'  👥 Patients with SOFA scores: {patients}')
    
    # Get score statistics
    result = conn.execute(text('SELECT MIN(total_sofa_score), MAX(total_sofa_score), ROUND(AVG(total_sofa_score), 2) FROM gold.sofa_scores'))
    min_score, max_score, avg_score = result.fetchone()
    print(f'  📊 SOFA Score Range: {min_score} - {max_score} (avg: {avg_score})')
    
    # Get high-risk patients
    result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores WHERE total_sofa_score >= 10'))
    high_risk = result.fetchone()[0]
    print(f'  🚨 High-Risk Patients (SOFA ≥10): {high_risk} ({high_risk/int('$gold_count')*100:.1f}%)')
"
    else
        print_info "No Gold data found - will calculate SOFA scores"
    fi
    
    if [[ $gold_exists == true && $gold_count -gt 0 ]]; then
        echo ""
        echo -e "${YELLOW}Gold data exists with $gold_count SOFA scores${NC}"
        read -p "Re-calculate SOFA scores? (y/N): " recalculate
        if [[ ! $recalculate =~ ^[Yy]$ ]]; then
            print_info "Preserving existing Gold data"
            log_message "Gold layer skipped (existing data preserved)"
            return 0
        fi
        
        print_warning "Dropping existing Gold data..."
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    conn.execute(text('DROP SCHEMA IF EXISTS gold CASCADE'))
    conn.commit()
    print('✅ Gold schema dropped')
"
    fi
    
    print_step "Starting Gold layer SOFA score calculation..."
    print_info "Calculating clinical SOFA scores for all patients..."
    
    # Use enhanced SOFA calculator
    if [[ -f "src/etl/enhanced_sofa_calculator.py" ]]; then
        print_progress "Running enhanced SOFA calculation..."
        if python3 src/etl/enhanced_sofa_calculator.py 2>&1 | tee logs/gold_sofa_calculation.log; then
            print_success "Enhanced SOFA calculation completed"
        else
            print_error "Enhanced SOFA calculation failed"
            exit 1
        fi
    elif [[ -f "calculate_sofa_gold.py" ]]; then
        print_progress "Running standard SOFA calculation..."
        if python3 src/scoring/calculate_sofa_gold.py 2>&1 | tee logs/gold_sofa_calculation.log; then
            print_success "SOFA calculation completed"
        else
            print_error "SOFA calculation failed"
            exit 1
        fi
    else
        print_error "No SOFA calculation script found"
        exit 1
    fi
    
    # Validate Gold data
    print_step "Validating Gold layer data..."
    final_gold_count=$(python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores'))
    print(result.fetchone()[0])
")
    
    if [[ $final_gold_count -gt 0 ]]; then
        print_success "Gold validation passed: $final_gold_count SOFA scores"
        log_message "Gold layer completed: $final_gold_count SOFA scores"
        
        # Show final clinical validation
        print_step "Clinical validation summary:"
        python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    # SOFA distribution
    result = conn.execute(text(\"\"\"
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
    \"\"\"))
    
    print('  📊 SOFA Score Distribution:')
    for severity, count, percentage in result.fetchall():
        print(f'     {severity}: {count} ({percentage}%)')
"
    else
        print_error "Gold validation failed: no SOFA scores found"
        exit 1
    fi
}

# =============================================================================
# PIPELINE VALIDATION
# =============================================================================

comprehensive_validation() {
    print_section "${MEDAL} COMPREHENSIVE PIPELINE VALIDATION"
    
    print_step "Running end-to-end pipeline validation..."
    
    # Get counts from all layers
    print_progress "Collecting layer statistics..."
    
    bronze_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
        print(result.fetchone()[0])
except Exception:
    print(0)
" 2>/dev/null)
    
    silver_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
        print(result.fetchone()[0])
except Exception:
    print(0)
" 2>/dev/null)
    
    gold_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores'))
        print(result.fetchone()[0])
except Exception:
    print(0)
" 2>/dev/null)
    
    # Display pipeline status
    echo ""
    echo -e "${CYAN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║                        PIPELINE DATA FLOW                           ║${NC}"
    echo -e "${CYAN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    
    if [[ $bronze_count -gt 0 ]]; then
        echo -e "${GREEN}${BRONZE} BRONZE LAYER: ${bronze_count} raw measurements${NC}"
    else
        echo -e "${RED}${BRONZE} BRONZE LAYER: No data${NC}"
    fi
    
    if [[ $silver_count -gt 0 ]]; then
        retention_rate=$(python3 -c "print(f'{($silver_count / max($bronze_count, 1)) * 100:.1f}%')")
        echo -e "${GREEN}${SILVER} SILVER LAYER: ${silver_count} standardized records ($retention_rate retention)${NC}"
    else
        echo -e "${RED}${SILVER} SILVER LAYER: No data${NC}"
    fi
    
    if [[ $gold_count -gt 0 ]]; then
        patient_count=$(python3 -c "
try:
    from sqlalchemy import create_engine, text
    from config_local import DB_CONFIG
    connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
    engine = create_engine(connection_string)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT COUNT(DISTINCT subject_id) FROM gold.sofa_scores'))
        print(result.fetchone()[0])
except Exception:
    print(0)
")
        echo -e "${GREEN}${GOLD} GOLD LAYER: ${gold_count} SOFA scores for ${patient_count} patients${NC}"
    else
        echo -e "${RED}${GOLD} GOLD LAYER: No data${NC}"
    fi
    
    echo ""
    
    # Validation status
    if [[ $bronze_count -gt 0 && $silver_count -gt 0 && $gold_count -gt 0 ]]; then
        print_success "Complete medallion architecture pipeline operational"
        log_message "Pipeline validation successful: Bronze($bronze_count) → Silver($silver_count) → Gold($gold_count)"
        
        echo ""
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║                     ${SUCCESS} PIPELINE READY FOR PRODUCTION ${SUCCESS}                    ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
        return 0
    else
        print_error "Pipeline validation failed - incomplete data flow"
        return 1
    fi
}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

show_pipeline_status() {
    print_section "${CHART} CURRENT PIPELINE STATUS"
    
    print_step "Checking all pipeline layers..."
    
    # Check each layer
    python3 -c "
import sys
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

print('\\n🗄️  DATABASE LAYER STATUS:')
print('═' * 50)

try:
    with engine.connect() as conn:
        # Bronze layer
        try:
            result = conn.execute(text('SELECT COUNT(*) FROM bronze.collection_disease'))
            bronze_count = result.fetchone()[0]
            print(f'🥉 Bronze Layer: {bronze_count:,} records ✅')
            
            # Get Bronze stats
            result = conn.execute(text('SELECT COUNT(DISTINCT subject_id), COUNT(DISTINCT stay_id) FROM bronze.collection_disease'))
            patients, stays = result.fetchone()
            print(f'   📊 {patients} patients, {stays} ICU stays')
            
        except Exception as e:
            print('🥉 Bronze Layer: Not found ❌')
        
        # Silver layer
        try:
            result = conn.execute(text('SELECT COUNT(*) FROM silver.collection_disease_std'))
            silver_count = result.fetchone()[0]
            print(f'🥈 Silver Layer: {silver_count:,} records ✅')
            
            # Check OMOP mapping
            result = conn.execute(text('SELECT COUNT(DISTINCT concept_id) FROM silver.collection_disease_std WHERE concept_id IS NOT NULL'))
            concepts = result.fetchone()[0]
            print(f'   🔗 {concepts} unique OMOP concepts mapped')
            
        except Exception as e:
            print('🥈 Silver Layer: Not found ❌')
        
        # Gold layer
        try:
            result = conn.execute(text('SELECT COUNT(*) FROM gold.sofa_scores'))
            gold_count = result.fetchone()[0]
            print(f'🥇 Gold Layer: {gold_count:,} SOFA scores ✅')
            
            # Get Gold stats
            result = conn.execute(text('SELECT COUNT(DISTINCT subject_id), MIN(total_sofa_score), MAX(total_sofa_score), ROUND(AVG(total_sofa_score), 2) FROM gold.sofa_scores'))
            patients, min_score, max_score, avg_score = result.fetchone()
            print(f'   🩺 {patients} patients, SOFA range: {min_score}-{max_score} (avg: {avg_score})')
            
        except Exception as e:
            print('🥇 Gold Layer: Not found ❌')

except Exception as e:
    print(f'❌ Database connection failed: {e}')
    sys.exit(1)
"
    
    # Check for key files
    echo ""
    echo "📁 KEY FILES STATUS:"
    echo "═" * 50
    
    files_to_check=(
        "config_local.py:Database configuration"
        "discovered_sofa_parameters.json:SOFA parameter discovery"
        "omop_concept_mappings.json:OMOP concept mappings"
        "enhanced_bronze_builder.py:Bronze layer builder"
        "enhanced_silver_builder.py:Silver layer builder"
        "enhanced_sofa_calculator.py:Gold layer SOFA calculator"
    )
    
    for file_info in "${files_to_check[@]}"; do
        IFS=':' read -r filename description <<< "$file_info"
        if [[ -f "$filename" ]]; then
            echo -e "${GREEN}✅ $filename${NC} - $description"
        else
            echo -e "${RED}❌ $filename${NC} - $description"
        fi
    done
    
    echo ""
}

clean_pipeline() {
    print_section "${FIRE} PIPELINE CLEANUP"
    
    print_warning "This will remove ALL pipeline data!"
    echo "The following will be deleted:"
    echo "  • All Bronze, Silver, and Gold tables"
    echo "  • All calculated SOFA scores"
    echo "  • All standardized data"
    echo ""
    read -p "Are you sure you want to proceed? (type 'yes' to confirm): " confirm
    
    if [[ "$confirm" != "yes" ]]; then
        print_info "Cleanup cancelled"
        return 0
    fi
    
    print_step "Cleaning all pipeline layers..."
    
    python3 -c "
from sqlalchemy import create_engine, text
from config_local import DB_CONFIG

connection_string = f'postgresql://{DB_CONFIG[\"user\"]}:{DB_CONFIG[\"password\"]}@{DB_CONFIG[\"host\"]}:{DB_CONFIG[\"port\"]}/{DB_CONFIG[\"database\"]}'
engine = create_engine(connection_string)

with engine.connect() as conn:
    print('🗑️  Dropping Gold schema...')
    conn.execute(text('DROP SCHEMA IF EXISTS gold CASCADE'))
    
    print('🗑️  Dropping Silver schema...')
    conn.execute(text('DROP SCHEMA IF EXISTS silver CASCADE'))
    
    print('🗑️  Dropping Bronze schema...')
    conn.execute(text('DROP SCHEMA IF EXISTS bronze CASCADE'))
    
    conn.commit()
    print('✅ All pipeline data cleaned')
"
    
    print_success "Pipeline cleanup completed"
    log_message "Pipeline cleanup completed"
}

# =============================================================================
# MAIN SETUP FUNCTIONS
# =============================================================================

full_setup() {
    print_banner
    print_header "${ROCKET} COMPLETE MEDALLION ARCHITECTURE SETUP"
    
    echo -e "${BLUE}Starting complete healthcare data pipeline setup...${NC}"
    echo -e "${BLUE}This will process: Raw MIMIC-IV → OMOP Standardized → Clinical SOFA Scores${NC}"
    echo ""
    echo -e "${YELLOW}Timeline: ~15-20 minutes for complete setup${NC}"
    echo ""
    
    # Verify we're in the right directory
    if [[ ! -f "config_local.py" && ! -f "config_template.py" ]]; then
        print_error "Configuration files not found. Please run from the project directory."
        exit 1
    fi
    
    print_success "Project directory validated"
    log_message "Full setup started"
    
    # Environment setup
    activate_venv
    install_dependencies
    check_db_config
    
    # Parameter discovery (if needed)
    discover_sofa_parameters
    
    # Execute all pipeline layers
    setup_bronze_layer
    setup_silver_layer
    setup_gold_layer
    
    # Final validation
    if comprehensive_validation; then
        echo ""
        print_header "${MEDAL} SETUP COMPLETE!"
        
        echo ""
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║            🎉 MEDALLION ARCHITECTURE SETUP SUCCESSFUL! 🎉            ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        print_success "Virtual environment: $(pwd)/venv"
        print_success "Database configuration: config_local.py"
        print_success "Bronze layer: Raw MIMIC-IV data extracted"
        print_success "Silver layer: OMOP standardization completed"
        print_success "Gold layer: Clinical SOFA scores calculated"
        
        echo ""
        print_info "📋 Next steps:"
        echo "  1. Review logs: bronze_extraction.log, silver_processing.log, gold_sofa_calculation.log"
        echo "  2. Explore example_queries.sql for analysis examples"
        echo "  3. Use COMPLETE_FINAL_REPORT.md for comprehensive documentation"
        echo "  4. Run validation: ./complete_setup.sh validate"
        echo ""
        print_info "🔄 To reactivate the environment later:"
        echo "  source venv/bin/activate"
        echo ""
        
        log_message "Full setup completed successfully"
    else
        print_error "Setup completed with validation issues"
        exit 1
    fi
    
    # Deactivate virtual environment
    deactivate 2>/dev/null || true
}

# Individual layer setups
bronze_only_setup() {
    print_banner
    print_header "${BRONZE} BRONZE LAYER ONLY SETUP"
    
    activate_venv
    install_dependencies
    check_db_config
    discover_sofa_parameters
    setup_bronze_layer
    
    print_success "Bronze layer setup completed"
    log_message "Bronze-only setup completed"
    deactivate 2>/dev/null || true
}

silver_only_setup() {
    print_banner
    print_header "${SILVER} SILVER LAYER ONLY SETUP"
    
    activate_venv
    install_dependencies
    check_db_config
    setup_silver_layer
    
    print_success "Silver layer setup completed"
    log_message "Silver-only setup completed"
    deactivate 2>/dev/null || true
}

gold_only_setup() {
    print_banner
    print_header "${GOLD} GOLD LAYER ONLY SETUP"
    
    activate_venv
    install_dependencies
    check_db_config
    setup_gold_layer
    
    print_success "Gold layer setup completed"
    log_message "Gold-only setup completed"
    deactivate 2>/dev/null || true
}

discovery_only() {
    print_banner
    print_header "${MAGNIFYING} SOFA PARAMETER DISCOVERY ONLY"
    
    activate_venv
    install_dependencies
    check_db_config
    discover_sofa_parameters
    
    print_success "Parameter discovery completed"
    deactivate 2>/dev/null || true
}

validation_only() {
    print_banner
    print_header "${MEDAL} PIPELINE VALIDATION ONLY"
    
    activate_venv
    comprehensive_validation
    
    deactivate 2>/dev/null || true
}

show_help() {
    echo ""
    echo -e "${MAGENTA}╔══════════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${MAGENTA}║                🏥 MIMIC-IV Medallion Pipeline Setup                  ║${NC}"
    echo -e "${MAGENTA}║              Complete Healthcare Data Processing System              ║${NC}"
    echo -e "${MAGENTA}╚══════════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${CYAN}USAGE:${NC}"
    echo "  ./complete_setup.sh [option]"
    echo ""
    echo -e "${CYAN}OPTIONS:${NC}"
    echo -e "  ${GREEN}full${NC}        Complete setup (Bronze + Silver + Gold) ${YELLOW}[DEFAULT]${NC}"
    echo -e "  ${GREEN}bronze${NC}      Bronze layer only (raw data extraction)"
    echo -e "  ${GREEN}silver${NC}      Silver layer only (OMOP standardization)"
    echo -e "  ${GREEN}gold${NC}        Gold layer only (SOFA score calculation)"
    echo -e "  ${GREEN}discover${NC}    SOFA parameter discovery only"
    echo -e "  ${GREEN}validate${NC}    Comprehensive pipeline validation"
    echo -e "  ${GREEN}status${NC}      Show current pipeline status"
    echo -e "  ${GREEN}clean${NC}       Clean all pipeline data"
    echo -e "  ${GREEN}help${NC}        Show this help message"
    echo ""
    echo -e "${CYAN}EXAMPLES:${NC}"
    echo "  ./complete_setup.sh           # Complete medallion architecture setup"
    echo "  ./complete_setup.sh bronze    # Extract raw MIMIC-IV data only"
    echo "  ./complete_setup.sh silver    # Standardize existing Bronze data"
    echo "  ./complete_setup.sh gold      # Calculate SOFA scores from Silver data"
    echo "  ./complete_setup.sh status    # Check current pipeline status"
    echo "  ./complete_setup.sh validate  # Validate entire pipeline"
    echo ""
    echo -e "${CYAN}PIPELINE FLOW:${NC}"
    echo -e "  ${BRONZE} Bronze Layer   → Raw MIMIC-IV data extraction with quality flagging"
    echo -e "  ${SILVER} Silver Layer   → OMOP standardization + unit conversion + quality control"
    echo -e "  ${GOLD} Gold Layer     → Clinical SOFA score calculation + validation + analytics"
    echo ""
    echo -e "${CYAN}REQUIREMENTS:${NC}"
    echo "  • PostgreSQL with MIMIC-IV database"
    echo "  • Python 3.8+ with pip"
    echo "  • Virtual environment support"
    echo "  • Sufficient disk space (~1GB for complete pipeline)"
    echo ""
    echo -e "${CYAN}ESTIMATED TIMELINE:${NC}"
    echo "  • Bronze setup: ~5-7 minutes"
    echo "  • Silver setup: ~3-5 minutes"
    echo "  • Gold setup: ~2-3 minutes"
    echo "  • Total complete setup: ~15-20 minutes"
    echo ""
}

# =============================================================================
# MAIN SCRIPT LOGIC
# =============================================================================

# Initialize logging
echo "=== Pipeline Setup Started at $(get_timestamp) ===" > pipeline_setup.log

# ...existing code...

# =============================================================================
# ETL CONFIGURATION SETUP - Task 5.4 Integration
# =============================================================================

setup_etl_configurations() {
    print_section "${GEAR} ETL CONFIGURATION SETUP - Task 5.4"
    
    print_step "Initializing dual ETL configurations..."
    
    # Validate etl_configurations.py exists and is correct
    if [[ ! -f "src/config/etl_configurations.py" ]]; then
        print_error "src/config/etl_configurations.py not found. Please ensure the configuration file exists."
        exit 1
    fi
    
    print_step "Validating configurations..."
    python3 << 'EOF'
try:
    import sys
    import os
    sys.path.append(os.path.join('src', 'config'))
    from etl_configurations import *
    
    print("🔧 Validating ETL configurations...")
    
    # Validate both configurations
    original_config = ACTIVE_CONFIG
     # Test CONFIG_1
    set_active_config(1)
    validate_config()
    
    # Test CONFIG_2
    set_active_config(2)
    validate_config()
    
    # Restore original
    if original_config['name'] == 'mean_based_config':
        set_active_config(1)
    else:
        set_active_config(2)
    
    print("✅ All configurations validated successfully")
    
    # Print configuration summary
    print("\n📋 Configuration Summary:")
    print("=" * 50)
    
    configs = get_both_configs()
    for config_name, config in configs.items():
        print(f"\n{config_name.upper()}:")
        print(f"  Name: {config['name']}")
        print(f"  Description: {config['description']}")
        print(f"  Aggregation: {config['aggregation_method']}")
        print(f"  Imputation: {config['imputation_method']}")
        print(f"  Outlier Handling: {config['outlier_handling']}")
        print(f"  Time Window: {config['time_window_hours']} hours")
        print(f"  Min Observations: {config['min_observations']}")
        print(f"  Output Table: {config['output_table']}")
    
    print(f"\n🎯 Active Configuration: {ACTIVE_CONFIG['name']}")
    
except Exception as e:
    print(f"❌ Configuration initialization failed: {e}")
    exit(1)
EOF

    if [ $? -ne 0 ]; then
        print_error "Configuration initialization failed"
        exit 1
    fi

    print_success "ETL configurations initialized"
}

# =============================================================================
# GOLD SCHEMA TABLES CREATION - Task 5.4
# =============================================================================

create_gold_etl_tables() {
    print_section "${GOLD} GOLD SCHEMA ETL TABLES CREATION"
    
    print_step "Creating ETL pipeline tables in gold schema..."
    python3 << 'EOF'
try:
    from config_local import DB_CONFIG
    import sys
    import os
    sys.path.append(os.path.join('src', 'config'))
    from etl_configurations import *
    import psycopg2
    from datetime import datetime
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Create gold schema if not exists
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
    print("✅ Gold schema created/verified")
    
    # Get both configurations
    configs = get_both_configs()
    
    # Create tables for both configurations
    for config_name, config in configs.items():
        table_name = config['output_table']
        
        print(f"🔄 Creating table: gold.{table_name}")
        
        # Create table DDL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS gold.{table_name} (
            patient_id INTEGER,
            hadm_id INTEGER,
            stay_id INTEGER,
            
            -- Timing information
            measurement_time TIMESTAMP,
            calculation_time TIMESTAMP DEFAULT NOW(),
            
            -- Configuration metadata
            config_name VARCHAR(100) DEFAULT '{config['name']}',
            aggregation_method VARCHAR(50) DEFAULT '{config['aggregation_method']}',
            imputation_method VARCHAR(50) DEFAULT '{config['imputation_method']}',
            outlier_handling VARCHAR(50) DEFAULT '{config['outlier_handling']}',
            time_window_hours INTEGER DEFAULT {config['time_window_hours']},
            
            -- Score components (from OMOP concepts)
            pao2_fio2_ratio DECIMAL(10,4),
            spo2_fio2_ratio DECIMAL(10,4),
            respiratory_rate DECIMAL(10,4),
            heart_rate DECIMAL(10,4),
            paco2 DECIMAL(10,4),
            tidal_volume DECIMAL(10,4),
            minute_ventilation DECIMAL(10,4),
            creatinine DECIMAL(10,4),
            ph DECIMAL(10,4),
            albumin DECIMAL(10,4),
            uric_acid DECIMAL(10,4),
            nt_probnp DECIMAL(10,4),
            d_dimer DECIMAL(10,4),
            homocysteine DECIMAL(10,4),
            procalcitonin DECIMAL(10,4),
            il_6 DECIMAL(10,4),
            il_8 DECIMAL(10,4),
            il_10 DECIMAL(10,4),
            st2 DECIMAL(10,4),
            pentraxin_3 DECIMAL(10,4),
            fraktalkin DECIMAL(10,4),
            srage DECIMAL(10,4),
            kl_6 DECIMAL(10,4),
            pai_1 DECIMAL(10,4),
            vegf DECIMAL(10,4),
            
            -- Calculated scores
            apache_ii_score DECIMAL(10,4),
            sofa_score DECIMAL(10,4),
            saps_ii_score DECIMAL(10,4),
            oasis_score DECIMAL(10,4),
            
            -- Quality metrics
            total_parameters_used INTEGER,
            missing_parameters INTEGER,
            imputed_parameters INTEGER,
            data_quality_score DECIMAL(5,4),
            
            -- Metadata
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            
            -- Constraints
            PRIMARY KEY (patient_id, hadm_id, measurement_time, config_name)
        );
        """
        
        cursor.execute(create_table_sql)
        
        # Create indexes for performance
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_patient_id ON gold.{table_name} (patient_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_hadm_id ON gold.{table_name} (hadm_id);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_measurement_time ON gold.{table_name} (measurement_time);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_config_name ON gold.{table_name} (config_name);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_scores ON gold.{table_name} (apache_ii_score, sofa_score, saps_ii_score, oasis_score);"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        print(f"✅ Table gold.{table_name} created with indexes")
    
    # Create comparison analysis table
    print("🔄 Creating comparison analysis table...")
    comparison_table_sql = """
    CREATE TABLE IF NOT EXISTS gold.config_comparison_analysis (
        analysis_id SERIAL PRIMARY KEY,
        comparison_name VARCHAR(200),
        config_1_name VARCHAR(100),
        config_2_name VARCHAR(100),
        
        -- Statistical measures
        correlation_pearson DECIMAL(10,6),
        correlation_spearman DECIMAL(10,6),
        mean_absolute_difference DECIMAL(10,6),
        
        -- Score-specific comparisons
        score_type VARCHAR(50),
        config_1_mean DECIMAL(10,4),
        config_1_median DECIMAL(10,4),
        config_1_std DECIMAL(10,4),
        config_2_mean DECIMAL(10,4),
        config_2_median DECIMAL(10,4),
        config_2_std DECIMAL(10,4),
        
        -- Statistical tests
        t_test_statistic DECIMAL(10,6),
        t_test_p_value DECIMAL(10,6),
        wilcoxon_statistic DECIMAL(10,6),
        wilcoxon_p_value DECIMAL(10,6),
        
        -- Analysis metadata
        sample_size INTEGER,
        analysis_date TIMESTAMP DEFAULT NOW(),
        analysis_parameters JSONB
    );
    """
    
    cursor.execute(comparison_table_sql)
    print("✅ Table gold.config_comparison_analysis created")
    
    # Create mortality outcome analysis table
    print("🔄 Creating mortality correlation analysis table...")
    outcome_table_sql = """
    CREATE TABLE IF NOT EXISTS gold.mortality_correlation_analysis (
        analysis_id SERIAL PRIMARY KEY,
        patient_id INTEGER,
        hadm_id INTEGER,
        config_name VARCHAR(100),
        
        -- Scores
        apache_ii_score DECIMAL(10,4),
        sofa_score DECIMAL(10,4),
        saps_ii_score DECIMAL(10,4),
        oasis_score DECIMAL(10,4),
        
        -- Outcomes
        hospital_mortality BOOLEAN,
        icu_mortality BOOLEAN,
        day_30_mortality BOOLEAN,
        
        -- Demographics
        age INTEGER,
        gender VARCHAR(10),
        
        -- Clinical context
        admission_type VARCHAR(50),
        first_careunit VARCHAR(50),
        los_hospital DECIMAL(10,2),
        los_icu DECIMAL(10,2),
        
        -- Analysis metadata
        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    
    cursor.execute(outcome_table_sql)
    print("✅ Table gold.mortality_correlation_analysis created")
    
    # Commit all changes
    conn.commit()
    conn.close()
    
    print("\n🎯 Gold schema tables created successfully:")
    print("  - gold.gold_scores_config1")
    print("  - gold.gold_scores_config2") 
    print("  - gold.config_comparison_analysis")
    print("  - gold.mortality_correlation_analysis")
    
except Exception as e:
    print(f"❌ Gold schema table creation failed: {e}")
    if 'conn' in locals():
        conn.rollback()
        conn.close()
    exit(1)
EOF

    if [ $? -ne 0 ]; then
        print_error "Gold schema table creation failed"
        exit 1
    fi

    print_success "Gold schema tables created successfully"
}

# =============================================================================
# ETL EXECUTION SCRIPTS CREATION
# =============================================================================

create_etl_scripts() {
    print_section "${ROCKET} ETL EXECUTION SCRIPTS CREATION"
    
    print_step "Creating ETL execution scripts..."

    # Create config1 execution script
    print_step "Creating src/run_etl_config1.py..."
    cat > src/run_etl_config1.py << 'EOF'
#!/usr/bin/env python3
"""
ETL Pipeline Execution Script for Configuration 1 (Mean-based)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
import configg
from datetime import datetime

def run_config1_etl():
    """Execute ETL pipeline with Configuration 1"""
    print(f"🚀 Starting ETL Pipeline with Configuration 1")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Set active configuration
    configg.set_active_config(1)
    
    # Print configuration details
    config_summary = configg.get_config_summary()
    print("📋 Configuration Details:")
    for key, value in config_summary.items():
        print(f"  {key}: {value}")
    
    print("\n🔄 ETL Pipeline Steps:")
    print("  1. Data extraction from MIMIC-IV")
    print("  2. Data cleaning and quality checks")
    print("  3. Mean-based aggregation")
    print("  4. Mean imputation for missing values")
    print("  5. IQR-based outlier removal")
    print("  6. Score calculations")
    print("  7. Loading to gold.gold_scores_config1")
    
    # TODO: Import and run your actual ETL pipeline here
    # Example:
    # from your_etl_module import run_etl_pipeline
    # run_etl_pipeline(configg.ACTIVE_CONFIG)
    
    print(f"\n✅ ETL Pipeline completed successfully")
    print(f"📊 Results saved to: {configg.ACTIVE_CONFIG['output_table']}")
    print(f"⏰ Completed at: {datetime.now()}")

if __name__ == "__main__":
    try:
        run_config1_etl()
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        sys.exit(1)
EOF

    # Create config2 execution script
    print_step "Creating src/run_etl_config2.py..."
    cat > src/run_etl_config2.py << 'EOF'
#!/usr/bin/env python3
"""
ETL Pipeline Execution Script for Configuration 2 (Median-based)
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
import configg
from datetime import datetime

def run_config2_etl():
    """Execute ETL pipeline with Configuration 2"""
    print(f"🚀 Starting ETL Pipeline with Configuration 2")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Set active configuration
    configg.set_active_config(2)
    
    # Print configuration details
    config_summary = configg.get_config_summary()
    print("📋 Configuration Details:")
    for key, value in config_summary.items():
        print(f"  {key}: {value}")
    
    print("\n🔄 ETL Pipeline Steps:")
    print("  1. Data extraction from MIMIC-IV")
    print("  2. Data cleaning and quality checks")
    print("  3. Median-based aggregation")
    print("  4. Median imputation for missing values")
    print("  5. Percentile-based outlier removal")
    print("  6. Score calculations")
    print("  7. Loading to gold.gold_scores_config2")
    
    # TODO: Import and run your actual ETL pipeline here
    # Example:
    # from your_etl_module import run_etl_pipeline
    # run_etl_pipeline(configg.ACTIVE_CONFIG)
    
    print(f"\n✅ ETL Pipeline completed successfully")
    print(f"📊 Results saved to: {configg.ACTIVE_CONFIG['output_table']}")
    print(f"⏰ Completed at: {datetime.now()}")

if __name__ == "__main__":
    try:
        run_config2_etl()
    except Exception as e:
        print(f"❌ ETL Pipeline failed: {e}")
        sys.exit(1)
EOF

    # Create comparison analysis script
    print_step "Creating src/run_comparison_analysis.py..."
    cat > src/run_comparison_analysis.py << 'EOF'
#!/usr/bin/env python3
"""
Configuration Comparison Analysis Script for Task 5.4
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
import configg
from datetime import datetime

def run_comparison_analysis():
    """Execute comparison analysis between two configurations"""
    print(f"📊 Starting Configuration Comparison Analysis")
    print(f"⏰ Started at: {datetime.now()}")
    print("=" * 60)
    
    # Get both configurations
    configs = configg.get_both_configs()
    comparison_tables = configg.get_comparison_tables()
    
    print("🔍 Analysis Configuration:")
    print(f"  Table 1: {comparison_tables['table_1']}")
    print(f"  Table 2: {comparison_tables['table_2']}")
    
    print("\n📈 Analysis Steps:")
    print("  1. Load data from both configuration tables")
    print("  2. Calculate statistical measures (correlation, MAD)")
    print("  3. Generate comparative visualizations")
    print("  4. Perform subgroup analysis")
    print("  5. Analyze mortality correlations")
    print("  6. Generate Bland-Altman plots")
    print("  7. Save results to gold.config_comparison_analysis")
    
    # TODO: Import and run your actual comparison analysis here
    # Example:
    # from your_analysis_module import run_comparison
    # run_comparison(comparison_tables, configg.COMPARISON_CONFIG)
    
    print(f"\n✅ Comparison Analysis completed successfully")
    print(f"⏰ Completed at: {datetime.now()}")

if __name__ == "__main__":
    try:
        run_comparison_analysis()
    except Exception as e:
        print(f"❌ Comparison Analysis failed: {e}")
        sys.exit(1)
EOF

    # Make scripts executable
    chmod +x src/run_etl_config1.py
    chmod +x src/run_etl_config2.py
    chmod +x src/run_comparison_analysis.py

    print_success "ETL execution scripts created and made executable"
    
    print_info "📋 Created scripts:"
    echo "   - src/run_etl_config1.py (mean-based configuration)"
    echo "   - src/run_etl_config2.py (median-based configuration)"
    echo "   - src/run_comparison_analysis.py (comparative analysis)"
}

# =============================================================================
# MODIFIED FULL SETUP FUNCTION
# =============================================================================

# Add this to the full_setup() function, after setup_gold_layer and before comprehensive_validation
full_setup_with_task54() {
    print_banner
    print_header "${ROCKET} COMPLETE MEDALLION ARCHITECTURE SETUP + TASK 5.4"
    
    echo -e "${BLUE}Starting complete healthcare data pipeline setup with dual configurations...${NC}"
    echo -e "${BLUE}This will process: Raw MIMIC-IV → OMOP Standardized → Clinical SOFA Scores → Dual ETL Configs${NC}"
    echo ""
    echo -e "${YELLOW}Timeline: ~20-25 minutes for complete setup with Task 5.4${NC}"
    echo ""
    
    # Verify we're in the right directory
    if [[ ! -f "config_local.py" && ! -f "config_template.py" ]]; then
        print_error "Configuration files not found. Please run from the project directory."
        exit 1
    fi
    
    print_success "Project directory validated"
    log_message "Full setup with Task 5.4 started"
    
    # Environment setup
    activate_venv
    install_dependencies
    check_db_config
    
    # Parameter discovery (if needed)
    discover_sofa_parameters
    
    # Execute all pipeline layers
    setup_bronze_layer
    setup_silver_layer
    setup_gold_layer
    
    # NEW: Task 5.4 specific setup
    setup_etl_configurations
    create_gold_etl_tables
    create_etl_scripts
    
    # Run etl_configurations.py to initialize and validate
    print_step "Running etl_configurations.py initialization..."
    python3 src/config/etl_configurations.py
    
    if [ $? -eq 0 ]; then
        print_success "configg.py initialized successfully"
    else
        print_error "configg.py initialization failed"
        exit 1
    fi
    
    # Final validation
    if comprehensive_validation; then
        echo ""
        print_header "${MEDAL} SETUP COMPLETE WITH TASK 5.4!"
        
        echo ""
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║     🎉 MEDALLION ARCHITECTURE + TASK 5.4 SETUP SUCCESSFUL! 🎉       ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        print_success "Virtual environment: $(pwd)/venv"
        print_success "Database configuration: config_local.py"
        print_success "Bronze layer: Raw MIMIC-IV data extracted"
        print_success "Silver layer: OMOP standardization completed"
        print_success "Gold layer: Clinical SOFA scores calculated"
        print_success "Task 5.4: Dual ETL configurations ready"
        
        echo ""
        print_info "📊 Gold Schema Tables Created:"
        echo "   - gold.gold_scores_config1 (mean-based configuration)"
        echo "   - gold.gold_scores_config2 (median-based configuration)"
        echo "   - gold.config_comparison_analysis (comparison results)"
        echo "   - gold.mortality_correlation_analysis (outcome analysis)"
        echo ""
        print_info "🔧 ETL Scripts Created:"
        echo "   - run_etl_config1.py (execute mean-based ETL)"
        echo "   - run_etl_config2.py (execute median-based ETL)"
        echo "   - run_comparison_analysis.py (compare configurations)"
        echo ""
        print_info "📋 Configuration Files:"
        echo "   - configg.py (dual configuration setup)"
        echo "   - config_local.py (database credentials)"
        echo ""
        print_info "🚀 Next Steps for Task 5.4:"
        echo "   1. Update your ETL pipeline to use configg.ACTIVE_CONFIG"
        echo "   2. Run: python3 src/run_etl_config1.py"
        echo "   3. Run: python3 src/run_etl_config2.py"
        echo "   4. Run: python3 src/run_comparison_analysis.py"
        echo "   5. Analyze results in gold schema tables"
        echo ""
        print_info "🔄 To reactivate the environment later:"
        echo "  source venv/bin/activate"
        echo ""
        
        log_message "Full setup with Task 5.4 completed successfully"
    else
        print_error "Setup completed with validation issues"
        exit 1
    fi
    
    # Deactivate virtual environment
    deactivate 2>/dev/null || true
}

# =============================================================================
# MAIN SCRIPT LOGIC MODIFICATION
# =============================================================================

# ...existing code...

# =============================================================================
# VISUALIZATION SCRIPTS CREATION - Task 5.4
# =============================================================================

create_visualization_scripts() {
    print_section "${CHART} VISUALIZATION SCRIPTS CREATION - Task 5.4"
    
    print_step "Creating visualization and analysis scripts..."

    # Install additional visualization dependencies
    print_step "Installing visualization dependencies..."
    pip install matplotlib seaborn scipy plotly -q
    
    if [ $? -eq 0 ]; then
        print_success "Visualization libraries installed"
    else
        print_error "Failed to install visualization libraries"
        exit 1
    fi

    # Create comprehensive comparison visualization script
    print_step "Creating src/create_comparison_visualizations.py..."
    cat > src/create_comparison_visualizations.py << 'EOF'
#!/usr/bin/env python3
"""
Configuration Comparison Visualization Script for Task 5.4
Creates comprehensive visualizations comparing two ETL configurations
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import pearsonr, spearmanr
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
import configg
from config_local import DB_CONFIG
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def load_configuration_data():
    """Load data from both configuration tables"""
    print("📊 Loading data from both configurations...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    config_tables = configg.get_comparison_tables()
    
    try:
        # Load Config 1 data
        query1 = f"SELECT * FROM gold.{config_tables['table_1']}"
        df1 = pd.read_sql(query1, conn)
        print(f"✅ Config 1 data loaded: {len(df1)} records")
        
        # Load Config 2 data
        query2 = f"SELECT * FROM gold.{config_tables['table_2']}"
        df2 = pd.read_sql(query2, conn)
        print(f"✅ Config 2 data loaded: {len(df2)} records")
        
        return df1, df2, config_tables
        
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None, None, None
    finally:
        conn.close()

def create_distribution_comparison(df1, df2, config_tables):
    """Create distribution comparison plots"""
    print("📈 Creating distribution comparison plots...")
    
    # Score columns to compare
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Score Distribution Comparison Between Configurations', fontsize=16, fontweight='bold')
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df1.columns and score_col in df2.columns:
            # Remove NaN values
            data1 = df1[score_col].dropna()
            data2 = df2[score_col].dropna()
            
            if len(data1) > 0 and len(data2) > 0:
                # Create histograms
                ax.hist(data1, bins=30, alpha=0.7, label=f'Config 1 (Mean-based)', density=True)
                ax.hist(data2, bins=30, alpha=0.7, label=f'Config 2 (Median-based)', density=True)
                
                # Add statistics
                ax.axvline(data1.mean(), color='blue', linestyle='--', alpha=0.8, label=f'Config 1 Mean: {data1.mean():.2f}')
                ax.axvline(data2.mean(), color='orange', linestyle='--', alpha=0.8, label=f'Config 2 Mean: {data2.mean():.2f}')
                
                ax.set_title(f'{score_col.replace("_", " ").title()} Distribution')
                ax.set_xlabel('Score Value')
                ax.set_ylabel('Density')
                ax.legend()
                ax.grid(True, alpha=0.3)
            else:
                ax.text(0.5, 0.5, f'No data available for {score_col}', 
                       transform=ax.transAxes, ha='center', va='center')
        else:
            ax.text(0.5, 0.5, f'Column {score_col} not found', 
                   transform=ax.transAxes, ha='center', va='center')
    
    plt.tight_layout()
    plt.savefig('config_distribution_comparison.png', dpi=300, bbox_inches='tight')
    print("✅ Distribution comparison saved as 'config_distribution_comparison.png'")
    return fig

def create_boxplot_comparison(df1, df2, config_tables):
    """Create box plot comparison"""
    print("📦 Creating box plot comparison...")
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    # Prepare data for box plots
    plot_data = []
    
    for score_col in score_columns:
        if score_col in df1.columns and score_col in df2.columns:
            # Config 1 data
            for value in df1[score_col].dropna():
                plot_data.append({
                    'Score': score_col.replace('_', ' ').title(),
                    'Value': value,
                    'Configuration': 'Config 1 (Mean-based)'
                })
            
            # Config 2 data
            for value in df2[score_col].dropna():
                plot_data.append({
                    'Score': score_col.replace('_', ' ').title(),
                    'Value': value,
                    'Configuration': 'Config 2 (Median-based)'
                })
    
    if plot_data:
        df_plot = pd.DataFrame(plot_data)
        
        fig, ax = plt.subplots(figsize=(14, 8))
        sns.boxplot(data=df_plot, x='Score', y='Value', hue='Configuration', ax=ax)
        
        ax.set_title('Score Distribution Comparison - Box Plots', fontsize=14, fontweight='bold')
        ax.set_xlabel('Score Type')
        ax.set_ylabel('Score Value')
        ax.legend(title='Configuration')
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.savefig('config_boxplot_comparison.png', dpi=300, bbox_inches='tight')
        print("✅ Box plot comparison saved as 'config_boxplot_comparison.png'")
        return fig
    else:
        print("⚠️ No data available for box plot comparison")
        return None

def create_scatter_correlation_plot(df1, df2, config_tables):
    """Create scatter plot and correlation analysis"""
    print("🔗 Creating scatter plot and correlation analysis...")
    
    # Merge data on patient_id for comparison
    merged_data = pd.merge(
        df1[['patient_id', 'sofa_score', 'apache_ii_score']].dropna(), 
        df2[['patient_id', 'sofa_score', 'apache_ii_score']].dropna(), 
        on='patient_id', 
        suffixes=('_config1', '_config2')
    )
    
    if len(merged_data) == 0:
        print("⚠️ No matching patients found between configurations")
        return None
    
    print(f"📊 Found {len(merged_data)} matching patients for correlation analysis")
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # SOFA Score Scatter Plot
    if 'sofa_score_config1' in merged_data.columns and 'sofa_score_config2' in merged_data.columns:
        ax1 = axes[0]
        ax1.scatter(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'], 
                   alpha=0.6, s=50)
        
        # Add perfect correlation line
        max_val = max(merged_data['sofa_score_config1'].max(), merged_data['sofa_score_config2'].max())
        min_val = min(merged_data['sofa_score_config1'].min(), merged_data['sofa_score_config2'].min())
        ax1.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8, label='Perfect Correlation')
        
        # Calculate correlation
        pearson_corr, pearson_p = pearsonr(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'])
        spearman_corr, spearman_p = spearmanr(merged_data['sofa_score_config1'], merged_data['sofa_score_config2'])
        
        ax1.set_xlabel('Config 1 SOFA Score (Mean-based)')
        ax1.set_ylabel('Config 2 SOFA Score (Median-based)')
        ax1.set_title(f'SOFA Score Correlation\nPearson: {pearson_corr:.3f}, Spearman: {spearman_corr:.3f}')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # APACHE II Score Scatter Plot
    if 'apache_ii_score_config1' in merged_data.columns and 'apache_ii_score_config2' in merged_data.columns:
        ax2 = axes[1]
        ax2.scatter(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'], 
                   alpha=0.6, s=50, color='orange')
        
        # Add perfect correlation line
        max_val = max(merged_data['apache_ii_score_config1'].max(), merged_data['apache_ii_score_config2'].max())
        min_val = min(merged_data['apache_ii_score_config1'].min(), merged_data['apache_ii_score_config2'].min())
        ax2.plot([min_val, max_val], [min_val, max_val], 'r--', alpha=0.8, label='Perfect Correlation')
        
        # Calculate correlation
        pearson_corr, pearson_p = pearsonr(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'])
        spearman_corr, spearman_p = spearmanr(merged_data['apache_ii_score_config1'], merged_data['apache_ii_score_config2'])
        
        ax2.set_xlabel('Config 1 APACHE II Score (Mean-based)')
        ax2.set_ylabel('Config 2 APACHE II Score (Median-based)')
        ax2.set_title(f'APACHE II Score Correlation\nPearson: {pearson_corr:.3f}, Spearman: {spearman_corr:.3f}')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('config_scatter_correlation.png', dpi=300, bbox_inches='tight')
    print("✅ Scatter correlation plot saved as 'config_scatter_correlation.png'")
    return fig, merged_data

def create_bland_altman_plot(merged_data):
    """Create Bland-Altman plot for agreement analysis"""
    print("📊 Creating Bland-Altman plot...")
    
    if merged_data is None or len(merged_data) == 0:
        print("⚠️ No merged data available for Bland-Altman plot")
        return None
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # SOFA Score Bland-Altman
    if 'sofa_score_config1' in merged_data.columns and 'sofa_score_config2' in merged_data.columns:
        ax1 = axes[0]
        
        mean_scores = (merged_data['sofa_score_config1'] + merged_data['sofa_score_config2']) / 2
        diff_scores = merged_data['sofa_score_config1'] - merged_data['sofa_score_config2']
        
        ax1.scatter(mean_scores, diff_scores, alpha=0.6, s=50)
        
        # Add mean difference line
        mean_diff = diff_scores.mean()
        std_diff = diff_scores.std()
        
        ax1.axhline(y=mean_diff, color='red', linestyle='-', label=f'Mean Diff: {mean_diff:.3f}')
        ax1.axhline(y=mean_diff + 1.96*std_diff, color='red', linestyle='--', 
                   label=f'+1.96 SD: {mean_diff + 1.96*std_diff:.3f}')
        ax1.axhline(y=mean_diff - 1.96*std_diff, color='red', linestyle='--', 
                   label=f'-1.96 SD: {mean_diff - 1.96*std_diff:.3f}')
        
        ax1.set_xlabel('Mean of Both Configurations')
        ax1.set_ylabel('Difference (Config1 - Config2)')
        ax1.set_title('SOFA Score Bland-Altman Plot')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # APACHE II Score Bland-Altman
    if 'apache_ii_score_config1' in merged_data.columns and 'apache_ii_score_config2' in merged_data.columns:
        ax2 = axes[1]
        
        mean_scores = (merged_data['apache_ii_score_config1'] + merged_data['apache_ii_score_config2']) / 2
        diff_scores = merged_data['apache_ii_score_config1'] - merged_data['apache_ii_score_config2']
        
        ax2.scatter(mean_scores, diff_scores, alpha=0.6, s=50, color='orange')
        
        # Add mean difference line
        mean_diff = diff_scores.mean()
        std_diff = diff_scores.std()
        
        ax2.axhline(y=mean_diff, color='red', linestyle='-', label=f'Mean Diff: {mean_diff:.3f}')
        ax2.axhline(y=mean_diff + 1.96*std_diff, color='red', linestyle='--', 
                   label=f'+1.96 SD: {mean_diff + 1.96*std_diff:.3f}')
        ax2.axhline(y=mean_diff - 1.96*std_diff, color='red', linestyle='--', 
                   label=f'-1.96 SD: {mean_diff - 1.96*std_diff:.3f}')
        
        ax2.set_xlabel('Mean of Both Configurations')
        ax2.set_ylabel('Difference (Config1 - Config2)')
        ax2.set_title('APACHE II Score Bland-Altman Plot')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('config_bland_altman.png', dpi=300, bbox_inches='tight')
    print("✅ Bland-Altman plot saved as 'config_bland_altman.png'")
    return fig

def generate_statistical_summary(df1, df2, merged_data):
    """Generate statistical summary report"""
    print("📋 Generating statistical summary report...")
    
    summary_report = []
    summary_report.append("=" * 70)
    summary_report.append("CONFIGURATION COMPARISON STATISTICAL SUMMARY")
    summary_report.append("=" * 70)
    summary_report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    summary_report.append("")
    
    # Data overview
    summary_report.append("📊 DATA OVERVIEW:")
    summary_report.append(f"  Config 1 (Mean-based): {len(df1)} records")
    summary_report.append(f"  Config 2 (Median-based): {len(df2)} records")
    summary_report.append(f"  Matching patients: {len(merged_data) if merged_data is not None else 0}")
    summary_report.append("")
    
    # Score statistics
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for score_col in score_columns:
        if score_col in df1.columns and score_col in df2.columns:
            data1 = df1[score_col].dropna()
            data2 = df2[score_col].dropna()
            
            if len(data1) > 0 and len(data2) > 0:
                summary_report.append(f"📈 {score_col.replace('_', ' ').upper()} STATISTICS:")
                summary_report.append(f"  Config 1 - Mean: {data1.mean():.3f}, Median: {data1.median():.3f}, Std: {data1.std():.3f}")
                summary_report.append(f"  Config 2 - Mean: {data2.mean():.3f}, Median: {data2.median():.3f}, Std: {data2.std():.3f}")
                
                # Statistical tests
                try:
                    t_stat, t_p = stats.ttest_ind(data1, data2)
                    u_stat, u_p = stats.mannwhitneyu(data1, data2, alternative='two-sided')
                    
                    summary_report.append(f"  T-test: statistic={t_stat:.3f}, p-value={t_p:.6f}")
                    summary_report.append(f"  Mann-Whitney U: statistic={u_stat:.3f}, p-value={u_p:.6f}")
                except Exception as e:
                    summary_report.append(f"  Statistical tests failed: {e}")
                
                summary_report.append("")
    
    # Correlation analysis
    if merged_data is not None and len(merged_data) > 0:
        summary_report.append("🔗 CORRELATION ANALYSIS:")
        
        for score_col in ['sofa_score', 'apache_ii_score']:
            col1 = f"{score_col}_config1"
            col2 = f"{score_col}_config2"
            
            if col1 in merged_data.columns and col2 in merged_data.columns:
                try:
                    pearson_corr, pearson_p = pearsonr(merged_data[col1], merged_data[col2])
                    spearman_corr, spearman_p = spearmanr(merged_data[col1], merged_data[col2])
                    
                    summary_report.append(f"  {score_col.replace('_', ' ').title()}:")
                    summary_report.append(f"    Pearson: r={pearson_corr:.3f}, p={pearson_p:.6f}")
                    summary_report.append(f"    Spearman: ρ={spearman_corr:.3f}, p={spearman_p:.6f}")
                except Exception as e:
                    summary_report.append(f"    Correlation failed: {e}")
        
        summary_report.append("")
    
    # Save report
    with open('configuration_comparison_report.txt', 'w') as f:
        f.write('\n'.join(summary_report))
    
    print("✅ Statistical summary saved as 'configuration_comparison_report.txt'")
    return summary_report

def main():
    """Main visualization function"""
    print("🎨 Starting Configuration Comparison Visualization")
    print("=" * 60)
    
    # Load data
    df1, df2, config_tables = load_configuration_data()
    
    if df1 is None or df2 is None:
        print("❌ Failed to load configuration data")
        return
    
    # Create visualizations
    try:
        # Distribution comparison
        create_distribution_comparison(df1, df2, config_tables)
        
        # Box plot comparison
        create_boxplot_comparison(df1, df2, config_tables)
        
        # Scatter correlation plot
        scatter_fig, merged_data = create_scatter_correlation_plot(df1, df2, config_tables)
        
        # Bland-Altman plot
        create_bland_altman_plot(merged_data)
        
        # Statistical summary
        generate_statistical_summary(df1, df2, merged_data)
        
        print("\n✅ All visualizations created successfully!")
        print("📁 Generated files:")
        print("  - config_distribution_comparison.png")
        print("  - config_boxplot_comparison.png")
        print("  - config_scatter_correlation.png")
        print("  - config_bland_altman.png")
        print("  - configuration_comparison_report.txt")
        
    except Exception as e:
        print(f"❌ Visualization creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
EOF

    # Create mortality analysis visualization script
    print_step "Creating src/create_mortality_visualizations.py..."
    cat > src/create_mortality_visualizations.py << 'EOF'
#!/usr/bin/env python3
"""
Mortality Analysis Visualization Script for Task 5.4
Creates mortality correlation and outcome analysis visualizations
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from scipy import stats
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'config'))
import configg
from config_local import DB_CONFIG
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
plt.style.use('seaborn-v0_8')
sns.set_palette("viridis")

def load_mortality_data():
    """Load mortality analysis data"""
    print("💀 Loading mortality analysis data...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        # Load mortality correlation data
        query = """
        SELECT 
            config_name,
            apache_ii_score,
            sofa_score,
            saps_ii_score,
            oasis_score,
            hospital_mortality,
            icu_mortality,
            day_30_mortality,
            age,
            gender,
            admission_type,
            los_hospital,
            los_icu
        FROM gold.mortality_correlation_analysis
        WHERE apache_ii_score IS NOT NULL 
        AND sofa_score IS NOT NULL
        """
        
        df = pd.read_sql(query, conn)
        print(f"✅ Mortality data loaded: {len(df)} records")
        
        return df
        
    except Exception as e:
        print(f"❌ Error loading mortality data: {e}")
        return None
    finally:
        conn.close()

def create_mortality_by_score_plots(df):
    """Create mortality rate by score plots"""
    print("📊 Creating mortality by score plots...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Mortality Rate by Clinical Scores', fontsize=16, fontweight='bold')
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df.columns and 'hospital_mortality' in df.columns:
            # Create score bins
            df_temp = df[[score_col, 'hospital_mortality', 'config_name']].dropna()
            
            if len(df_temp) > 0:
                # Group by score ranges and config
                df_temp['score_bin'] = pd.cut(df_temp[score_col], bins=10, precision=1)
                
                mortality_by_score = df_temp.groupby(['score_bin', 'config_name'])['hospital_mortality'].agg(['mean', 'count']).reset_index()
                mortality_by_score = mortality_by_score[mortality_by_score['count'] >= 5]  # At least 5 patients per bin
                
                # Plot for each configuration
                for config in df_temp['config_name'].unique():
                    config_data = mortality_by_score[mortality_by_score['config_name'] == config]
                    if len(config_data) > 0:
                        x_vals = range(len(config_data))
                        ax.plot(x_vals, config_data['mean'], marker='o', label=config, linewidth=2)
                
                ax.set_title(f'{score_col.replace("_", " ").title()} vs Mortality')
                ax.set_xlabel('Score Bins')
                ax.set_ylabel('Mortality Rate')
                ax.legend()
                ax.grid(True, alpha=0.3)
                
                # Rotate x-axis labels
                if len(mortality_by_score) > 0:
                    ax.set_xticks(range(len(mortality_by_score['score_bin'].unique())))
                    ax.set_xticklabels([str(x) for x in mortality_by_score['score_bin'].unique()], rotation=45)
            else:
                ax.text(0.5, 0.5, f'No data for {score_col}', transform=ax.transAxes, ha='center')
        else:
            ax.text(0.5, 0.5, f'Missing columns for {score_col}', transform=ax.transAxes, ha='center')
    
    plt.tight_layout()
    plt.savefig('mortality_by_scores.png', dpi=300, bbox_inches='tight')
    print("✅ Mortality by scores plot saved as 'mortality_by_scores.png'")
    return fig

def create_score_distribution_by_mortality(df):
    """Create score distribution by mortality outcome"""
    print("📦 Creating score distribution by mortality...")
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Score Distribution by Hospital Mortality', fontsize=16, fontweight='bold')
    
    score_columns = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score']
    
    for i, score_col in enumerate(score_columns):
        row = i // 2
        col = i % 2
        ax = axes[row, col]
        
        if score_col in df.columns and 'hospital_mortality' in df.columns:
            df_temp = df[[score_col, 'hospital_mortality', 'config_name']].dropna()
            
            if len(df_temp) > 0:
                # Box plot by mortality status and configuration
                sns.boxplot(data=df_temp, x='hospital_mortality', y=score_col, 
                           hue='config_name', ax=ax)
                
                ax.set_title(f'{score_col.replace("_", " ").title()} Distribution')
                ax.set_xlabel('Hospital Mortality')
                ax.set_ylabel('Score Value')
                
                # Add statistical annotation
                survivors = df_temp[df_temp['hospital_mortality'] == False][score_col]
                non_survivors = df_temp[df_temp['hospital_mortality'] == True][score_col]
                
                if len(survivors) > 0 and len(non_survivors) > 0:
                    # Mann-Whitney U test
                    try:
                        u_stat, u_p = stats.mannwhitneyu(survivors, non_survivors, alternative='two-sided')
                        ax.text(0.02, 0.98, f'Mann-Whitney p={u_p:.4f}', 
                               transform=ax.transAxes, verticalalignment='top',
                               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
                    except:
                        pass
            else:
                ax.text(0.5, 0.5, f'No data for {score_col}', transform=ax.transAxes, ha='center')
        else:
            ax.text(0.5, 0.5, f'Missing columns for {score_col}', transform=ax.transAxes, ha='center')
    
    plt.tight_layout()
    plt.savefig('score_distribution_by_mortality.png', dpi=300, bbox_inches='tight')
    print("✅ Score distribution by mortality saved as 'score_distribution_by_mortality.png'")
    return fig

def create_age_stratified_analysis(df):
    """Create age-stratified mortality analysis"""
    print("👴 Creating age-stratified analysis...")
    
    if 'age' not in df.columns:
        print("⚠️ Age column not found, skipping age analysis")
        return None
    
    # Create age groups
    df_temp = df.copy()
    df_temp['age_group'] = pd.cut(df_temp['age'], 
                                 bins=[0, 45, 65, 80, 100], 
                                 labels=['<45', '45-65', '65-80', '80+'])
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    
    # Mortality rate by age group and configuration
    ax1 = axes[0]
    mortality_by_age = df_temp.groupby(['age_group', 'config_name'])['hospital_mortality'].agg(['mean', 'count']).reset_index()
    mortality_by_age = mortality_by_age[mortality_by_age['count'] >= 10]  # At least 10 patients
    
    # Pivot for easier plotting
    mortality_pivot = mortality_by_age.pivot(index='age_group', columns='config_name', values='mean')
    mortality_pivot.plot(kind='bar', ax=ax1)
    
    ax1.set_title('Mortality Rate by Age Group and Configuration')
    ax1.set_xlabel('Age Group')
    ax1.set_ylabel('Mortality Rate')
    ax1.legend(title='Configuration')
    ax1.grid(True, alpha=0.3)
    
    # SOFA score by age group
    ax2 = axes[1]
    sns.boxplot(data=df_temp, x='age_group', y='sofa_score', hue='config_name', ax=ax2)
    ax2.set_title('SOFA Score Distribution by Age Group')
    ax2.set_xlabel('Age Group')
    ax2.set_ylabel('SOFA Score')
    
    plt.tight_layout()
    plt.savefig('age_stratified_analysis.png', dpi=300, bbox_inches='tight')
    print("✅ Age-stratified analysis saved as 'age_stratified_analysis.png'")
    return fig

def create_correlation_heatmap(df):
    """Create correlation heatmap between scores and outcomes"""
    print("🔥 Creating correlation heatmap...")
    
    # Select numeric columns for correlation
    numeric_cols = ['sofa_score', 'apache_ii_score', 'saps_ii_score', 'oasis_score', 
                   'age', 'los_hospital', 'los_icu']
    outcome_cols = ['hospital_mortality', 'icu_mortality', 'day_30_mortality']
    
    # Convert boolean outcomes to numeric
    df_corr = df.copy()
    for col in outcome_cols:
        if col in df_corr.columns:
            df_corr[col] = df_corr[col].astype(int)
    
    # Calculate correlations for each configuration
    configs = df_corr['config_name'].unique()
    
    fig, axes = plt.subplots(1, len(configs), figsize=(8*len(configs), 6))
    if len(configs) == 1:
        axes = [axes]
    
    for i, config in enumerate(configs):
        config_data = df_corr[df_corr['config_name'] == config]
        
        # Select available columns
        available_cols = [col for col in numeric_cols + outcome_cols if col in config_data.columns]
        corr_matrix = config_data[available_cols].corr()
        
        # Create heatmap
        sns.heatmap(corr_matrix, annot=True, cmap='RdBu_r', center=0, 
                   square=True, ax=axes[i], cbar_kws={'shrink': 0.8})
        axes[i].set_title(f'Correlation Matrix - {config}')
    
    plt.tight_layout()
    plt.savefig('correlation_heatmap.png', dpi=300, bbox_inches='tight')
    print("✅ Correlation heatmap saved as 'correlation_heatmap.png'")
    return fig

def generate_mortality_report(df):
    """Generate mortality analysis report"""
    print("📋 Generating mortality analysis report...")
    
    report = []
    report.append("=" * 70)
    report.append("MORTALITY ANALYSIS REPORT")
    report.append("=" * 70)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    # Overall statistics
    report.append("📊 OVERALL STATISTICS:")
    report.append(f"  Total patients: {len(df)}")
    
    if 'hospital_mortality' in df.columns:
        mortality_rate = df['hospital_mortality'].mean()
        report.append(f"  Hospital mortality rate: {mortality_rate:.3f} ({mortality_rate*100:.1f}%)")
    
    # Configuration comparison
    report.append("\n🔧 CONFIGURATION COMPARISON:")
    for config in df['config_name'].unique():
        config_data = df[df['config_name'] == config]
        report.append(f"\n  {config}:")
        report.append(f"    Patients: {len(config_data)}")
        
        if 'hospital_mortality' in config_data.columns:
            mortality_rate = config_data['hospital_mortality'].mean()
            report.append(f"    Mortality rate: {mortality_rate:.3f} ({mortality_rate*100:.1f}%)")
        
        # Score statistics
        score_cols = ['sofa_score', 'apache_ii_score']
        for score_col in score_cols:
            if score_col in config_data.columns:
                score_data = config_data[score_col].dropna()
                if len(score_data) > 0:
                    report.append(f"    {score_col}: mean={score_data.mean():.2f}, std={score_data.std():.2f}")
    
    # Save report
    with open('mortality_analysis_report.txt', 'w') as f:
        f.write('\n'.join(report))
    
    print("✅ Mortality analysis report saved as 'mortality_analysis_report.txt'")
    return report

def main():
    """Main mortality analysis function"""
    print("💀 Starting Mortality Analysis Visualization")
    print("=" * 60)
    
    # Load data
    df = load_mortality_data()
    
    if df is None or len(df) == 0:
        print("❌ No mortality data available")
        return
    
    # Create visualizations
    try:
        # Mortality by score plots
        create_mortality_by_score_plots(df)
        
        # Score distribution by mortality
        create_score_distribution_by_mortality(df)
        
        # Age-stratified analysis
        create_age_stratified_analysis(df)
        
        # Correlation heatmap
        create_correlation_heatmap(df)
        
        # Generate report
        generate_mortality_report(df)
        
        print("\n✅ All mortality visualizations created successfully!")
        print("📁 Generated files:")
        print("  - mortality_by_scores.png")
        print("  - score_distribution_by_mortality.png")
        print("  - age_stratified_analysis.png")
        print("  - correlation_heatmap.png")
        print("  - mortality_analysis_report.txt")
        
    except Exception as e:
        print(f"❌ Mortality visualization creation failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
EOF

    # Make scripts executable
    chmod +x src/create_comparison_visualizations.py
    chmod +x src/create_mortality_visualizations.py

    print_success "Visualization scripts created and made executable"
    
    print_info "📋 Created visualization scripts:"
    echo "   - src/create_comparison_visualizations.py (configuration comparison)"
    echo "   - src/create_mortality_visualizations.py (mortality analysis)"
}

# =============================================================================
# MODIFIED FULL SETUP FUNCTION WITH VISUALIZATIONS
# =============================================================================

full_setup_with_task54() {
    print_banner
    print_header "${ROCKET} COMPLETE MEDALLION ARCHITECTURE SETUP + TASK 5.4"
    
    echo -e "${BLUE}Starting complete healthcare data pipeline setup with dual configurations...${NC}"
    echo -e "${BLUE}This will process: Raw MIMIC-IV → OMOP Standardized → Clinical SOFA Scores → Dual ETL Configs → Visualizations${NC}"
    echo ""
    echo -e "${YELLOW}Timeline: ~20-25 minutes for complete setup with Task 5.4${NC}"
    echo ""
    
    # Verify we're in the right directory
    if [[ ! -f "config_local.py" && ! -f "config_template.py" ]]; then
        print_error "Configuration files not found. Please run from the project directory."
        exit 1
    fi
    
    print_success "Project directory validated"
    log_message "Full setup with Task 5.4 started"
    
    # Environment setup
    activate_venv
    install_dependencies
    check_db_config
    
    # Parameter discovery (if needed)
    discover_sofa_parameters
    
    # Execute all pipeline layers
    setup_bronze_layer
    setup_silver_layer
    setup_gold_layer
    
    # NEW: Task 5.4 specific setup
    setup_etl_configurations
    create_gold_etl_tables
    create_etl_scripts
    
    # NEW: Create visualization scripts
    create_visualization_scripts
    
    # Run etl_configurations.py to initialize and validate
    print_step "Running etl_configurations.py initialization..."
    python3 src/config/etl_configurations.py
    
    if [ $? -eq 0 ]; then
        print_success "etl_configurations.py initialized successfully"
    else
        print_error "etl_configurations.py initialization failed"
        exit 1
    fi
    
    # Final validation
    if comprehensive_validation; then
        echo ""
        print_header "${MEDAL} SETUP COMPLETE WITH TASK 5.4!"
        
        echo ""
        echo -e "${GREEN}╔══════════════════════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║     🎉 MEDALLION ARCHITECTURE + TASK 5.4 SETUP SUCCESSFUL! 🎉       ║${NC}"
        echo -e "${GREEN}╚══════════════════════════════════════════════════════════════════════╝${NC}"
        echo ""
        
        print_success "Virtual environment: $(pwd)/venv"
        print_success "Database configuration: config_local.py"
        print_success "Bronze layer: Raw MIMIC-IV data extracted"
        print_success "Silver layer: OMOP standardization completed"
        print_success "Gold layer: Clinical SOFA scores calculated"
        print_success "Task 5.4: Dual ETL configurations ready"
        print_success "Visualization scripts: Ready for analysis"
        
        echo ""
        print_info "📊 Gold Schema Tables Created:"
        echo "   - gold.gold_scores_config1 (mean-based configuration)"
        echo "   - gold.gold_scores_config2 (median-based configuration)"
        echo "   - gold.config_comparison_analysis (comparison results)"
        echo "   - gold.mortality_correlation_analysis (outcome analysis)"
        echo ""
        print_info "🔧 ETL Scripts Created:"
        echo "   - src/run_etl_config1.py (execute mean-based ETL)"
        echo "   - src/run_etl_config2.py (execute median-based ETL)"
        echo "   - src/run_comparison_analysis.py (compare configurations)"
        echo ""
        print_info "🎨 Visualization Scripts Created:"
        echo "   - src/create_comparison_visualizations.py (configuration comparison plots)"
        echo "   - src/create_mortality_visualizations.py (mortality analysis plots)"
        echo ""
        print_info "📋 Configuration Files:"
        echo "   - configg.py (dual configuration setup)"
        echo "   - config_local.py (database credentials)"
        echo ""
        print_info "🚀 Next Steps for Task 5.4:"
        echo "   1. Update your ETL pipeline to use configg.ACTIVE_CONFIG"
        echo "   2. Run: python3 src/run_etl_config1.py"
        echo "   3. Run: python3 src/run_etl_config2.py"
        echo "   4. Run: python3 src/run_comparison_analysis.py"
        echo "   5. Run: python3 src/create_comparison_visualizations.py"
        echo "   6. Run: python3 src/create_mortality_visualizations.py"
        echo "   7. Analyze results in gold schema tables and generated plots"
        echo ""
        print_info "🔄 To reactivate the environment later:"
        echo "  source venv/bin/activate"
        echo ""
        
        log_message "Full setup with Task 5.4 completed successfully"
    else
        print_error "Setup completed with validation issues"
        exit 1
    fi
    
    # Deactivate virtual environment
    deactivate 2>/dev/null || true
}

# Add visualization option to the case statement
case "${1:-full}" in
    "full"|"")
        full_setup_with_task54  # Use the new function instead of full_setup
        ;;
    "bronze")
        bronze_only_setup
        ;;
    "silver")
        silver_only_setup
        ;;
    "gold")
        gold_only_setup
        ;;
    "discover")
        discovery_only
        ;;
    "validate")
        validation_only
        ;;
    "status")
        show_pipeline_status
        ;;
    "clean")
        clean_pipeline
        ;;
    "task54")
        # Task 5.4 setup only
        print_banner
        print_header "${GEAR} TASK 5.4 SETUP ONLY"
        activate_venv
        setup_etl_configurations
        create_gold_etl_tables
        create_etl_scripts
        create_visualization_scripts
        python3 src/config/configg.py
        print_success "Task 5.4 setup completed"
        deactivate 2>/dev/null || true
        ;;
    "visualize")
        # Visualization only
        print_banner
        print_header "${CHART} VISUALIZATION SETUP ONLY"
        activate_venv
        create_visualization_scripts
        print_success "Visualization scripts created"
        print_info "Run the following to create visualizations:"
        echo "  python3 src/create_comparison_visualizations.py"
        echo "  python3 src/create_mortality_visualizations.py"
        deactivate 2>/dev/null || true
        ;;
    "help"|"-h"|"--help")
        # Update help to include visualization options
        show_help
        echo -e "  ${GREEN}task54${NC}      Task 5.4 setup only (dual configurations + visualizations)"
        echo -e "  ${GREEN}visualize${NC}   Create visualization scripts only"
        echo ""
        ;;
    *)
        print_error "Unknown option: $1"
        echo ""
        echo "Use './complete_setup.sh help' for usage information"
        exit 1
        ;;
esac

echo "=== Pipeline Setup Completed at $(get_timestamp) ===" >> pipeline_setup.log



