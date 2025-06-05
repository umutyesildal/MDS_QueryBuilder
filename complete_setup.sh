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
        print_success "Virtual environment created successfully"
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
    
    if python3 parameter_discovery.py; then
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
    if [[ -f "enhanced_bronze_builder.py" ]]; then
        print_progress "Running enhanced Bronze extraction..."
        if python3 enhanced_bronze_builder.py 2>&1 | tee bronze_extraction.log; then
            print_success "Enhanced Bronze layer extraction completed"
        else
            print_error "Enhanced Bronze extraction failed"
            exit 1
        fi
    elif [[ -f "querybuilder.py" ]]; then
        print_progress "Running standard Bronze extraction..."
        if python3 querybuilder.py 2>&1 | tee bronze_extraction.log; then
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
    if [[ -f "enhanced_silver_builder.py" ]]; then
        print_progress "Running enhanced Silver processing..."
        if python3 enhanced_silver_builder.py 2>&1 | tee silver_processing.log; then
            print_success "Enhanced Silver layer processing completed"
        else
            print_error "Enhanced Silver processing failed"
            exit 1
        fi
    elif [[ -f "standardize_data.py" ]]; then
        print_progress "Running standard Silver processing..."
        if python3 standardize_data.py 2>&1 | tee silver_processing.log; then
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
    if [[ -f "enhanced_sofa_calculator.py" ]]; then
        print_progress "Running enhanced SOFA calculation..."
        if python3 enhanced_sofa_calculator.py 2>&1 | tee gold_sofa_calculation.log; then
            print_success "Enhanced SOFA calculation completed"
        else
            print_error "Enhanced SOFA calculation failed"
            exit 1
        fi
    elif [[ -f "calculate_sofa_gold.py" ]]; then
        print_progress "Running standard SOFA calculation..."
        if python3 calculate_sofa_gold.py 2>&1 | tee gold_sofa_calculation.log; then
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

# Handle command line arguments
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
    "help"|"-h"|"--help")
        show_help
        ;;
    *)
        print_error "Unknown option: $1"
        echo ""
        echo "Use './complete_setup.sh help' for usage information"
        exit 1
        ;;
esac

echo "=== Pipeline Setup Completed at $(get_timestamp) ===" >> pipeline_setup.log
