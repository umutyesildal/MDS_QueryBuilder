#!/bin/bash
# QueryBuilder Medical Data Science - Setup & Test Script
# ========================================================
# Übungsblatt 3.2 - Automated setup and validation
# This script sets up the environment and runs comprehensive tests

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️ $1${NC}"
}

# Main setup function
setup_environment() {
    print_header "QUERYBUILDER SETUP & TEST SUITE"
    echo "🏥 Medical Data Science Project Setup"
    echo "📅 $(date)"
    echo ""
    
    # Check if we're in the right directory
    if [[ ! -f "querybuilder.py" ]]; then
        print_error "querybuilder.py not found. Please run this script from the project directory."
        exit 1
    fi
    
    print_success "Project directory validated"
    
    # Step 1: Database Configuration Check
    print_header "STEP 1: DATABASE CONFIGURATION"
    
    if [[ ! -f "config_local.py" ]]; then
        print_warning "config_local.py not found"
        print_info "Creating config_local.py from template..."
        
        if [[ -f "config_template.py" ]]; then
            cp config_template.py config_local.py
            print_warning "Please edit config_local.py with your database credentials before continuing"
            print_info "Opening config_local.py for editing..."
            
            # Try to open with common editors
            if command -v code &> /dev/null; then
                code config_local.py
            elif command -v nano &> /dev/null; then
                nano config_local.py
            else
                print_info "Please manually edit config_local.py with your database settings"
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
    
    # Step 2: Virtual Environment Setup
    print_header "STEP 2: PYTHON ENVIRONMENT SETUP"
    
    if [[ ! -d "venv" ]]; then
        print_info "Creating virtual environment..."
        python3 -m venv venv
        print_success "Virtual environment created"
    else
        print_success "Virtual environment already exists"
    fi
    
    print_info "Activating virtual environment..."
    source venv/bin/activate
    print_success "Virtual environment activated"
    
    print_info "Installing Python dependencies..."
    pip install -r requirements.txt
    print_success "Dependencies installed"
    
    # Step 3: Database Connection Test
    print_header "STEP 3: DATABASE CONNECTION TEST"
    
    print_info "Testing database connection..."
    if python test_db.py; then
        print_success "Database connection successful"
    else
        print_error "Database connection failed"
        print_warning "Please check your config_local.py settings"
        exit 1
    fi
    
    # Step 4: Schema Validation
    print_header "STEP 4: SCHEMA VALIDATION"
    
    print_info "Checking if Bronze schema exists..."
    if python -c "
import psycopg2
from config import DB_CONFIG
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute(\"SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bronze';\")
result = cursor.fetchone()
conn.close()
if result:
    print('Bronze schema exists')
    exit(0)
else:
    print('Bronze schema not found')
    exit(1)
"; then
        print_success "Bronze schema exists"
        
        # Check if data exists
        print_info "Checking existing data..."
        record_count=$(python -c "
import psycopg2
from config import DB_CONFIG
conn = psycopg2.connect(**DB_CONFIG)
cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM bronze.collection_disease;')
count = cursor.fetchone()[0]
conn.close()
print(count)
")
        
        if [[ $record_count -gt 0 ]]; then
            print_success "Found $record_count existing records"
            echo ""
            read -p "Do you want to re-extract data? (y/N): " re_extract
            if [[ $re_extract =~ ^[Yy]$ ]]; then
                run_extraction=true
            else
                run_extraction=false
            fi
        else
            print_info "No existing data found"
            run_extraction=true
        fi
    else
        print_warning "Bronze schema not found - will create during extraction"
        run_extraction=true
    fi
    
    # Step 5: Data Extraction (if needed)
    if [[ $run_extraction == true ]]; then
        print_header "STEP 5: DATA EXTRACTION"
        
        print_info "Starting medical data extraction..."
        if python querybuilder.py; then
            print_success "Data extraction completed successfully"
        else
            print_error "Data extraction failed"
            exit 1
        fi
    else
        print_info "Skipping data extraction (existing data found)"
    fi
    
    # Step 6: Data Validation
    print_header "STEP 6: DATA VALIDATION"
    
    print_info "Running data quality validation..."
    if python validate_data.py; then
        print_success "Data validation completed successfully"
    else
        print_error "Data validation failed"
        exit 1
    fi
    
    # Step 7: Query Testing
    print_header "STEP 7: SQL QUERY TESTING"
    
    print_info "Testing SQL queries on extracted data..."
    if python test_queries.py; then
        print_success "SQL query testing completed successfully"
    else
        print_error "SQL query testing failed"
        exit 1
    fi
    
    # Step 8: Final Status Check
    print_header "STEP 8: FINAL STATUS CHECK"
    
    print_info "Running comprehensive status check..."
    if python check_status.py; then
        print_success "All systems operational"
    else
        print_error "Status check revealed issues"
        exit 1
    fi
    
    # Step 9: Generate Summary Report
    print_header "STEP 9: SUMMARY REPORT GENERATION"
    
    print_info "Generating final summary report..."
    if python generate_summary.py; then
        print_success "Summary report generated successfully"
    else
        print_warning "Summary report generation had issues"
    fi
    
    # Final Success Message
    print_header "🎉 SETUP COMPLETE!"
    
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════════╗"
    echo "║     QueryBuilder Setup Successful!      ║"
    echo "╚══════════════════════════════════════════╝"
    echo -e "${NC}"
    
    print_success "Virtual environment: $(pwd)/venv"
    print_success "Configuration: config_local.py"
    print_success "Database connection: Verified"
    print_success "Data extraction: Complete"
    print_success "Validation: Passed"
    print_success "Query testing: Successful"
    
    echo ""
    print_info "Next steps:"
    echo "  1. Review FINAL_SUMMARY_REPORT.txt for detailed results"
    echo "  2. Explore example_queries.sql for analysis examples"
    echo "  3. Use the extracted data for medical research"
    echo ""
    print_info "To reactivate the environment later:"
    echo "  source venv/bin/activate"
    echo ""
    
    # Deactivate virtual environment
    deactivate 2>/dev/null || true
}

# Quick test function (without full setup)
quick_test() {
    print_header "QUICK TEST MODE"
    
    if [[ ! -f "config_local.py" ]]; then
        print_error "config_local.py not found. Run full setup first."
        exit 1
    fi
    
    if [[ ! -d "venv" ]]; then
        print_error "Virtual environment not found. Run full setup first."
        exit 1
    fi
    
    source venv/bin/activate
    
    print_info "Testing database connection..."
    python test_db.py
    
    print_info "Running status check..."
    python check_status.py
    
    print_success "Quick test completed"
    deactivate 2>/dev/null || true
}

# Main script logic
case "${1:-setup}" in
    "setup"|"")
        setup_environment
        ;;
    "test")
        quick_test
        ;;
    "help"|"-h"|"--help")
        echo "QueryBuilder Setup & Test Script"
        echo ""
        echo "Usage:"
        echo "  ./setup_test.sh [command]"
        echo ""
        echo "Commands:"
        echo "  setup    Full setup and test (default)"
        echo "  test     Quick test of existing setup"
        echo "  help     Show this help message"
        echo ""
        ;;
    *)
        print_error "Unknown command: $1"
        echo "Use './setup_test.sh help' for usage information"
        exit 1
        ;;
esac
