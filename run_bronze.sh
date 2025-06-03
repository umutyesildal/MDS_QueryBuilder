#!/bin/bash
"""
Bronze Layer Data Extraction Runner
===================================

Standalone runner for Bronze layer data extraction with 
proper virtual environment activation.

Usage: ./run_bronze.sh
"""

# Color codes and functions
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ️ $1${NC}"; }

# Activate virtual environment
if [[ ! -d "venv" ]]; then
    print_error "Virtual environment not found. Run setup first: ./setup_test.sh"
    exit 1
fi

print_info "Activating virtual environment..."
source venv/bin/activate

if [[ "$VIRTUAL_ENV" == "" ]]; then
    print_error "Failed to activate virtual environment"
    exit 1
fi

print_success "Virtual environment activated"

# Run Bronze layer extraction
print_info "🗄️ Starting Bronze Layer Data Extraction..."
python querybuilder.py

# Check result
if [[ $? -eq 0 ]]; then
    print_success "Bronze layer extraction completed successfully"
else
    print_error "Bronze layer extraction failed"
    exit 1
fi

# Deactivate environment
deactivate 2>/dev/null || true
print_info "Virtual environment deactivated"
