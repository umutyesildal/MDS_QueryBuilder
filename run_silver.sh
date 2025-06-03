#!/bin/bash
"""
Silver Layer Data Standardization Runner
========================================

Standalone runner for Silver layer data standardization with 
proper virtual environment activation.

Usage: ./run_silver.sh
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

# Run Silver layer standardization
print_info "⚙️ Starting Silver Layer Data Standardization..."
python standardize_data.py

# Check result
if [[ $? -eq 0 ]]; then
    print_success "Silver layer standardization completed successfully"
else
    print_error "Silver layer standardization failed"
    exit 1
fi

# Deactivate environment
deactivate 2>/dev/null || true
print_info "Virtual environment deactivated"
