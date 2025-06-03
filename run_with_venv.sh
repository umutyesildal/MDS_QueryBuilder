#!/bin/bash
"""
Python Script Runner with Virtual Environment
==============================================

Utility script to run any Python script with proper virtual environment activation.

Usage: ./run_with_venv.sh <script_name.py> [arguments...]

Examples:
  ./run_with_venv.sh check_status.py
  ./run_with_venv.sh validate_data.py
  ./run_with_venv.sh test_db.py
"""

# Color codes and functions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_success() { echo -e "${GREEN}✅ $1${NC}"; }
print_error() { echo -e "${RED}❌ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️ $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ️ $1${NC}"; }

# Check if script name provided
if [[ $# -eq 0 ]]; then
    print_error "No script specified"
    echo ""
    echo "Usage: ./run_with_venv.sh <script_name.py> [arguments...]"
    echo ""
    echo "Examples:"
    echo "  ./run_with_venv.sh check_status.py"
    echo "  ./run_with_venv.sh validate_data.py"
    echo "  ./run_with_venv.sh test_db.py"
    exit 1
fi

SCRIPT_NAME="$1"
shift  # Remove script name from arguments, keep the rest

# Check if script exists
if [[ ! -f "$SCRIPT_NAME" ]]; then
    print_error "Script not found: $SCRIPT_NAME"
    exit 1
fi

# Check if virtual environment exists
if [[ ! -d "venv" ]]; then
    print_error "Virtual environment not found. Run setup first:"
    echo "  ./setup_test.sh"
    exit 1
fi

# Activate virtual environment
print_info "Activating virtual environment for: $SCRIPT_NAME"
source venv/bin/activate

if [[ "$VIRTUAL_ENV" == "" ]]; then
    print_error "Failed to activate virtual environment"
    exit 1
fi

print_success "Virtual environment activated"
print_info "🐍 Running: python $SCRIPT_NAME $@"

# Run the Python script with any additional arguments
python "$SCRIPT_NAME" "$@"
EXIT_CODE=$?

# Check result and provide appropriate feedback
if [[ $EXIT_CODE -eq 0 ]]; then
    print_success "Script executed successfully: $SCRIPT_NAME"
else
    print_error "Script execution failed: $SCRIPT_NAME (exit code: $EXIT_CODE)"
fi

# Deactivate environment
deactivate 2>/dev/null || true
print_info "Virtual environment deactivated"

exit $EXIT_CODE
