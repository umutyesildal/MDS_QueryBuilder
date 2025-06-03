#!/bin/bash
"""
Gold Layer Analytics Runner
==========================

Standalone runner for Gold layer analytics with virtual environment activation.
"""

# Color codes for output formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}ℹ️ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if we're in the right directory
if [[ ! -f "gold_analytics.py" ]]; then
    print_error "gold_analytics.py not found. Please run from the project directory."
    exit 1
fi

# Check if virtual environment exists
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

print_info "Starting Gold layer analytics pipeline..."
echo ""

# Run Gold layer analytics
if python gold_analytics.py; then
    print_success "Gold layer analytics completed successfully!"
    echo ""
    print_info "Generated outputs:"
    echo "  📊 gold_analytics_report.txt - Comprehensive analytics report"
    echo "  📝 gold_analytics.log - Detailed processing log"
    echo "  🗄️  PostgreSQL Gold views - Analytical views and KPIs"
    echo ""
    print_info "Available Gold layer views:"
    echo "  • gold.patient_summaries - Patient-level metrics"
    echo "  • gold.clinical_indicators - Parameter statistics"
    echo "  • gold.daily_trends - Daily trend analysis"
    echo "  • gold.hourly_patterns - Hourly measurement patterns"
    echo "  • gold.data_quality_summary - Quality dashboard"
else
    print_error "Gold layer analytics failed!"
    echo ""
    print_info "Check gold_analytics.log for details"
    exit 1
fi

# Deactivate virtual environment
deactivate 2>/dev/null || true
