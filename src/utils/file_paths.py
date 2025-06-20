#!/usr/bin/env python3
"""
File Path Utilities for Organized Output
========================================

Provides utilities to ensure files are saved in the correct directories:
- Images/plots: docs/visualizations/
- Reports: docs/reports/
- Logs: logs/
- Data files: data/ (create if needed)
"""

import os
from datetime import datetime

# Project root directory (assuming this file is in src/utils/)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Directory structure
DIRECTORIES = {
    'docs': os.path.join(PROJECT_ROOT, 'docs'),
    'docs_reports': os.path.join(PROJECT_ROOT, 'docs', 'reports'),
    'docs_visualizations': os.path.join(PROJECT_ROOT, 'docs', 'visualizations'),
    'logs': os.path.join(PROJECT_ROOT, 'logs'),
    'data': os.path.join(PROJECT_ROOT, 'data'),
    'output': os.path.join(PROJECT_ROOT, 'output')
}

def ensure_directories():
    """Create all necessary directories if they don't exist"""
    for dir_name, dir_path in DIRECTORIES.items():
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
            print(f"📁 Created directory: {dir_path}")

def get_report_path(filename):
    """Get full path for a report file in docs/reports/"""
    ensure_directories()
    return os.path.join(DIRECTORIES['docs_reports'], filename)

def get_visualization_path(filename):
    """Get full path for a visualization file in docs/visualizations/"""
    ensure_directories()
    return os.path.join(DIRECTORIES['docs_visualizations'], filename)

def get_log_path(filename):
    """Get full path for a log file in logs/"""
    ensure_directories()
    return os.path.join(DIRECTORIES['logs'], filename)

def get_data_path(filename):
    """Get full path for a data file in data/"""
    ensure_directories()
    return os.path.join(DIRECTORIES['data'], filename)

def get_output_path(filename):
    """Get full path for an output file in output/"""
    ensure_directories()
    return os.path.join(DIRECTORIES['output'], filename)

def get_timestamped_filename(base_name, extension, add_timestamp=True):
    """Generate a timestamped filename"""
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        name_parts = base_name.split('.')
        if len(name_parts) > 1:
            # Remove existing extension if present
            base_name = '.'.join(name_parts[:-1])
        return f"{base_name}_{timestamp}.{extension}"
    else:
        return f"{base_name}.{extension}"

def print_file_locations():
    """Print information about file organization"""
    print("\n📁 File Organization:")
    print("=" * 50)
    print(f"📊 Visualizations: {DIRECTORIES['docs_visualizations']}")
    print(f"📋 Reports: {DIRECTORIES['docs_reports']}")
    print(f"📝 Logs: {DIRECTORIES['logs']}")
    print(f"💾 Data: {DIRECTORIES['data']}")
    print(f"📤 Output: {DIRECTORIES['output']}")
    print("")

# Initialize directories when module is imported
ensure_directories()

if __name__ == "__main__":
    print("🔧 File Path Utilities")
    print("=" * 30)
    print_file_locations()
    
    # Test the functions
    print("🧪 Testing path functions:")
    print(f"Report path: {get_report_path('test_report.txt')}")
    print(f"Visualization path: {get_visualization_path('test_plot.png')}")
    print(f"Log path: {get_log_path('test.log')}")
    print(f"Timestamped file: {get_timestamped_filename('analysis_report', 'md')}")
