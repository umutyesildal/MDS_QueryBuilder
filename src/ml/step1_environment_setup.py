#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Step 1: Environment Setup & Data Verification
ICU Mortality Prediction ML Pipeline

This script verifies data availability and sets up the environment
for 48-hour mortality prediction modeling.
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path

# Colors for beautiful output
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
CYAN = '\033[0;36m'
RED = '\033[0;31m'
WHITE = '\033[1;37m'
NC = '\033[0m'

# Project paths
project_root = Path(__file__).parent.parent.parent
data_dir = project_root / 'data'
processed_dir = data_dir / 'processed'

def print_step(message):
    print(f"{CYAN}🔍 {message}{NC}")

def print_success(message):
    print(f"{GREEN}✅ {message}{NC}")

def print_info(message):
    print(f"{YELLOW}📋 {message}{NC}")

def print_error(message):
    print(f"{RED}❌ {message}{NC}")

def verify_data_availability():
    """Verify that required data files exist"""
    print_step("Verifying data availability...")
    
    required_files = [
        'ml_dataset_48h_mortality.csv'
    ]
    
    all_good = True
    for file in required_files:
        file_path = data_dir / file
        if file_path.exists():
            file_size = file_path.stat().st_size / (1024 * 1024)  # MB
            print_success(f"Found {file} ({file_size:.1f} MB)")
        else:
            print_error(f"Missing required file: {file}")
            all_good = False
    
    return all_good

def check_data_structure():
    """Check the structure of the mortality dataset"""
    print_step("Analyzing dataset structure...")
    
    try:
        df = pd.read_csv(data_dir / 'ml_dataset_48h_mortality.csv')
        
        print_info(f"Dataset shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print_info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Check for mortality column
        if 'mortality_48h' in df.columns:
            mortality_dist = df['mortality_48h'].value_counts()
            print_info(f"Mortality distribution: {mortality_dist[0]} survived, {mortality_dist[1]} died")
            imbalance_ratio = mortality_dist[0] / mortality_dist[1]
            print_info(f"Class imbalance ratio: {imbalance_ratio:.1f}:1")
        
        # Check missing values
        missing_cols = df.isnull().sum()
        missing_cols = missing_cols[missing_cols > 0]
        if len(missing_cols) > 0:
            print_info(f"Columns with missing values: {len(missing_cols)}")
        else:
            print_success("No missing values detected")
        
        return True
        
    except Exception as e:
        print_error(f"Error analyzing dataset: {str(e)}")
        return False

def setup_directories():
    """Create necessary directories for the ML pipeline"""
    print_step("Setting up directory structure...")
    
    directories = [
        processed_dir,
        project_root / 'models',
        project_root / 'docs' / 'reports',
        project_root / 'docs' / 'visualizations' / 'eda',
        project_root / 'docs' / 'visualizations' / 'models',
        project_root / 'docs' / 'visualizations' / 'xai'
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print_success(f"Directory ready: {directory.relative_to(project_root)}")

def check_python_environment():
    """Check Python environment and required packages"""
    print_step("Checking Python environment...")
    
    print_info(f"Python version: {sys.version.split()[0]}")
    
    required_packages = [
        'pandas', 'numpy', 'sklearn', 'matplotlib', 
        'seaborn', 'shap', 'imblearn'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print_success(f"Package available: {package}")
        except ImportError:
            print_error(f"Missing package: {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print_info(f"Install missing packages with: pip install {' '.join(missing_packages)}")
        return False
    
    return True

def main():
    """Main execution function"""
    print(f"{BLUE}{'='*70}{NC}")
    print(f"{WHITE}🚀 Step 1: Environment Setup & Data Verification{NC}")
    print(f"{BLUE}{'='*70}{NC}")
    print()
    
    success = True
    
    # Check Python environment
    if not check_python_environment():
        success = False
    
    print()
    
    # Verify data availability
    if not verify_data_availability():
        success = False
    
    print()
    
    # Check data structure
    if not check_data_structure():
        success = False
    
    print()
    
    # Setup directories
    setup_directories()
    
    print()
    print(f"{BLUE}{'='*70}{NC}")
    
    if success:
        print_success("Environment setup completed successfully!")
        print_info("Ready to proceed with mortality extraction (Step 2)")
        return 0
    else:
        print_error("Environment setup had issues. Please resolve before proceeding.")
        return 1

if __name__ == "__main__":
    exit(main())
