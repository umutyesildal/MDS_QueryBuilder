#!/usr/bin/env python3
"""
Setup Script for QueryBuilder Environment
=========================================

This script sets up the complete environment for the QueryBuilder
including virtual environment, dependencies, and initial database setup.
"""

import subprocess
import sys
import os


def run_command(command, description):
    """Run a shell command and handle errors."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False


def main():
    """Main setup function."""
    print("🚀 Setting up QueryBuilder for Medical Data Science")
    print("=" * 55)
    
    # Check if we're in the right directory
    if not os.path.exists("querybuilder.py"):
        print("❌ Please run this script from the kod directory")
        sys.exit(1)
    
    # Setup steps
    steps = [
        ("python3 -m venv venv", "Creating virtual environment"),
        ("source venv/bin/activate && pip install --upgrade pip", "Upgrading pip"),
        ("source venv/bin/activate && pip install sqlalchemy psycopg2-binary pandas", "Installing dependencies"),
        ("source venv/bin/activate && python test_db.py", "Testing database connection")
    ]
    
    for command, description in steps:
        if not run_command(command, description):
            print(f"❌ Setup failed at: {description}")
            sys.exit(1)
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Activate virtual environment: source venv/bin/activate")
    print("2. Test database: python test_db.py")
    print("3. Run QueryBuilder: python querybuilder.py") 
    print("4. Validate results: python validate_data.py")
    print("\n📄 Check README.md for detailed documentation")


if __name__ == "__main__":
    main()
