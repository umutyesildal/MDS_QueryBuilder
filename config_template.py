"""
Database Configuration Template
===============================

Copy this file to 'config_local.py' and update with your database credentials.
The 'config_local.py' file is gitignored for security.
"""

# Database Configuration - UPDATE THESE VALUES
DB_CONFIG = {
    'host': 'localhost',           # Your PostgreSQL host
    'port': 5432,                  # Your PostgreSQL port  
    'database': 'mimiciv',         # Your MIMIC-IV database name
    'user': 'bernazehraural',       # Your PostgreSQL username
    'password': None,              # Your password (None for OS auth)
    # Alternative: 'password': 'your_password'
}

# Example configurations for different setups:

# Local PostgreSQL with password authentication:
# DB_CONFIG = {
#     'host': 'localhost',
#     'port': 5432,
#     'database': 'mimiciv',
#     'user': 'postgres', 
#     'password': 'your_password'
# }

# Remote database:
# DB_CONFIG = {
#     'host': 'your-remote-host.com',
#     'port': 5432,
#     'database': 'mimiciv',
#     'user': 'your_username',
#     'password': 'your_password'
# }

# Using environment variables (recommended for production):
# import os
# DB_CONFIG = {
#     'host': os.getenv('DB_HOST', 'localhost'),
#     'port': int(os.getenv('DB_PORT', 5432)),
#     'database': os.getenv('DB_NAME', 'mimiciv'),
#     'user': os.getenv('DB_USER', 'postgres'),
#     'password': os.getenv('DB_PASSWORD')
# }
