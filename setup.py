"""
Ola Ride Insights - Project Setup Script
This script sets up the complete project environment and initializes all components.
Author: Data Analytics Team
Domain: Ride-Sharing & Mobility Analytics
"""

import os
import sys
import subprocess
import pandas as pd
from pathlib import Path

def create_directory_structure():
    """Create the complete project directory structure"""
    directories = [
        'data/raw',
        'data/processed',
        'data/database',
        'sql',
        'notebooks',
        'src',
        'streamlit_app/pages',
        'streamlit_app/utils',
        'dashboards',
        'logs',
        'tests',
        'config'
    ]
    
    print("Creating project directory structure...")
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ Created: {directory}")
    
    print("Directory structure created successfully!\n")

def create_config_files():
    """Create configuration files for the project"""
    print("Creating configuration files...")
    
    # Create .gitignore
    gitignore_content = """
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
ENV/
env.bak/
venv.bak/

# Jupyter Notebook
.ipynb_checkpoints

# Database files
*.db
*.sqlite
*.sqlite3

# Data files
*.csv
*.xlsx
*.xls
data/raw/*
!data/raw/.gitkeep
data/processed/*
!data/processed/.gitkeep

# Logs
logs/
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Power BI
*.pbix
*.pbit
"""
    
    with open('.gitignore', 'w') as f:
        f.write(gitignore_content)
    print("  ✓ Created: .gitignore")
    
    # Create database configuration
    db_config = """
# Database Configuration for Ola Ride Insights
DB_PATH = 'data/database/ola_rides.db'
BACKUP_PATH = 'data/database/backups/'
LOG_LEVEL = 'INFO'
"""
    
    with open('config/database_config.py', 'w') as f:
        f.write(db_config)
    print("  ✓ Created: config/database_config.py")
    
    # Create Streamlit configuration
    streamlit_config = """
[general]
dataFrameSerialization = "legacy"

[server]
runOnSave = true
port = 8501

[browser]
gatherUsageStats = false

[theme]
primaryColor = "#1f77b4"
backgroundColor = "#ffffff"
secondaryBackgroundColor = "#f0f2f6"
textColor = "#262730"
"""
    
    os.makedirs('.streamlit', exist_ok=True)
    with open('.streamlit/config.toml', 'w') as f:
        f.write(streamlit_config)
    print("  ✓ Created: .streamlit/config.toml")
    
    print("Configuration files created successfully!\n")

def create_sql_files():
    """Create SQL files for database operations"""
    print("Creating SQL files...")
    
    # Create table creation script
    create_tables_sql = """
-- Ola Ride Insights - Database Schema Creation
-- This script creates all necessary tables for the Ola rides database

-- Drop existing tables if they exist
DROP TABLE IF EXISTS rides;
DROP TABLE IF EXISTS vehicle_summary;
DROP TABLE IF EXISTS daily_summary;
DROP TABLE IF EXISTS customer_summary;

-- Create main rides table
CREATE TABLE IF NOT EXISTS rides (
    Booking_ID TEXT PRIMARY KEY,
    Date DATE NOT NULL,
    Time TIME,
    Customer_ID TEXT NOT NULL,
    Vehicle_Type TEXT NOT NULL,
    Pickup_Location TEXT,
    Drop_Location TEXT,
    Booking_Status TEXT NOT NULL,
    V_TAT REAL,
    C_TAT REAL,
    Canceled_Rides_by_Customer TEXT,
    Canceled_Rides_by_Driver TEXT,
    Incomplete_Rides TEXT,
    Incomplete_Rides_Reason TEXT,
    Booking_Value REAL,
    Payment_Method TEXT,
    Ride_Distance REAL,
    Driver_Ratings REAL,
    Customer_Rating REAL,
    Vehicle_Images TEXT,
    Year INTEGER,
    Month INTEGER,
    Day INTEGER,
    Weekday TEXT,
    Hour INTEGER,
    Is_Successful INTEGER,
    Is_Customer_Cancel INTEGER,
    Is_Driver_Cancel INTEGER,
    Revenue_Per_KM REAL
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_rides_date ON rides(Date);
CREATE INDEX IF NOT EXISTS idx_rides_customer ON rides(Customer_ID);
CREATE INDEX IF NOT EXISTS idx_rides_vehicle ON rides(Vehicle_Type);
CREATE INDEX IF NOT EXISTS idx_rides_status ON rides(Booking_Status);
CREATE INDEX IF NOT EXISTS idx_rides_successful ON rides(Is_Successful);
"""
    
    with open('sql/create_tables.sql', 'w') as f:
        f.write(create_tables_sql)
    print("  ✓ Created: sql/create_tables.sql")
    
    # Create data cleaning script
    data_cleaning_sql = """
-- Ola Ride Insights - Data Cleaning Queries
-- This script contains queries for data validation and cleaning

-- Check for duplicate booking IDs
SELECT Booking_ID, COUNT(*) as duplicate_count
FROM rides
GROUP BY Booking_ID
HAVING COUNT(*) > 1;

-- Check for invalid booking values
SELECT COUNT(*) as invalid_booking_values
FROM rides
WHERE Booking_Value < 0 OR Booking_Value IS NULL;

-- Check for invalid ratings
SELECT 
    COUNT(CASE WHEN Driver_Ratings < 1 OR Driver_Ratings > 5 THEN 1 END) as invalid_driver_ratings,
    COUNT(CASE WHEN Customer_Rating < 1 OR Customer_Rating > 5 THEN 1 END) as invalid_customer_ratings
FROM rides;

-- Check for missing critical data
SELECT 
    COUNT(CASE WHEN Customer_ID IS NULL THEN 1 END) as missing_customer_ids,
    COUNT(CASE WHEN Vehicle_Type IS NULL THEN 1 END) as missing_vehicle_types,
    COUNT(CASE WHEN Booking_Status IS NULL THEN 1 END) as missing_booking_status
FROM rides;

-- Update null string values to actual NULL
UPDATE rides SET Payment_Method = NULL WHERE Payment_Method = 'null';
UPDATE rides SET Driver_Ratings = NULL WHERE Driver_Ratings = 'null';
UPDATE rides SET Customer_Rating = NULL WHERE Customer_Rating = 'null';
"""
    
    with open('sql/data_cleaning.sql', 'w') as f:
        f.write(data_cleaning_sql)
    print("  ✓ Created: sql/data_cleaning.sql")
    
    print("SQL files created successfully!\n")

def create_notebook_templates():
    """Create Jupyter notebook templates"""
    print("Creating Jupyter notebook templates...")
    
    # Create EDA notebook template
    eda_template = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "# Ola Ride Insights - Exploratory Data Analysis\n",
                    "\n",
                    "This notebook performs comprehensive exploratory data analysis on the Ola rides dataset.\n",
                    "\n",
                    "## Objectives:\n",
                    "- Understand data structure and quality\n",
                    "- Identify patterns and trends\n",
                    "- Generate insights for business strategy\n"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "source": [
                    "# Import required libraries\n",
                    "import pandas as pd\n",
                    "import numpy as np\n",
                    "import matplotlib.pyplot as plt\n",
                    "import seaborn as sns\n",
                    "import plotly.express as px\n",
                    "import sqlite3\n",
                    "from datetime import datetime\n",
                    "import warnings\n",
                    "warnings.filterwarnings('ignore')\n",
                    "\n",
                    "# Set plotting style\n",
                    "plt.style.use('default')\n",
                    "sns.set_palette('husl')\n"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    import json
    with open('notebooks/01_data_exploration.ipynb', 'w') as f:
        json.dump(eda_template, f, indent=2)
    print("  ✓ Created: notebooks/01_data_exploration.ipynb")
    
    print("Notebook templates created successfully!\n")

def create_utils_files():
    """Create utility files"""
    print("Creating utility files...")
    
    # Create main utils file
    utils_content = '''"""
Ola Ride Insights - Utility Functions
This module contains helper functions used across the project.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

def setup_logging(log_level='INFO'):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs/ola_insights.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def validate_date_range(df, date_column='Date'):
    """Validate date ranges in DataFrame"""
    df[date_column] = pd.to_datetime(df[date_column])
    return {
        'min_date': df[date_column].min(),
        'max_date': df[date_column].max(),
        'date_range_days': (df[date_column].max() - df[date_column].min()).days
    }

def clean_currency_values(series):
    """Clean currency values in a pandas Series"""
    if series.dtype == 'object':
        series = series.str.replace('₹', '').str.replace(',', '')
        series = pd.to_numeric(series, errors='coerce')
    return series

def calculate_business_metrics(df):
    """Calculate key business metrics"""
    total_bookings = len(df)
    successful_bookings = len(df[df['Is_Successful'] == 1])
    success_rate = (successful_bookings / total_bookings) * 100
    
    return {
        'total_bookings': total_bookings,
        'successful_bookings': successful_bookings,
        'success_rate': round(success_rate, 2),
        'total_revenue': df[df['Is_Successful'] == 1]['Booking_Value'].sum(),
        'avg_booking_value': df[df['Is_Successful'] == 1]['Booking_Value'].mean()
    }
'''
    
    with open('src/utils.py', 'w') as f:
        f.write(utils_content)
    print("  ✓ Created: src/utils.py")
    
    # Create __init__.py files
    init_files = ['src/__init__.py', 'streamlit_app/__init__.py', 'streamlit_app/utils/__init__.py']
    for init_file in init_files:
        with open(init_file, 'w') as f:
            f.write('# Ola Ride Insights Package\n')
        print(f"  ✓ Created: {init_file}")
    
    print("Utility files created successfully!\n")

def install_dependencies():
    """Install required Python packages"""
    print("Installing project dependencies...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("  ✓ Dependencies installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"  ✗ Error installing dependencies: {e}")
        return False
    return True

def verify_setup():
    """Verify that the setup was completed successfully"""
    print("Verifying project setup...")
    
    required_dirs = [
        'data/raw', 'data/processed', 'data/database', 'sql', 'notebooks',
        'src', 'streamlit_app', 'dashboards', 'logs', 'config'
    ]
    
    required_files = [
        'requirements.txt', '.gitignore', 'sql/create_tables.sql',
        'sql/data_cleaning.sql', 'src/utils.py', 'config/database_config.py'
    ]
    
    missing_dirs = [d for d in required_dirs if not os.path.exists(d)]
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if not missing_dirs and not missing_files:
        print("  ✓ All directories and files created successfully!")
        return True
    else:
        if missing_dirs:
            print(f"  ✗ Missing directories: {missing_dirs}")
        if missing_files:
            print(f"  ✗ Missing files: {missing_files}")
        return False

def main():
    """Main setup function"""
    print("=" * 60)
    print("    Ola Ride Insights - Project Setup")
    print("    Domain: Ride-Sharing & Mobility Analytics")
    print("=" * 60)
    print()
    
    # Step 1: Create directory structure
    create_directory_structure()
    
    # Step 2: Create configuration files
    create_config_files()
    
    # Step 3: Create SQL files
    create_sql_files()
    
    # Step 4: Create notebook templates
    create_notebook_templates()
    
    # Step 5: Create utility files
    create_utils_files()
    
    # Step 6: Install dependencies
    if install_dependencies():
        print("Dependencies installation completed!\n")
    
    # Step 7: Verify setup
    if verify_setup():
        print("\n" + "=" * 60)
        print("    PROJECT SETUP COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Place your OLA_DataSet.xlsx file in data/raw/")
        print("2. Run: python src/data_processor.py")
        print("3. Launch Streamlit app: streamlit run streamlit_app/app.py")
        print("4. Open Power BI and connect to data/database/ola_rides.db")
        print("\nProject structure created with all required components!")
    else:
        print("\n" + "=" * 60)
        print("    SETUP COMPLETED WITH SOME ISSUES")
        print("=" * 60)
        print("Please check the missing components above.")

if __name__ == "__main__":
    main()