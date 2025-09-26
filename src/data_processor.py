"""
Ola Ride Insights - Data Processing Module
This module handles data loading, cleaning, and preprocessing for the Ola rides dataset.
Author: Data Analytics Team
Domain: Ride-Sharing & Mobility Analytics
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import os
import warnings
warnings.filterwarnings('ignore')

class OlaDataProcessor:
    """
    Main class for processing Ola ride data including cleaning, transformation,
    and database operations.
    """
    
    def __init__(self, excel_file_path='data/raw/OLA_DataSet.xlsx'):
        """
        Initialize the data processor with file path
        Args:
            excel_file_path (str): Path to the Excel file containing raw data
        """
        self.excel_file_path = excel_file_path
        self.raw_data = None
        self.clean_data = None
        self.db_path = 'data/database/ola_rides.db'
        
        # Create necessary directories if they don't exist
        os.makedirs('data/processed', exist_ok=True)
        os.makedirs('data/database', exist_ok=True)
        
    def load_data(self):
        """
        Load data from Excel file and perform initial inspection
        Returns:
            pd.DataFrame: Raw data loaded from Excel
        """
        try:
            print("Loading data from Excel file...")
            self.raw_data = pd.read_excel(self.excel_file_path, sheet_name='July')
            print(f"Data loaded successfully! Shape: {self.raw_data.shape}")
            print(f"Columns: {list(self.raw_data.columns)}")
            return self.raw_data
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return None
    
    def explore_data(self):
        """
        Perform initial data exploration and return key statistics
        Returns:
            dict: Dictionary containing data exploration results
        """
        if self.raw_data is None:
            print("Please load data first using load_data() method")
            return None
            
        exploration_results = {
            'total_rows': len(self.raw_data),
            'total_columns': len(self.raw_data.columns),
            'missing_values': self.raw_data.isnull().sum().to_dict(),
            'data_types': self.raw_data.dtypes.to_dict(),
            'unique_values': {col: self.raw_data[col].nunique() for col in self.raw_data.columns},
            'booking_status_counts': self.raw_data['Booking_Status'].value_counts().to_dict(),
            'vehicle_type_counts': self.raw_data['Vehicle_Type'].value_counts().to_dict()
        }
        
        print("Data Exploration Summary:")
        print(f"Total Records: {exploration_results['total_rows']:,}")
        print(f"Total Features: {exploration_results['total_columns']}")
        print(f"Date Range: {self.raw_data['Date'].min()} to {self.raw_data['Date'].max()}")
        
        return exploration_results
    
    def clean_data(self):
        """
        Perform comprehensive data cleaning including handling nulls,
        data type conversions, and feature engineering
        Returns:
            pd.DataFrame: Cleaned dataset
        """
        if self.raw_data is None:
            print("Please load data first using load_data() method")
            return None
            
        print("Starting data cleaning process...")
        self.clean_data = self.raw_data.copy()
        
        # 1. Handle 'null' string values and convert to actual NaN
        print("Converting 'null' strings to NaN...")
        self.clean_data = self.clean_data.replace('null', np.nan)
        
        # 2. Data type conversions
        print("Converting data types...")
        # Convert Date column to datetime
        self.clean_data['Date'] = pd.to_datetime(self.clean_data['Date'])
        
        # Convert Time column to time format
        self.clean_data['Time'] = pd.to_datetime(self.clean_data['Time']).dt.time
        
        # Convert numeric columns
        numeric_columns = ['V_TAT', 'C_TAT', 'Booking_Value', 'Ride_Distance', 
                          'Driver_Ratings', 'Customer_Rating']
        for col in numeric_columns:
            self.clean_data[col] = pd.to_numeric(self.clean_data[col], errors='coerce')
        
        # 3. Feature Engineering
        print("Creating new features...")
        
        # Extract date components for time-based analysis
        self.clean_data['Year'] = self.clean_data['Date'].dt.year
        self.clean_data['Month'] = self.clean_data['Date'].dt.month
        self.clean_data['Day'] = self.clean_data['Date'].dt.day
        self.clean_data['Weekday'] = self.clean_data['Date'].dt.day_name()
        self.clean_data['Hour'] = pd.to_datetime(self.clean_data['Date'].astype(str) + ' ' + 
                                                self.clean_data['Time'].astype(str)).dt.hour
        
        # Create success flag for easier filtering
        self.clean_data['Is_Successful'] = (self.clean_data['Booking_Status'] == 'Success').astype(int)
        
        # Create cancellation type flags
        self.clean_data['Is_Customer_Cancel'] = self.clean_data['Booking_Status'].str.contains(
            'Customer', na=False).astype(int)
        self.clean_data['Is_Driver_Cancel'] = self.clean_data['Booking_Status'].str.contains(
            'Driver', na=False).astype(int)
        
        # Calculate revenue per km for successful rides
        self.clean_data['Revenue_Per_KM'] = np.where(
            (self.clean_data['Ride_Distance'] > 0) & (self.clean_data['Is_Successful'] == 1),
            self.clean_data['Booking_Value'] / self.clean_data['Ride_Distance'],
            np.nan
        )
        
        # 4. Handle missing values based on business logic
        print("Handling missing values...")
        
        # For cancelled rides, set distance and ratings to 0 where appropriate
        cancelled_mask = self.clean_data['Booking_Status'] != 'Success'
        self.clean_data.loc[cancelled_mask, 'Ride_Distance'] = self.clean_data.loc[cancelled_mask, 'Ride_Distance'].fillna(0)
        
        # Fill Payment_Method for successful rides (assume Cash if missing)
        success_mask = self.clean_data['Booking_Status'] == 'Success'
        self.clean_data.loc[success_mask & self.clean_data['Payment_Method'].isna(), 'Payment_Method'] = 'Cash'
        
        # 5. Data validation and quality checks
        print("Performing data quality checks...")
        
        # Remove records with negative booking values or distances
        initial_count = len(self.clean_data)
        self.clean_data = self.clean_data[
            (self.clean_data['Booking_Value'] >= 0) | self.clean_data['Booking_Value'].isna()
        ]
        self.clean_data = self.clean_data[
            (self.clean_data['Ride_Distance'] >= 0) | self.clean_data['Ride_Distance'].isna()
        ]
        
        removed_count = initial_count - len(self.clean_data)
        if removed_count > 0:
            print(f"Removed {removed_count} records with invalid values")
        
        # 6. Create final cleaned dataset summary
        print("Data cleaning completed!")
        print(f"Final dataset shape: {self.clean_data.shape}")
        print(f"Success rate: {(self.clean_data['Is_Successful'].mean() * 100):.2f}%")
        print(f"Average booking value: ₹{self.clean_data['Booking_Value'].mean():.2f}")
        
        return self.clean_data
    
    def save_cleaned_data(self, output_path='data/processed/ola_rides_clean.csv'):
        """
        Save cleaned data to CSV file
        Args:
            output_path (str): Path where cleaned data should be saved
        """
        if self.clean_data is None:
            print("Please clean data first using clean_data() method")
            return False
            
        try:
            self.clean_data.to_csv(output_path, index=False)
            print(f"Cleaned data saved to: {output_path}")
            return True
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            return False
    
    def create_database(self):
        """
        Create SQLite database and load cleaned data
        Returns:
            bool: Success status of database creation
        """
        if self.clean_data is None:
            print("Please clean data first using clean_data() method")
            return False
            
        try:
            print("Creating SQLite database...")
            
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_path)
            
            # Create main rides table
            self.clean_data.to_sql('rides', conn, if_exists='replace', index=False)
            
            # Create summary tables for better query performance
            self._create_summary_tables(conn)
            
            conn.close()
            print(f"Database created successfully at: {self.db_path}")
            return True
            
        except Exception as e:
            print(f"Error creating database: {str(e)}")
            return False
    
    def _create_summary_tables(self, conn):
        """
        Create summary tables for optimized querying
        Args:
            conn: SQLite database connection
        """
        
        # Vehicle type summary
        vehicle_summary_query = """
        CREATE TABLE IF NOT EXISTS vehicle_summary AS
        SELECT 
            Vehicle_Type,
            COUNT(*) as Total_Bookings,
            SUM(Is_Successful) as Successful_Bookings,
            AVG(Booking_Value) as Avg_Booking_Value,
            SUM(Booking_Value) as Total_Revenue,
            AVG(Ride_Distance) as Avg_Distance,
            SUM(Ride_Distance) as Total_Distance,
            AVG(Driver_Ratings) as Avg_Driver_Rating,
            AVG(Customer_Rating) as Avg_Customer_Rating
        FROM rides 
        GROUP BY Vehicle_Type;
        """
        
        # Daily summary
        daily_summary_query = """
        CREATE TABLE IF NOT EXISTS daily_summary AS
        SELECT 
            Date,
            COUNT(*) as Total_Bookings,
            SUM(Is_Successful) as Successful_Bookings,
            SUM(Booking_Value) as Total_Revenue,
            AVG(Booking_Value) as Avg_Booking_Value,
            SUM(Ride_Distance) as Total_Distance
        FROM rides 
        GROUP BY Date
        ORDER BY Date;
        """
        
        # Customer summary
        customer_summary_query = """
        CREATE TABLE IF NOT EXISTS customer_summary AS
        SELECT 
            Customer_ID,
            COUNT(*) as Total_Bookings,
            SUM(Is_Successful) as Successful_Bookings,
            SUM(Booking_Value) as Total_Spent,
            AVG(Customer_Rating) as Avg_Rating_Given,
            MAX(Date) as Last_Booking_Date
        FROM rides 
        GROUP BY Customer_ID
        ORDER BY Total_Spent DESC;
        """
        
        # Execute summary table creation queries
        conn.execute(vehicle_summary_query)
        conn.execute(daily_summary_query)
        conn.execute(customer_summary_query)
        
        print("Summary tables created successfully!")

def main():
    """
    Main function to execute the complete data processing pipeline
    """
    print("=== Ola Ride Insights - Data Processing Pipeline ===")
    print("Initializing data processor...")
    
    # Initialize processor
    processor = OlaDataProcessor()
    
    # Step 1: Load raw data
    processor.load_data()
    
    # Step 2: Explore data
    exploration_results = processor.explore_data()
    
    # Step 3: Clean data
    processor.clean_data()
    
    # Step 4: Save cleaned data
    processor.save_cleaned_data()
    
    # Step 5: Create database
    processor.create_database()
    
    print("\n=== Data Processing Pipeline Completed Successfully! ===")
    print("Next steps:")
    print("1. Run SQL queries using: python sql/business_queries.py")
    print("2. Launch Streamlit app: streamlit run streamlit_app/app.py")
    print("3. Open Power BI dashboard for visualizations")

if __name__ == "__main__":
    main()