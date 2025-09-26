"""
Ola Ride Insights - Database Manager Module
This module handles all database operations including connection management,
query execution, and data validation.
Author: Data Analytics Team
Domain: Ride-Sharing & Mobility Analytics
"""

import sqlite3
import pandas as pd
import numpy as np
from typing import Union, List, Dict, Any
import logging
from contextlib import contextmanager

class DatabaseManager:
    """
    Database manager class for handling all SQLite operations
    for the Ola ride insights project
    """
    
    def __init__(self, db_path: str = 'data/database/ola_rides.db'):
        """
        Initialize database manager with database path
        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration for database operations"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections to ensure proper cleanup
        Yields:
            sqlite3.Connection: Database connection object
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            yield conn
        except sqlite3.Error as e:
            self.logger.error(f"Database error: {e}")
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> Union[List[Dict], None]:
        """
        Execute a SELECT query and return results as list of dictionaries
        Args:
            query (str): SQL query to execute
            params (tuple): Query parameters for prepared statements
        Returns:
            List[Dict]: Query results or None if error
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                columns = [description[0] for description in cursor.description]
                results = []
                for row in cursor.fetchall():
                    results.append(dict(zip(columns, row)))
                
                self.logger.info(f"Query executed successfully, returned {len(results)} rows")
                return results
                
        except sqlite3.Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None
    
    def execute_query_to_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame
        Args:
            query (str): SQL query to execute
            params (tuple): Query parameters for prepared statements
        Returns:
            pd.DataFrame: Query results as DataFrame
        """
        try:
            with self.get_connection() as conn:
                if params:
                    df = pd.read_sql_query(query, conn, params=params)
                else:
                    df = pd.read_sql_query(query, conn)
                
                self.logger.info(f"Query executed successfully, returned DataFrame with shape {df.shape}")
                return df
                
        except Exception as e:
            self.logger.error(f"Error executing query to DataFrame: {e}")
            return pd.DataFrame()
    
    def execute_update(self, query: str, params: tuple = None) -> bool:
        """
        Execute INSERT, UPDATE, DELETE queries
        Args:
            query (str): SQL query to execute
            params (tuple): Query parameters for prepared statements
        Returns:
            bool: Success status
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                conn.commit()
                affected_rows = cursor.rowcount
                self.logger.info(f"Update query executed successfully, {affected_rows} rows affected")
                return True
                
        except sqlite3.Error as e:
            self.logger.error(f"Error executing update query: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> List[Dict]:
        """
        Get table schema information
        Args:
            table_name (str): Name of the table
        Returns:
            List[Dict]: Table schema information
        """
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_all_tables(self) -> List[str]:
        """
        Get list of all tables in the database
        Returns:
            List[str]: List of table names
        """
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        results = self.execute_query(query)
        return [row['name'] for row in results] if results else []
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get row count for a specific table
        Args:
            table_name (str): Name of the table
        Returns:
            int: Number of rows in the table
        """
        query = f"SELECT COUNT(*) as row_count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]['row_count'] if result else 0
    
    def validate_database_integrity(self) -> Dict[str, Any]:
        """
        Perform comprehensive database integrity checks
        Returns:
            Dict[str, Any]: Validation results
        """
        validation_results = {
            'tables_exist': False,
            'data_quality': {},
            'referential_integrity': {},
            'summary': {}
        }
        
        try:
            # Check if main tables exist
            tables = self.get_all_tables()
            required_tables = ['rides', 'vehicle_summary', 'daily_summary', 'customer_summary']
            missing_tables = [table for table in required_tables if table not in tables]
            
            validation_results['tables_exist'] = len(missing_tables) == 0
            validation_results['existing_tables'] = tables
            validation_results['missing_tables'] = missing_tables
            
            if 'rides' in tables:
                # Data quality checks for main rides table
                rides_validation = self._validate_rides_table()
                validation_results['data_quality']['rides'] = rides_validation
                
                # Summary statistics
                summary_stats = self._get_database_summary()
                validation_results['summary'] = summary_stats
            
            self.logger.info("Database validation completed successfully")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Error during database validation: {e}")
            validation_results['error'] = str(e)
            return validation_results
    
    def _validate_rides_table(self) -> Dict[str, Any]:
        """
        Perform data quality validation on the rides table
        Returns:
            Dict[str, Any]: Validation results for rides table
        """
        validation = {}
        
        # Check for null values in critical columns
        null_check_query = """
        SELECT 
            COUNT(*) as total_rows,
            SUM(CASE WHEN Booking_ID IS NULL THEN 1 ELSE 0 END) as null_booking_ids,
            SUM(CASE WHEN Customer_ID IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
            SUM(CASE WHEN Vehicle_Type IS NULL THEN 1 ELSE 0 END) as null_vehicle_types,
            SUM(CASE WHEN Booking_Status IS NULL THEN 1 ELSE 0 END) as null_booking_status,
            SUM(CASE WHEN Date IS NULL THEN 1 ELSE 0 END) as null_dates
        FROM rides
        """
        
        null_results = self.execute_query_to_dataframe(null_check_query).iloc[0]
        validation['null_values'] = null_results.to_dict()
        
        # Check for data consistency
        consistency_query = """
        SELECT 
            COUNT(DISTINCT Booking_ID) as unique_booking_ids,
            COUNT(*) as total_records,
            COUNT(CASE WHEN Booking_Value < 0 THEN 1 END) as negative_booking_values,
            COUNT(CASE WHEN Ride_Distance < 0 THEN 1 END) as negative_distances,
            COUNT(CASE WHEN Driver_Ratings > 5 OR Driver_Ratings < 1 THEN 1 END) as invalid_driver_ratings,
            COUNT(CASE WHEN Customer_Rating > 5 OR Customer_Rating < 1 THEN 1 END) as invalid_customer_ratings
        FROM rides
        """
        
        consistency_results = self.execute_query_to_dataframe(consistency_query).iloc[0]
        validation['data_consistency'] = consistency_results.to_dict()
        
        # Check booking status distribution
        status_query = """
        SELECT 
            Booking_Status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides), 2) as percentage
        FROM rides 
        GROUP BY Booking_Status
        ORDER BY count DESC
        """
        
        status_results = self.execute_query_to_dataframe(status_query)
        validation['status_distribution'] = status_results.to_dict('records')
        
        return validation
    
    def _get_database_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive database summary statistics
        Returns:
            Dict[str, Any]: Summary statistics
        """
        summary = {}
        
        # Overall statistics
        overall_query = """
        SELECT 
            COUNT(*) as total_bookings,
            COUNT(DISTINCT Customer_ID) as unique_customers,
            COUNT(DISTINCT Vehicle_Type) as vehicle_types,
            SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as successful_bookings,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as total_revenue,
            MIN(Date) as earliest_date,
            MAX(Date) as latest_date
        FROM rides
        """
        
        overall_stats = self.execute_query_to_dataframe(overall_query).iloc[0]
        summary['overall'] = overall_stats.to_dict()
        
        # Vehicle type summary
        vehicle_query = """
        SELECT 
            Vehicle_Type,
            COUNT(*) as bookings,
            SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END) as revenue
        FROM rides 
        GROUP BY Vehicle_Type
        ORDER BY revenue DESC
        """
        
        vehicle_stats = self.execute_query_to_dataframe(vehicle_query)
        summary['vehicle_performance'] = vehicle_stats.to_dict('records')
        
        return summary
    
    def backup_database(self, backup_path: str = None) -> bool:
        """
        Create a backup of the current database
        Args:
            backup_path (str): Path for backup file
        Returns:
            bool: Success status
        """
        if backup_path is None:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"data/database/ola_rides_backup_{timestamp}.db"
        
        try:
            with self.get_connection() as source:
                backup_conn = sqlite3.connect(backup_path)
                source.backup(backup_conn)
                backup_conn.close()
            
            self.logger.info(f"Database backed up successfully to {backup_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating database backup: {e}")
            return False
    
    def optimize_database(self) -> bool:
        """
        Optimize database performance by running VACUUM and ANALYZE
        Returns:
            bool: Success status
        """
        try:
            with self.get_connection() as conn:
                conn.execute("VACUUM")
                conn.execute("ANALYZE")
                conn.commit()
            
            self.logger.info("Database optimization completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error optimizing database: {e}")
            return False

# Predefined business intelligence queries
class BusinessQueries:
    """
    Collection of predefined business intelligence queries
    """
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Initialize with database manager instance
        Args:
            db_manager (DatabaseManager): Database manager instance
        """
        self.db = db_manager
    
    def get_successful_bookings(self) -> pd.DataFrame:
        """Get all successful bookings with details"""
        query = """
        SELECT 
            Booking_ID, Date, Customer_ID, Vehicle_Type,
            Pickup_Location, Drop_Location, Booking_Value,
            Payment_Method, Ride_Distance, Driver_Ratings, Customer_Rating
        FROM rides 
        WHERE Booking_Status = 'Success'
        ORDER BY Date DESC
        """
        return self.db.execute_query_to_dataframe(query)
    
    def get_vehicle_performance(self) -> pd.DataFrame:
        """Get comprehensive vehicle type performance metrics"""
        query = """
        SELECT 
            Vehicle_Type,
            COUNT(*) as Total_Bookings,
            SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Bookings,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Total_Revenue,
            ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Booking_Value END), 2) as Avg_Booking_Value,
            ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Ride_Distance END), 2) as Avg_Distance,
            ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Driver_Ratings END), 2) as Avg_Driver_Rating,
            ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Customer_Rating END), 2) as Avg_Customer_Rating
        FROM rides 
        GROUP BY Vehicle_Type
        ORDER BY Total_Revenue DESC
        """
        return self.db.execute_query_to_dataframe(query)
    
    def get_top_customers(self, limit: int = 10) -> pd.DataFrame:
        """Get top customers by total spending"""
        query = """
        SELECT 
            Customer_ID,
            COUNT(*) as Total_Bookings,
            SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Bookings,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Total_Spent,
            ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Booking_Value END), 2) as Avg_Booking_Value,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate
        FROM rides 
        GROUP BY Customer_ID
        HAVING Total_Spent > 0
        ORDER BY Total_Spent DESC
        LIMIT ?
        """
        return self.db.execute_query_to_dataframe(query, (limit,))
    
    def get_cancellation_analysis(self) -> Dict[str, pd.DataFrame]:
        """Get comprehensive cancellation analysis"""
        results = {}
        
        # Overall cancellation stats
        overall_query = """
        SELECT 
            CASE 
                WHEN Booking_Status = 'Success' THEN 'Successful'
                WHEN Booking_Status LIKE '%Customer%' THEN 'Cancelled by Customer'
                WHEN Booking_Status LIKE '%Driver%' THEN 'Cancelled by Driver'
                ELSE 'Other'
            END as Status_Category,
            COUNT(*) as Count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides), 2) as Percentage
        FROM rides
        GROUP BY Status_Category
        ORDER BY Count DESC
        """
        results['overall'] = self.db.execute_query_to_dataframe(overall_query)
        
        # Customer cancellation reasons
        customer_query = """
        SELECT 
            COALESCE(Canceled_Rides_by_Customer, 'Not Specified') as Reason,
            COUNT(*) as Count
        FROM rides 
        WHERE Booking_Status LIKE '%Customer%'
        GROUP BY Canceled_Rides_by_Customer
        ORDER BY Count DESC
        """
        results['customer_reasons'] = self.db.execute_query_to_dataframe(customer_query)
        
        # Driver cancellation reasons
        driver_query = """
        SELECT 
            COALESCE(Canceled_Rides_by_Driver, 'Not Specified') as Reason,
            COUNT(*) as Count
        FROM rides 
        WHERE Booking_Status LIKE '%Driver%'
        GROUP BY Canceled_Rides_by_Driver
        ORDER BY Count DESC
        """
        results['driver_reasons'] = self.db.execute_query_to_dataframe(driver_query)
        
        return results
    
    def get_revenue_analysis(self) -> Dict[str, pd.DataFrame]:
        """Get comprehensive revenue analysis"""
        results = {}
        
        # Payment method analysis
        payment_query = """
        SELECT 
            Payment_Method,
            COUNT(*) as Total_Transactions,
            ROUND(SUM(Booking_Value), 2) as Total_Revenue,
            ROUND(AVG(Booking_Value), 2) as Avg_Transaction_Value,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides WHERE Payment_Method IS NOT NULL), 2) as Usage_Percentage
        FROM rides 
        WHERE Payment_Method IS NOT NULL AND Booking_Status = 'Success'
        GROUP BY Payment_Method
        ORDER BY Total_Revenue DESC
        """
        results['payment_methods'] = self.db.execute_query_to_dataframe(payment_query)
        
        # Daily revenue trends
        daily_query = """
        SELECT 
            Date,
            COUNT(*) as Total_Bookings,
            SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Bookings,
            ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Daily_Revenue
        FROM rides 
        GROUP BY Date
        ORDER BY Date
        """
        results['daily_trends'] = self.db.execute_query_to_dataframe(daily_query)
        
        return results

def main():
    """Test the database manager functionality"""
    print("=== Testing Database Manager ===")
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Test connection and basic operations
    tables = db_manager.get_all_tables()
    print(f"Available tables: {tables}")
    
    if 'rides' in tables:
        row_count = db_manager.get_table_row_count('rides')
        print(f"Total rides in database: {row_count:,}")
        
        # Test validation
        validation_results = db_manager.validate_database_integrity()
        print(f"Database validation completed: {validation_results['tables_exist']}")
        
        # Test business queries
        bq = BusinessQueries(db_manager)
        vehicle_performance = bq.get_vehicle_performance()
        print(f"Vehicle performance analysis completed: {len(vehicle_performance)} vehicle types")
        
    print("Database manager testing completed!")

if __name__ == "__main__":
    main()