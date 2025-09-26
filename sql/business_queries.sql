-- Ola Ride Insights - Business Intelligence SQL Queries
-- This file contains all SQL queries required for analyzing Ola ride data
-- Author: Data Analytics Team
-- Domain: Ride-Sharing & Mobility Analytics
-- Database: SQLite3

-- ============================================================================
-- SECTION 1: BASIC RIDE ANALYSIS QUERIES
-- ============================================================================

-- Query 1: Retrieve all successful bookings
-- Business Purpose: Get all completed rides for revenue and performance analysis
SELECT 
    Booking_ID,
    Date,
    Customer_ID,
    Vehicle_Type,
    Pickup_Location,
    Drop_Location,
    Booking_Value,
    Payment_Method,
    Ride_Distance,
    Driver_Ratings,
    Customer_Rating
FROM rides 
WHERE Booking_Status = 'Success'
ORDER BY Date DESC;

-- Query 2: Find the average ride distance for each vehicle type
-- Business Purpose: Understanding vehicle utilization patterns for fleet optimization
SELECT 
    Vehicle_Type,
    COUNT(*) as Total_Rides,
    ROUND(AVG(Ride_Distance), 2) as Avg_Distance_KM,
    ROUND(MIN(Ride_Distance), 2) as Min_Distance_KM,
    ROUND(MAX(Ride_Distance), 2) as Max_Distance_KM,
    ROUND(SUM(Ride_Distance), 2) as Total_Distance_KM
FROM rides 
WHERE Booking_Status = 'Success' 
    AND Ride_Distance > 0
GROUP BY Vehicle_Type
ORDER BY Avg_Distance_KM DESC;

-- Query 3: Get the total number of cancelled rides by customers
-- Business Purpose: Identify customer cancellation patterns for service improvement
SELECT 
    COUNT(*) as Total_Customer_Cancellations,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides), 2) as Cancellation_Rate_Percent
FROM rides 
WHERE Booking_Status LIKE '%Customer%';

-- Query 4: List the top 5 customers who booked the highest number of rides
-- Business Purpose: Identify VIP customers for loyalty programs and personalized services
SELECT 
    Customer_ID,
    COUNT(*) as Total_Bookings,
    SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Bookings,
    ROUND(SUM(Booking_Value), 2) as Total_Spent,
    ROUND(AVG(Booking_Value), 2) as Avg_Booking_Value,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate_Percent
FROM rides 
GROUP BY Customer_ID
ORDER BY Total_Bookings DESC
LIMIT 5;

-- Query 5: Get the number of rides cancelled by drivers due to personal and car-related issues
-- Business Purpose: Understand driver-side operational challenges for better support
SELECT 
    Canceled_Rides_by_Driver as Cancellation_Reason,
    COUNT(*) as Total_Cancellations,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides WHERE Booking_Status LIKE '%Driver%'), 2) as Percentage_of_Driver_Cancellations
FROM rides 
WHERE Booking_Status LIKE '%Driver%' 
    AND Canceled_Rides_by_Driver IS NOT NULL
GROUP BY Canceled_Rides_by_Driver
ORDER BY Total_Cancellations DESC;

-- Query 6: Find the maximum and minimum driver ratings for Prime Sedan bookings
-- Business Purpose: Quality assessment for premium vehicle category
SELECT 
    Vehicle_Type,
    COUNT(*) as Total_Ratings,
    ROUND(MIN(Driver_Ratings), 2) as Min_Driver_Rating,
    ROUND(MAX(Driver_Ratings), 2) as Max_Driver_Rating,
    ROUND(AVG(Driver_Ratings), 2) as Avg_Driver_Rating,
    COUNT(CASE WHEN Driver_Ratings >= 4.0 THEN 1 END) as High_Quality_Rides
FROM rides 
WHERE Vehicle_Type = 'Prime Sedan' 
    AND Driver_Ratings IS NOT NULL
GROUP BY Vehicle_Type;

-- Query 7: Retrieve all rides where payment was made using UPI
-- Business Purpose: Digital payment adoption analysis for financial strategy
SELECT 
    Booking_ID,
    Date,
    Customer_ID,
    Vehicle_Type,
    Booking_Value,
    Payment_Method,
    Booking_Status
FROM rides 
WHERE Payment_Method = 'UPI'
ORDER BY Booking_Value DESC;

-- Query 8: Find the average customer rating per vehicle type
-- Business Purpose: Vehicle type satisfaction analysis for service improvement
SELECT 
    Vehicle_Type,
    COUNT(Customer_Rating) as Total_Customer_Ratings,
    ROUND(AVG(Customer_Rating), 2) as Avg_Customer_Rating,
    ROUND(MIN(Customer_Rating), 2) as Min_Customer_Rating,
    ROUND(MAX(Customer_Rating), 2) as Max_Customer_Rating,
    COUNT(CASE WHEN Customer_Rating >= 4.0 THEN 1 END) as Satisfied_Customers,
    ROUND(COUNT(CASE WHEN Customer_Rating >= 4.0 THEN 1 END) * 100.0 / COUNT(Customer_Rating), 2) as Satisfaction_Rate_Percent
FROM rides 
WHERE Customer_Rating IS NOT NULL 
    AND Booking_Status = 'Success'
GROUP BY Vehicle_Type
ORDER BY Avg_Customer_Rating DESC;

-- Query 9: Calculate the total booking value of rides completed successfully
-- Business Purpose: Revenue analysis and business performance measurement
SELECT 
    COUNT(*) as Total_Successful_Rides,
    ROUND(SUM(Booking_Value), 2) as Total_Revenue,
    ROUND(AVG(Booking_Value), 2) as Average_Ride_Value,
    ROUND(MIN(Booking_Value), 2) as Minimum_Ride_Value,
    ROUND(MAX(Booking_Value), 2) as Maximum_Ride_Value
FROM rides 
WHERE Booking_Status = 'Success';

-- Query 10: List all incomplete rides along with the reason
-- Business Purpose: Identify operational issues causing ride incompletions
SELECT 
    Booking_ID,
    Date,
    Customer_ID,
    Vehicle_Type,
    Pickup_Location,
    Drop_Location,
    Incomplete_Rides,
    Incomplete_Rides_Reason,
    Booking_Value
FROM rides 
WHERE Incomplete_Rides IS NOT NULL 
    AND Incomplete_Rides != 'No'
ORDER BY Date DESC;

-- ============================================================================
-- SECTION 2: ADVANCED BUSINESS INTELLIGENCE QUERIES
-- ============================================================================

-- Query 11: Hourly demand analysis for driver allocation optimization
-- Business Purpose: Identify peak hours for dynamic driver allocation
SELECT 
    Hour,
    COUNT(*) as Total_Bookings,
    SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Bookings,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate_Percent,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Hourly_Revenue
FROM rides 
GROUP BY Hour
ORDER BY Total_Bookings DESC;

-- Query 12: Vehicle performance comparison with profitability analysis
-- Business Purpose: Fleet optimization and vehicle type ROI analysis
SELECT 
    Vehicle_Type,
    COUNT(*) as Total_Bookings,
    SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Rides,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Total_Revenue,
    ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Booking_Value END), 2) as Avg_Revenue_Per_Ride,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Ride_Distance ELSE 0 END), 2) as Total_Distance,
    ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Revenue_Per_KM END), 2) as Revenue_Per_KM,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate_Percent
FROM rides 
GROUP BY Vehicle_Type
ORDER BY Total_Revenue DESC;

-- Query 13: Payment method preference and revenue distribution
-- Business Purpose: Financial strategy and payment gateway optimization
SELECT 
    Payment_Method,
    COUNT(*) as Total_Transactions,
    ROUND(SUM(Booking_Value), 2) as Total_Revenue,
    ROUND(AVG(Booking_Value), 2) as Avg_Transaction_Value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM rides WHERE Payment_Method IS NOT NULL), 2) as Usage_Percentage,
    ROUND(SUM(Booking_Value) * 100.0 / (SELECT SUM(Booking_Value) FROM rides WHERE Payment_Method IS NOT NULL), 2) as Revenue_Percentage
FROM rides 
WHERE Payment_Method IS NOT NULL 
    AND Booking_Status = 'Success'
GROUP BY Payment_Method
ORDER BY Total_Revenue DESC;

-- Query 14: Geographic demand analysis by pickup location
-- Business Purpose: Location-based strategy and service area optimization
SELECT 
    Pickup_Location,
    COUNT(*) as Total_Pickups,
    SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as Successful_Pickups,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN Booking_Value ELSE 0 END), 2) as Location_Revenue,
    ROUND(AVG(CASE WHEN Booking_Status = 'Success' THEN Ride_Distance END), 2) as Avg_Trip_Distance,
    ROUND(SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Success_Rate_Percent
FROM rides 
GROUP BY Pickup_Location
HAVING COUNT(*) >= 100  -- Focus on high-volume locations
ORDER BY Total_Pickups DESC
LIMIT 10;

-- Query 15: Customer segmentation based on spending and frequency
-- Business Purpose: Customer lifetime value analysis and targeted marketing
SELECT 
    CASE 
        WHEN Total_Spent >= 5000 THEN 'High Value'
        WHEN Total_Spent >= 2000 THEN 'Medium Value'
        WHEN Total_Spent >= 500 THEN 'Regular'
        ELSE 'Occasional'
    END as Customer_Segment,
    COUNT(*) as Customer_Count,
    ROUND(AVG(Total_Spent), 2) as Avg_Customer_Value,
    ROUND(AVG(Total_Bookings), 2) as Avg_Bookings_Per_Customer,
    ROUND(SUM(Total_Spent), 2) as Segment_Revenue,
    ROUND(SUM(Total_Spent) * 100.0 / (SELECT SUM(Total_Spent) FROM customer_summary), 2) as Revenue_Contribution_Percent
FROM customer_summary
GROUP BY 
    CASE 
        WHEN Total_Spent >= 5000 THEN 'High Value'
        WHEN Total_Spent >= 2000 THEN 'Medium Value'
        WHEN Total_Spent >= 500 THEN 'Regular'
        ELSE 'Occasional'
    END
ORDER BY Avg_Customer_Value DESC;

-- Query 16: Daily revenue trend analysis for business forecasting
-- Business Purpose: Revenue forecasting and trend identification
SELECT 
    Date,
    Total_Bookings,
    Successful_Bookings,
    Total_Revenue,
    ROUND(Total_Revenue / Successful_Bookings, 2) as Revenue_Per_Successful_Ride,
    LAG(Total_Revenue) OVER (ORDER BY Date) as Previous_Day_Revenue,
    ROUND((Total_Revenue - LAG(Total_Revenue) OVER (ORDER BY Date)) * 100.0 / LAG(Total_Revenue) OVER (ORDER BY Date), 2) as Revenue_Growth_Percent
FROM daily_summary
WHERE Total_Revenue > 0
ORDER BY Date;

-- Query 17: Driver performance analysis for quality improvement
-- Business Purpose: Driver training and performance management
SELECT 
    ROUND(Driver_Ratings, 1) as Rating_Range,
    COUNT(*) as Total_Rides,
    COUNT(DISTINCT Customer_ID) as Unique_Customers,
    ROUND(AVG(Booking_Value), 2) as Avg_Booking_Value,
    ROUND(AVG(Customer_Rating), 2) as Avg_Customer_Rating,
    ROUND(AVG(Ride_Distance), 2) as Avg_Distance
FROM rides 
WHERE Driver_Ratings IS NOT NULL 
    AND Booking_Status = 'Success'
GROUP BY ROUND(Driver_Ratings, 1)
ORDER BY Rating_Range DESC;

-- Query 18: Cancellation pattern analysis by time and vehicle type
-- Business Purpose: Operational efficiency and service reliability improvement
SELECT 
    Vehicle_Type,
    Hour,
    COUNT(*) as Total_Bookings,
    SUM(CASE WHEN Booking_Status LIKE '%Cancel%' THEN 1 ELSE 0 END) as Total_Cancellations,
    ROUND(SUM(CASE WHEN Booking_Status LIKE '%Cancel%' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as Cancellation_Rate_Percent,
    SUM(CASE WHEN Booking_Status LIKE '%Customer%' THEN 1 ELSE 0 END) as Customer_Cancellations,
    SUM(CASE WHEN Booking_Status LIKE '%Driver%' THEN 1 ELSE 0 END) as Driver_Cancellations
FROM rides 
GROUP BY Vehicle_Type, Hour
HAVING COUNT(*) >= 10  -- Focus on significant data points
ORDER BY Vehicle_Type, Cancellation_Rate_Percent DESC;