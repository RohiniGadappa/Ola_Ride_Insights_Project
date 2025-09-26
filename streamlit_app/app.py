"""
Ola Ride Insights - Enhanced Streamlit Dashboard
Matching the Power BI design with professional sidebar navigation
Author: Data Analytics Team
Domain: Ride-Sharing & Mobility Analytics
"""

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np

# Page configuration with custom styling
st.set_page_config(
    page_title="Ola Ride Insights",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS to match the Power BI design
st.markdown("""
<style>
    /* Main app styling */
    .main > div {
        padding-top: 2rem;
    }
    
    /* Sidebar styling to match Ola design */
    .css-1d391kg {
        background-color: #2e2e2e;
    }
    
    .sidebar .sidebar-content {
        background-color: #2e2e2e;
        color: white;
    }
    
    /* Custom sidebar navigation buttons */
    .nav-button {
        display: flex;
        align-items: center;
        padding: 15px 20px;
        margin: 5px 0;
        background-color: #2e2e2e;
        color: white;
        text-decoration: none;
        border-radius: 5px;
        border: none;
        cursor: pointer;
        width: 100%;
        text-align: left;
        font-size: 16px;
    }
    
    .nav-button:hover {
        background-color: #7cb342;
    }
    
    .nav-button.active {
        background-color: #7cb342;
    }
    
    /* Vehicle type cards styling */
    .vehicle-card {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Metrics styling */
    .metric-container {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
        border-left: 4px solid #7cb342;
    }
    
    /* Title styling */
    .main-title {
        color: #2e2e2e;
        font-size: 2.5rem;
        font-weight: bold;
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .section-title {
        color: #2e2e2e;
        font-size: 1.8rem;
        font-weight: bold;
        margin-bottom: 1rem;
        border-bottom: 3px solid #7cb342;
        padding-bottom: 10px;
    }
    
    /* Table styling */
    .dataframe {
        border: none;
        border-radius: 10px;
        overflow: hidden;
    }
    
    .dataframe th {
        background-color: #1f77b4;
        color: white;
        font-weight: bold;
    }
    
    .dataframe td {
        border-bottom: 1px solid #e0e0e0;
    }
</style>
""", unsafe_allow_html=True)

# Database connection function
@st.cache_data
def load_data(query):
    """Load data from SQLite database with caching"""
    try:
        conn = sqlite3.connect("data/database/ola_rides.db")
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return pd.DataFrame()

# Sidebar with Ola-style navigation
def create_sidebar():
    """Create the sidebar navigation matching Power BI design"""
    
    # Ola logo and title
    st.sidebar.markdown("""
    <div style='text-align: center; padding: 20px 0;'>
        <div style='background-color: #7cb342; color: white; padding: 10px; border-radius: 10px; font-size: 24px; font-weight: bold;'>
            🚗 OLA
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation options
    nav_options = {
        "📊 Overall": "overall",
        "🚙 Vehicle Type": "vehicle",
        "💰 Revenue": "revenue", 
        "🚫 Cancellation": "cancellation",
        "⭐ Ratings": "ratings"
    }
    
    # Create navigation
    selected_page = st.sidebar.selectbox(
        "Navigate to:",
        list(nav_options.keys()),
        format_func=lambda x: x
    )
    
    return nav_options[selected_page]

# Overall Dashboard Page
def show_overall_dashboard():
    """Display the Overall analytics page matching Power BI design"""
    
    st.markdown('<h1 class="main-title">🚗 Ola Ride Insights Dashboard</h1>', unsafe_allow_html=True)
    
    # Load key metrics
    metrics_query = """
    SELECT 
        COUNT(*) as total_bookings,
        SUM(Is_Successful) as successful_bookings,
        ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Booking_Value ELSE 0 END), 2) as total_revenue,
        COUNT(DISTINCT Customer_ID) as unique_customers,
        ROUND(AVG(CASE WHEN Is_Successful = 1 THEN Driver_Ratings END), 2) as avg_driver_rating,
        ROUND(AVG(CASE WHEN Is_Successful = 1 THEN Customer_Rating END), 2) as avg_customer_rating,
        ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Ride_Distance ELSE 0 END), 2) as total_distance
    FROM rides
    """
    metrics = load_data(metrics_query)
    
    if not metrics.empty:
        m = metrics.iloc[0]
        
        # Key Performance Indicators
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "📊 Total Bookings",
                f"{m['total_bookings']:,}",
                delta=f"Success: {(m['successful_bookings']/m['total_bookings']*100):.1f}%"
            )
        
        with col2:
            st.metric(
                "✅ Successful Rides",
                f"{m['successful_bookings']:,}",
                delta=f"Rate: {(m['successful_bookings']/m['total_bookings']*100):.1f}%"
            )
        
        with col3:
            st.metric(
                "💰 Total Revenue",
                f"₹{m['total_revenue']:,.0f}",
                delta=f"Avg: ₹{(m['total_revenue']/m['successful_bookings']):.0f}"
            )
        
        with col4:
            st.metric(
                "👥 Unique Customers",
                f"{m['unique_customers']:,}",
                delta=f"Avg Bookings: {(m['total_bookings']/m['unique_customers']):.1f}"
            )
    
    st.markdown("---")
    
    # Charts section
    col1, col2 = st.columns(2)
    
    with col1:
        # Booking Status Distribution (matching Power BI pie chart)
        st.markdown('<h3 class="section-title">📈 Booking Status Distribution</h3>', unsafe_allow_html=True)
        
        status_query = """
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
        status_data = load_data(status_query)
        
        if not status_data.empty:
            fig_status = px.pie(
                status_data,
                values='Count',
                names='Status_Category',
                title="Booking Status Distribution",
                color_discrete_sequence=['#2E8B57', '#DC143C', '#FF8C00', '#4682B4']
            )
            fig_status.update_traces(textposition='inside', textinfo='percent+label')
            fig_status.update_layout(showlegend=True, height=400)
            st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Vehicle Type Performance (matching Power BI bar chart)
        st.markdown('<h3 class="section-title">🚙 Vehicle Performance</h3>', unsafe_allow_html=True)
        
        vehicle_query = """
        SELECT 
            Vehicle_Type,
            COUNT(*) as Total_Bookings,
            ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Booking_Value ELSE 0 END), 2) as Revenue
        FROM rides 
        GROUP BY Vehicle_Type
        ORDER BY Revenue DESC
        """
        vehicle_data = load_data(vehicle_query)
        
        if not vehicle_data.empty:
            fig_vehicle = px.bar(
                vehicle_data,
                x='Vehicle_Type',
                y='Revenue',
                title="Revenue by Vehicle Type",
                color='Revenue',
                color_continuous_scale='Blues',
                text='Revenue'
            )
            fig_vehicle.update_traces(texttemplate='₹%{text:,.0f}', textposition='outside')
            fig_vehicle.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig_vehicle, use_container_width=True)
    
    # Daily trends (full width)
    st.markdown('<h3 class="section-title">📊 Daily Booking & Revenue Trends</h3>', unsafe_allow_html=True)
    
    daily_query = """
    SELECT 
        Date,
        COUNT(*) as Total_Bookings,
        SUM(Is_Successful) as Successful_Bookings,
        ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Booking_Value ELSE 0 END), 2) as Daily_Revenue
    FROM rides 
    GROUP BY Date
    ORDER BY Date
    """
    daily_data = load_data(daily_query)
    
    if not daily_data.empty:
        daily_data['Date'] = pd.to_datetime(daily_data['Date'])
        
        fig_daily = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Add booking trends
        fig_daily.add_trace(
            go.Scatter(x=daily_data['Date'], y=daily_data['Total_Bookings'], 
                      name="Total Bookings", line=dict(color='blue', width=2)),
            secondary_y=False,
        )
        
        fig_daily.add_trace(
            go.Scatter(x=daily_data['Date'], y=daily_data['Successful_Bookings'], 
                      name="Successful Bookings", line=dict(color='green', width=2)),
            secondary_y=False,
        )
        
        # Add revenue on secondary y-axis
        fig_daily.add_trace(
            go.Scatter(x=daily_data['Date'], y=daily_data['Daily_Revenue'], 
                      name="Daily Revenue", line=dict(color='red', dash='dash', width=2)),
            secondary_y=True,
        )
        
        # Update layout
        fig_daily.update_xaxes(title_text="Date")
        fig_daily.update_yaxes(title_text="Number of Bookings", secondary_y=False)
        fig_daily.update_yaxes(title_text="Revenue (₹)", secondary_y=True)
        fig_daily.update_layout(title="Daily Booking and Revenue Trends", height=500)
        
        st.plotly_chart(fig_daily, use_container_width=True)

# Vehicle Type Analysis Page
def show_vehicle_analysis():
    """Display Vehicle Type analysis matching Power BI table design"""
    
    st.markdown('<h1 class="section-title">🚙 Vehicle Type Analysis</h1>', unsafe_allow_html=True)
    
    # Load vehicle summary data (matching the Power BI table structure)
    vehicle_query = """
    SELECT 
        Vehicle_Type,
        ROUND(SUM(Booking_Value), 0) as Total_Booking_Value,
        ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Booking_Value ELSE 0 END), 0) as Success_Booking_Value,
        ROUND(AVG(CASE WHEN Is_Successful = 1 AND Ride_Distance > 0 THEN Ride_Distance END), 1) as Avg_Distance_Travelled,
        ROUND(SUM(CASE WHEN Is_Successful = 1 THEN Ride_Distance ELSE 0 END), 1) as Total_Distance_Travelled,
        COUNT(*) as Total_Bookings,
        SUM(Is_Successful) as Successful_Bookings,
        ROUND(SUM(Is_Successful) * 100.0 / COUNT(*), 2) as Success_Rate
    FROM rides 
    GROUP BY Vehicle_Type
    ORDER BY Success_Booking_Value DESC
    """
    vehicle_summary = load_data(vehicle_query)
    
    if not vehicle_summary.empty:
        # Display the summary table matching Power BI design
        st.markdown("### 📊 Vehicle Performance Summary Table")
        
        # Format the dataframe for better display
        display_df = vehicle_summary.copy()
        display_df['Total_Booking_Value'] = display_df['Total_Booking_Value'].apply(lambda x: f"₹{x:,.0f}")
        display_df['Success_Booking_Value'] = display_df['Success_Booking_Value'].apply(lambda x: f"₹{x:,.0f}")
        display_df['Total_Distance_Travelled'] = display_df['Total_Distance_Travelled'].apply(lambda x: f"{x:,.1f} km")
        display_df['Avg_Distance_Travelled'] = display_df['Avg_Distance_Travelled'].apply(lambda x: f"{x:.1f} km")
        
        # Rename columns for display
        display_df = display_df.rename(columns={
            'Vehicle_Type': 'Vehicle Type',
            'Total_Booking_Value': 'Total Booking Value',
            'Success_Booking_Value': 'Success Booking Value',
            'Avg_Distance_Travelled': 'Avg. Distance Travelled',
            'Total_Distance_Travelled': 'Total Distance Travelled'
        })
        
        # Display only the columns shown in Power BI
        st.dataframe(
            display_df[['Vehicle Type', 'Total Booking Value', 'Success Booking Value', 
                       'Avg. Distance Travelled', 'Total Distance Travelled']],
            use_container_width=True,
            hide_index=True
        )
        
        # Vehicle performance charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Success rate comparison
            fig_success = px.bar(
                vehicle_summary,
                x='Vehicle_Type',
                y='Success_Rate',
                title="Success Rate by Vehicle Type (%)",
                color='Success_Rate',
                color_continuous_scale='RdYlGn',
                text='Success_Rate'
            )
            fig_success.update_traces(texttemplate='%{text}%', textposition='outside')
            fig_success.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig_success, use_container_width=True)
        
        with col2:
            # Revenue comparison
            fig_revenue = px.bar(
                vehicle_summary,
                x='Vehicle_Type',
                y='Success_Booking_Value',
                title="Revenue by Vehicle Type",
                color='Success_Booking_Value',
                color_continuous_scale='Blues'
            )
            fig_revenue.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig_revenue, use_container_width=True)

# Revenue Analysis Page
def show_revenue_analysis():
    """Display Revenue analysis page"""
    
    st.markdown('<h1 class="section-title">💰 Revenue Analysis</h1>', unsafe_allow_html=True)
    
    # Payment method analysis
    payment_query = """
    SELECT 
        Payment_Method,
        COUNT(*) as Total_Transactions,
        ROUND(SUM(Booking_Value), 2) as Total_Revenue,
        ROUND(AVG(Booking_Value), 2) as Avg_Transaction_Value
    FROM rides 
    WHERE Payment_Method IS NOT NULL AND Booking_Status = 'Success'
    GROUP BY Payment_Method
    ORDER BY Total_Revenue DESC
    """
    payment_data = load_data(payment_query)
    
    if not payment_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Revenue by payment method
            fig_payment = px.pie(
                payment_data,
                values='Total_Revenue',
                names='Payment_Method',
                title="Revenue Distribution by Payment Method"
            )
            st.plotly_chart(fig_payment, use_container_width=True)
        
        with col2:
            # Transaction volume
            fig_volume = px.bar(
                payment_data,
                x='Payment_Method',
                y='Total_Transactions',
                title="Transaction Volume by Payment Method",
                color='Total_Transactions',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig_volume, use_container_width=True)
        
        # Payment method summary table
        st.markdown("### 💳 Payment Method Summary")
        payment_display = payment_data.copy()
        payment_display['Total_Revenue'] = payment_display['Total_Revenue'].apply(lambda x: f"₹{x:,.2f}")
        payment_display['Avg_Transaction_Value'] = payment_display['Avg_Transaction_Value'].apply(lambda x: f"₹{x:.2f}")
        st.dataframe(payment_display, use_container_width=True, hide_index=True)

# Cancellation Analysis Page  
def show_cancellation_analysis():
    """Display Cancellation analysis page"""
    
    st.markdown('<h1 class="section-title">🚫 Cancellation Analysis</h1>', unsafe_allow_html=True)
    
    # Overall cancellation metrics
    cancel_query = """
    SELECT 
        COUNT(*) as total_bookings,
        SUM(CASE WHEN Booking_Status LIKE '%Customer%' THEN 1 ELSE 0 END) as customer_cancellations,
        SUM(CASE WHEN Booking_Status LIKE '%Driver%' THEN 1 ELSE 0 END) as driver_cancellations,
        SUM(CASE WHEN Booking_Status = 'Success' THEN 1 ELSE 0 END) as successful_bookings
    FROM rides
    """
    cancel_metrics = load_data(cancel_query).iloc[0]
    
    # Display cancellation metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        customer_rate = (cancel_metrics['customer_cancellations'] / cancel_metrics['total_bookings']) * 100
        st.metric(
            "👤 Customer Cancellations",
            f"{cancel_metrics['customer_cancellations']:,}",
            delta=f"{customer_rate:.1f}% of total"
        )
    
    with col2:
        driver_rate = (cancel_metrics['driver_cancellations'] / cancel_metrics['total_bookings']) * 100
        st.metric(
            "🚗 Driver Cancellations", 
            f"{cancel_metrics['driver_cancellations']:,}",
            delta=f"{driver_rate:.1f}% of total"
        )
    
    with col3:
        success_rate = (cancel_metrics['successful_bookings'] / cancel_metrics['total_bookings']) * 100
        st.metric(
            "✅ Success Rate",
            f"{success_rate:.1f}%",
            delta=f"{cancel_metrics['successful_bookings']:,} rides"
        )
    
    # Cancellation reasons analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Customer cancellation reasons
        customer_reasons_query = """
        SELECT 
            COALESCE(Canceled_Rides_by_Customer, 'Not Specified') as Reason,
            COUNT(*) as Count
        FROM rides 
        WHERE Booking_Status LIKE '%Customer%'
        GROUP BY Canceled_Rides_by_Customer
        ORDER BY Count DESC
        """
        customer_reasons = load_data(customer_reasons_query)
        
        if not customer_reasons.empty:
            fig_customer = px.bar(
                customer_reasons,
                x='Reason',
                y='Count',
                title="Customer Cancellation Reasons",
                color='Count',
                color_continuous_scale='Reds'
            )
            fig_customer.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_customer, use_container_width=True)
    
    with col2:
        # Driver cancellation reasons
        driver_reasons_query = """
        SELECT 
            COALESCE(Canceled_Rides_by_Driver, 'Not Specified') as Reason,
            COUNT(*) as Count
        FROM rides 
        WHERE Booking_Status LIKE '%Driver%'
        GROUP BY Canceled_Rides_by_Driver
        ORDER BY Count DESC
        """
        driver_reasons = load_data(driver_reasons_query)
        
        if not driver_reasons.empty:
            fig_driver = px.bar(
                driver_reasons,
                x='Reason', 
                y='Count',
                title="Driver Cancellation Reasons",
                color='Count',
                color_continuous_scale='Oranges'
            )
            fig_driver.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig_driver, use_container_width=True)

# Ratings Analysis Page
def show_ratings_analysis():
    """Display Ratings analysis page"""
    
    st.markdown('<h1 class="section-title">⭐ Ratings Analysis</h1>', unsafe_allow_html=True)
    
    # Overall rating metrics
    ratings_query = """
    SELECT 
        COUNT(Driver_Ratings) as total_driver_ratings,
        ROUND(AVG(Driver_Ratings), 2) as avg_driver_rating,
        COUNT(Customer_Rating) as total_customer_ratings,
        ROUND(AVG(Customer_Rating), 2) as avg_customer_rating
    FROM rides 
    WHERE Booking_Status = 'Success'
    """
    rating_metrics = load_data(ratings_query).iloc[0]
    
    # Display rating metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric(
            "🚗 Average Driver Rating",
            f"{rating_metrics['avg_driver_rating']:.2f}/5.0",
            delta=f"Based on {rating_metrics['total_driver_ratings']:,} ratings"
        )
    
    with col2:
        st.metric(
            "👤 Average Customer Rating", 
            f"{rating_metrics['avg_customer_rating']:.2f}/5.0",
            delta=f"Based on {rating_metrics['total_customer_ratings']:,} ratings"
        )
    
    # Rating distribution by vehicle type
    vehicle_ratings_query = """
    SELECT 
        Vehicle_Type,
        ROUND(AVG(Driver_Ratings), 2) as Avg_Driver_Rating,
        ROUND(AVG(Customer_Rating), 2) as Avg_Customer_Rating,
        COUNT(Driver_Ratings) as Driver_Rating_Count,
        COUNT(Customer_Rating) as Customer_Rating_Count
    FROM rides 
    WHERE Booking_Status = 'Success'
    GROUP BY Vehicle_Type
    ORDER BY Avg_Driver_Rating DESC
    """
    vehicle_ratings = load_data(vehicle_ratings_query)
    
    if not vehicle_ratings.empty:
        # Rating comparison chart
        fig_ratings = go.Figure()
        
        fig_ratings.add_trace(go.Bar(
            name='Driver Ratings',
            x=vehicle_ratings['Vehicle_Type'],
            y=vehicle_ratings['Avg_Driver_Rating'],
            offsetgroup=1
        ))
        
        fig_ratings.add_trace(go.Bar(
            name='Customer Ratings',
            x=vehicle_ratings['Vehicle_Type'],
            y=vehicle_ratings['Avg_Customer_Rating'],
            offsetgroup=2
        ))
        
        fig_ratings.update_layout(
            title='Average Ratings Comparison by Vehicle Type',
            xaxis_tickangle=-45,
            barmode='group',
            yaxis=dict(title='Rating (1-5 scale)'),
            height=500
        )
        
        st.plotly_chart(fig_ratings, use_container_width=True)
        
        # Ratings summary table
        st.markdown("### 📊 Ratings Summary by Vehicle Type")
        ratings_display = vehicle_ratings.copy()
        st.dataframe(ratings_display, use_container_width=True, hide_index=True)

# Main application logic
def main():
    """Main application function with navigation"""
    
    # Get selected page from sidebar
    selected_page = create_sidebar()
    
    # Display selected page content
    if selected_page == "overall":
        show_overall_dashboard()
    elif selected_page == "vehicle":
        show_vehicle_analysis()
    elif selected_page == "revenue":
        show_revenue_analysis()
    elif selected_page == "cancellation":
        show_cancellation_analysis()
    elif selected_page == "ratings":
        show_ratings_analysis()
    
    # Footer
    st.markdown("---")
    st.markdown(
        "<div style='text-align: center; color: #888; padding: 20px;'>"
        "<b>Ola Ride Insights Dashboard</b> | Built with Streamlit & SQLite | "
        "Data Analytics Project"
        "</div>",
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()