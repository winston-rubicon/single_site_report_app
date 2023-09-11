###----- Streamlit app specific imports
import streamlit as st
import os

###----- Imports needed for generating plots, etc. 
from report_functions import report_functions
from report_functions import sql_queries

import pandas as pd
import pyspark.sql.functions as F
import pyspark
from pyspark.sql import SparkSession
from databricks import sql

# Title for App
st.title('Wash Index Single Site Quarterly Report')

# Get hub_id, site_id, and access token
hub_id = st.number_input('Enter Hub ID:', min_value=10000, key=1)
site_id = st.number_input('Enter Site ID:', min_value=1, key=2)
access_token = st.text_input('Enter Databricks access token:')


###----- Need to set up for the queries here
# Store access token in environment
os.environ["DATABRICKS_TOKEN"] = access_token

# Start spark session
spark = SparkSession.builder \
    .appName("Databricks App") \
    .getOrCreate()

# Setting up for queries
server_hostname = "dbc-c3859ab4-d7b8.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/362bb606bc6f1cdc" 

os.environ["DATABRICKS_SQL_SERVER_HOSTNAME"] = server_hostname
os.environ["DATABRICKS_HTTP_PATH"] = http_path

spark.conf.set("spark.databricks.service.server.enabled", "true")
spark.conf.set("spark.databricks.service.server.hostname", os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"))
spark.conf.set("spark.databricks.service.http.path", os.getenv("DATABRICKS_HTTP_PATH"))
spark.conf.set("spark.databricks.service.token", os.getenv("DATABRICKS_TOKEN"))
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# Your start and end year, month
# TODO: How to implement these programmatically? User input? Current date and do previous quarter?
start_year, start_month = 2022, 4
end_year, end_month = 2023, 6
query_year = start_year-1

# Current quarter
quarter = 2

if access_token:
    ###------ Query database
    full_df = sql_queries.gsr_report_query(spark, hub_id, site_id)


    ###----- Plotting/Displaying
    # Revenue Per Car
    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    df = full_df[
        ((full_df['date'] >= pd.to_datetime(f'{start_year}-{start_month}-01')) & 
        (full_df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')))
    ]

    # define col, ylabel
    col = 'revenue_per_car'
    ylabel = 'Revenue Per Car ($)'

    # plot
    ax = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    st.pyplot(ax.get_figure())

    # # Quarterly site analysis
    # quarterly_df = site_qoq(df=full_df.copy(), col=[col])

    # # Average across all sites
    # quarterly_avg_df = avg_qoq(df=full_df.copy(), col=[col])

    # YoY RPC
    # Calculate YoY% change
    df = full_df.copy()
    df = report_functions.yoy_monthly(df=df, col=col)

    # define ylabel, col
    ylabel = 'YoY Change (%)'
    yoy_col = f"yoy_{col}"

    # Plot
    ax = report_functions.line_plot(df=df, col=yoy_col, ylabel=ylabel)
    st.pyplot(ax.get_figure())



if st.button('Get Retail Package Distribution'):
    # Query for package breakdown
    full_package_df = sql_queries.retail_package_query(hub_id, site_id)

    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    df = full_package_df[
        ((full_package_df['date'] >= pd.to_datetime(f'{start_year}-{start_month}-01')) & 
        (full_package_df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')))
    ]
    # List of words to filter on
    words = ['bronze','silver','gold','platinum']
    # Column of interest
    col = 'car_counts'
    # Define a percentage threshold for 'Other' category, e.g., 1%
    threshold = 0
    # Define title of plot
    title = 'Retail Package\nDistribution'
    # Plot
    fig = report_functions.package_distribution_plot(df=df, words=words, col=col, title=title)
    st.pyplot(fig)
    # qoq_df = qoq_package(df=df,col=col)
    # Retail package distribution over time
    fig = report_functions.monthly_package_distribution_plot(df=df, words=words, col=col)
    st.pyplot(fig)


if st.button('Get Membership Package Distribution'):
    # Query to get membership package breakdown
    df = sql_queries.membership_package_query(hub_id, site_id)

    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    df = df[
        ((df['date'] >= pd.to_datetime(f'{start_year}-{start_month}-01')) & 
        (df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')))
    ]
    # List of words to filter on
    words = ['bronze','silver','gold','platinum']
    # Define a percentage threshold for 'Other' category, e.g., 1%
    threshold = 5
    # Define title of plot
    title = 'Active Membership \nDistribution'
    # Column of interest
    col = 'active_arms'
    # Plot
    fig = report_functions.package_distribution_plot(df=df, words=words, col=col, title=title, threshold=threshold)
    st.pyplot(fig)
    # qoq_df = qoq_package(df=df,col=col,threshold=threshold)
    # Membership change in wash package distribution over time
    fig = report_functions.monthly_package_distribution_plot(df=df, words=words, col=col, threshold=threshold)
    st.pyplot(fig)
