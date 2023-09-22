###----- Imports
import streamlit as st
import os
from report_functions import report_functions, sql_queries
import reportlab_example as rl

# from pdf_generators.pdf_generator import internal_page, package_page
# from pdf_generators.pdf_compiler import save_pdf
import pandas as pd
from pyspark.sql import SparkSession
from io import BytesIO
import matplotlib.pyplot as plt

# Title for App
st.title("Wash Index Single Site Quarterly Report")

# Get hub_id, site_id, and access token
hub_id = st.number_input("Enter Hub ID:", min_value=10000, key=1)
site_id = st.number_input("Enter Site ID:", min_value=1, key=2)
access_token = st.text_input("Enter Databricks access token:")


###----- Need to set up for the queries here
# Store access token in environment
os.environ["DATABRICKS_TOKEN"] = access_token

# Start spark session
spark = SparkSession.builder.appName("Databricks App").getOrCreate()

# Setting up for queries
server_hostname = "dbc-c3859ab4-d7b8.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/362bb606bc6f1cdc"

os.environ["DATABRICKS_SQL_SERVER_HOSTNAME"] = server_hostname
os.environ["DATABRICKS_HTTP_PATH"] = http_path

spark.conf.set("spark.databricks.service.server.enabled", "true")
spark.conf.set(
    "spark.databricks.service.server.hostname",
    os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"),
)
spark.conf.set("spark.databricks.service.http.path", os.getenv("DATABRICKS_HTTP_PATH"))
spark.conf.set("spark.databricks.service.token", os.getenv("DATABRICKS_TOKEN"))
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# Your start and end year, month
# TODO: How to implement these programmatically? User input? Current date and do previous quarter?
start_year, start_month = 2022, 4
end_year, end_month = 2023, 6
query_year = start_year - 1

# Current quarter
quarter = 2

plots_for_pdf = {}


def save_plot(fig):
    """Saving plots generated in app to buffers in order to make pdf without saving files"""
    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=300)
    buffer.seek(0)
    return buffer


if access_token:
    ###------ Query database
    full_df = sql_queries.gsr_report_query(spark, hub_id, site_id)
    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    df = full_df[
        (
            (full_df["date"] >= pd.to_datetime(f"{start_year}-{start_month}-01"))
            & (full_df["date"] <= pd.to_datetime(f"{end_year}-{end_month}-01"))
        )
    ]

    ###----- Plotting/Displaying

    ### Wash Counts
    # Define column, ylabel to be used for plot
    col = "total_wash_counts"
    ylabel = "Number of Washes"
    # Call function to plot data
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    st.pyplot(fig)
    wash_counts_fig = save_plot(fig)
    plots_for_pdf["total_wash_counts"] = wash_counts_fig

    ### Revenue Per Car
    # define col, ylabel
    col = "revenue_per_car"
    ylabel = "Revenue Per Car ($)"
    # plot
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    st.pyplot(fig)
    rpc_fig = save_plot(fig)
    plots_for_pdf["revenue_per_car"] = rpc_fig

    ### Retail vs. Membership Sales
    fig = report_functions.bar_plot(df)
    retail_memberships_plot = save_plot(fig)
    plots_for_pdf["retail_membership_distribution"] = retail_memberships_plot

    ### Membership RPC
    # define col, ylabel
    col = "arm_average_ticket"
    ylabel = "Average Revenue ($)"
    # # If threshold necessary, include it here
    # threshold = 40
    # df = df[df[col]<threshold]
    # plot
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    membership_rpc_plot = save_plot(fig)
    plots_for_pdf["membership_rpc"] = membership_rpc_plot

    ### Retail RPC
    # define col, ylabel
    col = "retail_ticket_average"
    ylabel = "Average Revenue ($)"
    # # If threshold necessary, include it here
    # threshold = 6
    # df = df[df[col]>threshold]
    # plot
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    retail_rpc_plot = save_plot(fig)
    plots_for_pdf["retail_rpc"] = retail_rpc_plot

    ### Churn Rate
    # define col, ylabel
    col = "churn_rate"
    ylabel = "Churn %"
    # If threshold necessary, include it here
    # threshold = 40
    # df = df[df[col]<threshold]
    # plot
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    churn_plot = save_plot(fig)
    plots_for_pdf["churn_rate"] = churn_plot

    ### Capture Rate
    # define col, ylabel
    col = "capture_rate"
    ylabel = "Capture %"
    # If threshold necessary, include it here
    # threshold = 40
    # df = df[df[col]<threshold]
    # plot
    fig = report_functions.line_plot(df=df, col=col, ylabel=ylabel)
    capture_plot = save_plot(fig)
    plots_for_pdf["capture_rate"] = capture_plot
    st.pyplot(fig)

    ### Popular Days
    days_df = sql_queries.popular_days_query(hub_id)
    fig = report_functions.popular_days(days_df)
    popular_days_plot = save_plot(fig)
    plots_for_pdf["popular_days"] = popular_days_plot
    st.pyplot(fig)

    ### Popular Hours
    hours_df = sql_queries.popular_hours_query(hub_id)
    fig = report_functions.popular_hours(hours_df)
    popular_hours_plot = save_plot(fig)
    plots_for_pdf["popular_hours"] = popular_hours_plot
    st.pyplot(fig)

    ### Optimal Weather Days
    weather_df = sql_queries.optimal_weather_days_query(hub_id)
    fig = report_functions.optimal_weather_days(weather_df)
    optimal_weather_days_plot = save_plot(fig)
    plots_for_pdf["optimal_weather_days"] = optimal_weather_days_plot
    st.pyplot(fig)

    ### Washes Per Optimal Weather day
    fig = report_functions.washes_per_optimal_day(weather_df)
    washes_per_optimal_day_plot = save_plot(fig)
    plots_for_pdf["washes_per_optimal_day"] = washes_per_optimal_day_plot
    st.pyplot(fig)

# if st.button("Get Retail Package Distribution"):
    # Query for package breakdown
    full_package_df = sql_queries.retail_package_query(hub_id, site_id)
    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    package_df = full_package_df[
        (full_package_df["date"] >= pd.to_datetime(f"{start_year}-{start_month}-01"))
        & (full_package_df["date"] <= pd.to_datetime(f"{end_year}-{end_month}-01"))
    ]
    # List of words to filter on
    # TODO: This needs to either be in the hub dictionary or determiend programmatically (or both)
    words = ["bronze", "silver", "gold", "platinum"]
    # Column of interest
    col = "car_counts"
    # Define a percentage threshold for 'Other' category, e.g., 1%
    threshold = 0
    # Define title of plot
    title = "Retail Package\nDistribution"
    # Plot
    fig = report_functions.package_distribution_plot(
        df=package_df, words=words, col=col, title=title
    )
    st.pyplot(fig)
    retail_package_plot = save_plot(fig)
    plots_for_pdf["retail_package_distribution"] = retail_package_plot
    # qoq_df = qoq_package(df=df,col=col)
    # Retail package distribution over time
    fig = report_functions.monthly_package_distribution_plot(
        df=package_df, words=words, col=col
    )
    st.pyplot(fig)
    retail_monthly_package_plot = save_plot(fig)
    plots_for_pdf["retail_monthly_package_distribution"] = retail_monthly_package_plot


# if st.button("Get Membership Package Distribution"):
    # Query to get membership package breakdown
    df = sql_queries.membership_package_query(hub_id, site_id)
    # Copy df to avoid issues in overwriting it, make sure to only get desired dates
    package_df = df[
        (df["date"] >= pd.to_datetime(f"{start_year}-{start_month}-01"))
        & (df["date"] <= pd.to_datetime(f"{end_year}-{end_month}-01"))
    ]
    # List of words to filter on
    # TODO: This needs to either be in the hub dictionary or determiend programmatically (or both)
    words = ["bronze", "silver", "gold", "platinum"]
    # Define a percentage threshold for 'Other' category, e.g., 1%
    threshold = 5
    # Define title of plot
    title = "Active Membership \nDistribution"
    # Column of interest
    col = "active_arms"
    # Plot
    fig = report_functions.package_distribution_plot(
        df=package_df, words=words, col=col, title=title, threshold=threshold
    )
    st.pyplot(fig)
    membership_package_plot = save_plot(fig)
    plots_for_pdf["membership_package_distribution"] = membership_package_plot
    # qoq_df = qoq_package(df=df,col=col,threshold=threshold)
    # Membership change in wash package distribution over time
    fig = report_functions.monthly_package_distribution_plot(
        df=package_df, words=words, col=col, threshold=threshold
    )
    st.pyplot(fig)
    membership_monthly_package_plot = save_plot(fig)
    plots_for_pdf[
        "membership_monthly_package_distribution"
    ] = membership_monthly_package_plot


if st.button("Generate Report PDF"):
    # internal_page()
    # package_page()
    # pdf_file = save_pdf()
    # with open(pdf_file, "rb") as f:
    #     pdf_bytes = f.read()
    # st.download_button(
    #     label="Download Report PDF",
    #     data=pdf_bytes,
    #     file_name=f"Quarterly_Report_Hub_{hub_id}_Site_{site_id}.pdf",
    #     mime="application/pdf",
    # )

    rl.PDFPSReporte(plots_for_pdf)

    pass
