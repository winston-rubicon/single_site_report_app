###----- Streamlit app specific imports
import streamlit as st
from databricks import sql
import os
from pyspark.sql import SparkSession

# Title for App
st.title('Wash Index Single Site Quarterly Report')

# Get hub_id, site_id, and access token
hub_id = st.number_input('Enter Hub ID:', min_value=10000)
site_id = st.number_input('Enter Site ID:', min_value=1)
access_token = st.text_input('Enter Databricks access token:')

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

###----- Imports needed for generating plots, etc. 
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import calendar
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import numpy as np
from collections import OrderedDict
from pyspark.sql.functions import concat_ws
import pyspark.sql.functions as F
import pyspark
import datetime as dt
import matplotlib.ticker as ticker

plt.rcParams['font.family'] = 'Atlas Grotesk'

# Defining dictionary for package_rpt codes, etc. associated with a hub_id
hub_dict = {
        10007: {
                'package_rpt': (103958, 1000002),
                'bls_division': 'East North Central',
                'state': 'Ohio'
                },
}


# Define color palette, this will change at package distribution plots!
color_palette = ['#003264', '#0b75e1', '#87cefa', '#ffcb00', '#e7af50', '#c70039', '#800080', '#006400', '#8b4513', '#ffb6c1', '#e6e6fa', '#00A000']

# Your start and end year, month
# TODO: How to implement these programmatically? User input? Current date and do previous quarter?
start_year, start_month = 2022, 4
end_year, end_month = 2023, 6
query_year = start_year-1

# Current quarter
quarter = 2


###----- Define commonly used functions
# Line Plot
def line_plot(df, col, ylabel):
    '''
    Plotting a given column's data for all sites on a single plot, as a function of date. Assumes that the DataFrame df will need to have a groupby in date and hub site performed on it, and is a copy of the main DataFrame used throughout the notebook. Returns None, just plots the data.
    '''
    # Attempt to set the font
    plt.rcParams['font.family'] = 'Atlas Grotesk'

    # Groupby desired parameters (date, site) and plot the data you want (total_wash_counts in this case)
    ax = df.groupby(['date','hub_site'])[col].first().unstack().plot(kind='line', color=color_palette, figsize=(10, 6))

    # ax.set_title('Total Wash Counts By Site', color='#003264', fontsize=24, y=1.1)
    ax.set_ylabel(ylabel, color='#0b75e1', fontsize=14)
    ax.set_xlabel(None)

    ax.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='MS')
    ax.set_xticks(date_range)
    ax.set_xticklabels([dt.strftime('%b %Y') if dt.month == 1 else dt.strftime('%b') for dt in date_range])

    # Add gray border and vertical lines
    ax.spines['top'].set_color('lightgray'), ax.spines['right'].set_color('lightgray'), ax.spines['bottom'].set_color('lightgray'), ax.spines['left'].set_color('lightgray')
    for date in date_range:
        ax.axvline(date, color='lightgray', linestyle='-', linewidth=1)

    plt.yticks(fontsize=14)
    plt.xticks(rotation=45, fontsize=14)
    legend = plt.legend(title='Site', bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontsize(14)  # Set the font size of the legend title
    plt.tight_layout()
    st.pyplot(plt)
    plt.savefig(col, dpi=300)

    return None

# Bar Plot
def bar_plot(df, col=None, ylabel=None):
    '''
    Calculates the fraction of retail and membership sales in the current quarter, and plots the monthly progression over the past year. 
    '''

    # Print out which hub_sites are present (either going to be all or one)
    print(f"Sites included: {np.sort(df['hub_site'].unique())}")

    # Get the fraction of retail and membership sales in current quarter
    retail_counts = df[(df['quarter']==quarter) & (df['year']==end_year)]['retail_counts'].sum()
    arms_redeemed = df[(df['quarter']==quarter) & (df['year']==end_year)]['arms_redeemed'].sum()

    retail_percentage = round((retail_counts / (retail_counts + arms_redeemed)) * 100)
    redeemed_percentage = round((arms_redeemed / (retail_counts + arms_redeemed)) * 100)

    # Print result
    print(f"Percentage of Retail Counts in Q{quarter} {end_year}: {retail_percentage}%")
    print(f"Percentage of Arms Redeemed in Q{quarter} {end_year}: {redeemed_percentage}%")

    # Begin plotting work
    # Group data by month and year, then calculate sum of 'retail_counts' and 'arms_redeemed'
    grouped_data = df.groupby(['year', 'month'])[['retail_counts', 'arms_redeemed']].sum().reset_index()

    # Replace the numerical months with abbreviated month names and concatenate it with year
    grouped_data['month'] = grouped_data.apply(lambda row: f"{calendar.month_abbr[int(row['month'])]} {int(row['year'])}", axis=1)

    # Set month as index (for better x-axis labels)
    grouped_data.set_index('month', inplace=True)

    # Generate bar plot
    ax = grouped_data[['retail_counts', 'arms_redeemed']].plot(kind='bar', figsize=(10, 6), width=0.8, color=['#0b75e1', '#003264'])

    # ax.set_title('Retail & Membership Wash Counts Across All Sites', color='#003264', fontsize=22, y=1.1, x=0.47, fontweight='bold')
    ax.set_ylabel('Wash Count', color='#0b75e1', fontsize=14)
    plt.xlabel(None)

    ax.set_frame_on(False)
    plt.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')
    ax.tick_params(left=False, bottom=False, colors='#525661', labelsize=14)

    plt.subplots_adjust(right=0.85)
    labels = ['Retail', 'Membership']
    ax.legend(labels, loc='center left', bbox_to_anchor=(1.02, 0.5), fontsize=14)

    # Modify x-axis tick labels to include only the year on January
    x_labels = [label.split()[0] if label.split()[0] != 'Jan' else label for label in grouped_data.index]
    ax.set_xticklabels(x_labels, rotation=45, fontsize=14)

    st.pyplot(plt)

    return None

# Package Distribution Plots
def package_distribution_plot(df, words, col, title, threshold=0):
    # Defining color palette
    palette = ['#003264', '#0b75e1', '#87cefa', '#ffcb00', '#e7af50', '#c70039']

    # Generate a mapping of words to a priority order based on word list
    word_to_priority = {word: i for i, word in enumerate(words)}

    # Create a new column in the DataFrame that assigns the priority to each name
    df['priority'] = df['name'].apply(lambda name: min([word_to_priority[word] for word in words if word in name.lower()], default=len(words)))

    # Then group by 'priority' and 'name' and calculate the sum of col for each group
    df_grouped = df[(df['quarter']==quarter) & (df['year']==end_year)].groupby(['priority', 'name'])[col].sum()
    
    # Normalize the col to get the percentage for each 'name'
    df_grouped = df_grouped / df_grouped.sum() * 100

    # If percentage is less than threshold, replace the 'name' with 'Other'
    df_grouped.index = pd.MultiIndex.from_tuples([(i if p > threshold else len(words), n if p > threshold else 'Other') for ((i, n), p) in df_grouped.items()])

    # Regroup the data to sum the col of 'Other'
    df_grouped = df_grouped.groupby(level=[0, 1]).sum()

    # Now you can plot your pie chart as a donut plot
    fig, ax = plt.subplots()
    p_labels = ['{:.1f}%'.format(i) for i in df_grouped]
    patches, texts = ax.pie(df_grouped, labels=p_labels, colors=palette)

    # Draw a white circle at the center to create the "donut hole" effect
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig.gca().add_artist(centre_circle)

    # Set the title at the center
    label = ax.annotate(title, color='#003264', fontsize=12, ha='center', va='center', xy=(0, 0), weight='bold')

    # Add a legend
    legend = ax.legend(patches, labels=[name for _, name in df_grouped.index], 
                       loc="lower center", bbox_to_anchor=(0.5, -0.25), 
                       title=None, title_fontsize=12, prop={'family': 'Atlas Grotesk'})

    legend.get_frame().set_facecolor('#f5f5f5')
    legend.get_frame().set_linewidth(0)

    for text in texts:
        text.set_color('#003264')
        text.set_weight('bold')
    plt.tight_layout()
    st.pyplot(plt)
    plt.savefig(col, dpi=300)
    return df

def monthly_package_distribution_plot(df, words, col=None, threshold=0):

    # Defining color palette
    palette = ['#003264', '#0b75e1', '#87cefa', '#ffcb00', '#e7af50', '#c70039']

    # Filter the data based on the names containing the words
    df_filtered = df[df['name'].str.contains('|'.join(words), case=False)]

    # Generate a mapping of words to a priority order based on your list
    word_to_priority = {word: i for i, word in enumerate(words)}

    # Assign priority to names based on the highest priority word they contain
    df_filtered['priority'] = df_filtered['name'].apply(
        lambda name: min([word_to_priority[word] for word in words if word in name.lower()], default=len(words)))

    # Sort the DataFrame by the priority column
    df_filtered = df_filtered.sort_values('priority')

    # Group by date and name
    grouped = df_filtered.groupby(['date', 'name']).sum().reset_index()

    # Calculate total car_count for each month
    total_monthly_count = grouped.groupby('date')[col].sum()

    # Divide the car_count of each name type by the total count of the month to get the distribution
    grouped['distribution'] = grouped.apply(lambda row: (row[col] / total_monthly_count[row['date']]) * 100, axis=1)

    # Exclude 'name' types with distribution below the threshold in the given time range
    grouped = grouped[grouped['distribution'] > threshold]

    # Exclude 'name' types with less than 2 unique months of data
    grouped = grouped[grouped.groupby('name')['date'].transform('nunique') > 1]

    # Get the order of names based on word priority
    name_order = grouped.groupby('name')['priority'].min().sort_values().index

    # Plotting the distribution over time

    # Unstack and drop NaNs
    grouped_unstacked = grouped.groupby(['date', 'name'])['distribution'].first().unstack().dropna(axis=1, how='any')

    # Reorder the columns based on name_order
    grouped_unstacked = grouped_unstacked[name_order]

    ax = grouped_unstacked.plot(kind='line', color=palette, figsize=(10, 6))

    ax.set_ylabel('Distribution (%)', color='#0b75e1', fontsize=14)
    ax.set_xlabel(None)

    ax.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(start=grouped_unstacked.index.min(), end=grouped_unstacked.index.max(), freq='MS')
    ax.set_xticks(date_range)
    ax.set_xticklabels([dt.strftime('%b %Y') if dt.month == 1 else dt.strftime('%b') for dt in date_range])

    # Set the x-axis limits to exclude ticks before the first month and after the last month
    ax.set_xlim(date_range[0], date_range[-1])

    # Add gray border and vertical lines
    ax.spines['top'].set_color('lightgray')
    ax.spines['right'].set_color('lightgray')
    ax.spines['bottom'].set_color('lightgray')
    ax.spines['left'].set_color('lightgray')

    for date in date_range:
        ax.axvline(date, color='lightgray', linestyle='-', linewidth=1)

    plt.yticks(fontsize=14)
    plt.xticks(rotation=45, fontsize=14)
    plt.legend(name_order, bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    plt.tight_layout()
    st.pyplot(plt)
    plt.savefig(col+'over_time', dpi=300)
    return None

# Econ Plots
def econ_plot(nat_df, div_df, nat_y, div_y, ylabel, div_label):
    # Plotting
    ax = nat_df.plot(x='date', y=nat_y, kind='line', color=color_palette, figsize=(10, 6),label='National')
    div_df.plot(x='date', y=div_y, ax=ax, label=div_label)

    ax.set_ylabel(ylabel, color='#0b75e1', fontsize=14)
    ax.set_xlabel(None)

    ax.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')

    ax.set_xlim(div_df['date'].min(), div_df['date'].max())

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(start=div_df['date'].min(), end=div_df['date'].max(), freq='MS')
    ax.set_xticks(date_range)
    ax.set_xticklabels([dt.strftime('%b %Y') if dt.month == 1 else dt.strftime('%b') for dt in date_range])

    # Add gray border and vertical lines
    ax.spines['top'].set_color('lightgray'), ax.spines['right'].set_color('lightgray'), ax.spines['bottom'].set_color('lightgray'), ax.spines['left'].set_color('lightgray')
    for date in date_range:
        ax.axvline(date, color='lightgray', linestyle='-', linewidth=1)

    plt.yticks(fontsize=14)
    plt.xticks(rotation=45, fontsize=14)
    legend = plt.legend(bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontsize(14)  # Set the font size of the legend title

    st.pyplot(plt)

    return None

# YoY Monthly
def yoy_monthly(df, col):
    '''
    Updates the given DataFrame with a column of YoY percent changes based on the given DataFrame (df) and column (col). Assumes that the data is in monthly form so that a month to month comparison is given for YoY. Returns the passed DataFrame.
    '''
    
    df[f"yoy_{col}"] = df.groupby('hub_site')[col].pct_change(periods=12) * 100
    df.reset_index(inplace=True, drop=True)

    #drop sites with no values
    # df.dropna(subset=[f"yoy_{col}"], inplace=True)

    return df[
    ((df['date'] >= pd.to_datetime(f'{start_year}-{start_month}-01')) & 
    (df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')))
    ]

# Site QoQ
def site_qoq(df, col: list, average=True):
    '''
    Displays and returns the DataFrame containing the desired Quarter over Quarter (QoQ) percent change, as indicated by col. col MUST BE a list in order for the function to work! This allows for multiple columns to be chosen at a time. Analysis done at the hub site level.
    '''
    if average:
        # Calculate average for each quarter
        quarterly_df = df.groupby(['hub_site', 'year', 'quarter'])[col].mean().reset_index()
    
    else:
        quarterly_df = df.groupby(['hub_site', 'year', 'quarter'])[col].sum().reset_index()
   
    for col in col:
        # Calculate QoQ change

        quarterly_df['qoq_'+col] = quarterly_df.groupby('hub_site')[col].pct_change()*100

        # Calculate quarterly YoY
        quarterly_df['yoy_quarterly_'+col] = quarterly_df.groupby('hub_site')[col].pct_change(periods=4)*100

    # Only keep relevant quarter's data
    quarterly_df = quarterly_df[(quarterly_df['quarter']==quarter) & (quarterly_df['year']==end_year)]

    # Display df
    # display(quarterly_df)

    # Return df
    return(quarterly_df)

# Average QoQ
def avg_qoq(df, col: list, average=True):
    '''
    Displays and returns the DataFrame containing the desired Quarter over Quarter (QoQ) percent change, as indicated by col. col MUST BE a list in order for the function to work! This allows for multiple columns to be chosen at a time. Analysis done with average value across all sites.
    '''
    if average:
        # Calculate average for each quarter
        quarterly_df = df.groupby(['year', 'quarter'])[col].mean().reset_index()
    
    else:
        quarterly_df = df.groupby(['year', 'quarter'])[col].sum().reset_index()
   
    for col in col:
        # Calculate QoQ change

        quarterly_df['qoq_'+col] = quarterly_df[col].pct_change()*100

        # Calculate quarterly YoY
        quarterly_df['yoy_quarterly_'+col] = quarterly_df[col].pct_change(periods=4)*100

    # Only keep relevant quarter's data
    quarterly_df = quarterly_df[(quarterly_df['quarter']==quarter) & (quarterly_df['year']==end_year)]

    # Display df
    # display(quarterly_df)

    # Return df
    return(quarterly_df)

# Package QoQ
def qoq_package(df, col=None, threshold=0):
    # Group by date and name
    grouped = df.groupby(['year', 'quarter', 'name']).sum().reset_index()

    # Calculate total car_count for each month
    total_quarterly_count = grouped.groupby(['year', 'quarter'])[col].transform('sum')

    grouped['distribution'] = grouped[col]/total_quarterly_count

    # Exclude 'name' types with distribution below the threshold in the given time range
    grouped = grouped[grouped['distribution'] > threshold/100]

    # Calculate the quarterly increase in distributions for each name
    grouped['qoq'] = grouped.groupby('name')['distribution'].pct_change() * 100
    grouped['qoq_yoy'] = grouped.groupby('name')['distribution'].pct_change(periods=4)*100

    # Display only current quarter
    # display(grouped[(grouped['quarter']==quarter) & (grouped['year']==end_year)])

    # Return all data
    return grouped




if access_token:

    ###----- Hub Membership Query to pull all "GSR Report" section values
    # Getting the amounts and counts for each category
    query1 = f"""
            SELECT 
                {hub_id} as hub_id,
                sale.site,
                item.reportcategory as report_id,
                rpt.name as report_name,
                MONTH(sale.logdate) as month,
                YEAR(sale.logdate) as year,
                sum(si.amt) as total_amount,
                count(sale.objid) as total_count

            FROM  ncs_index_dev.hub_{hub_id}.sale as sale
                LEFT JOIN ncs_index_dev.hub_{hub_id}.saleitems as si
                    on sale.SITE = si.SITE
                    AND sale.OBJID = si.SALEID
                LEFT JOIN ncs_index_dev.hub_{hub_id}.item
                    on si.ITEM = item.OBJID
                LEFT JOIN ncs_index_dev.hub_{hub_id}.itemrptcategory rpt
                    ON item.reportcategory = rpt.objid
            
                WHERE MONTH(sale.logdate) IS NOT NULL
                    AND YEAR(sale.logdate) IS NOT NULL
                    AND YEAR(sale.logdate) >= {query_year}
                    
            GROUP BY
                hub_id,
                sale.site,
                item.reportcategory,
                rpt.name,
                MONTH(sale.logdate),
                YEAR(sale.logdate)
            ORDER BY
                site,
                year,
                month,
                report_name
        """

    query2 = f"""
            SELECT 
                {hub_id} AS hub_id,
                sp.SITE as hub_site,
                sites.site_id, 
                MONTH(sale.LOGDATE) AS MONTH,
                YEAR(sale.LOGDATE) AS YEAR,
                sp.PLANTYPE,
                plan.NAME,
                COUNT(CASE WHEN sp.STATUS = 0 THEN 1 END) AS `redeemed`,
                COUNT(DISTINCT CASE WHEN sp.STATUS = 0 THEN sale.customercode END) AS `redeem_customer_count`,
                SUM(CASE WHEN sp.STATUS = 0 THEN sp.AMOUNT ELSE 0 END) AS redeemed_amt,
                COUNT(CASE WHEN sp.STATUS = 1 THEN 1 END) AS `joined`,
                SUM(CASE WHEN sp.STATUS = 1 THEN sp.AMOUNT ELSE 0 END) AS joined_amt,
                COUNT(CASE WHEN sp.STATUS = 2 THEN 1 END) AS `transfer_in`,
                SUM(CASE WHEN sp.STATUS = 2 THEN sp.AMOUNT ELSE 0 END) AS transfer_in_amt,
                COUNT(CASE WHEN sp.STATUS = 3 THEN 1 END) AS `renewed`,
                SUM(CASE WHEN sp.STATUS = 3 THEN sp.AMOUNT ELSE 0 END) AS renewed_amt,
                COUNT(CASE WHEN sp.STATUS = 6 THEN 1 END) AS `resumed`,
                COUNT(CASE WHEN sp.STATUS = 7 THEN 1 END) AS `discontinuing`,
                COUNT(CASE WHEN sp.STATUS = 21 THEN 1 END) AS `plan_expired`,
                COUNT(CASE WHEN sp.STATUS = 27 THEN 1 END) AS `discontinued`,
                COUNT(CASE WHEN sp.STATUS = 29 THEN 1 END) AS `terminated`,
                SUM(CASE WHEN sp.STATUS = 29 THEN sp.AMOUNT ELSE 0 END) AS terminated_amt,
                COUNT(CASE WHEN sp.STATUS = 30 THEN 1 END) AS `transfer_out`,
                SUM(CASE WHEN sp.STATUS = 30 THEN sp.AMOUNT ELSE 0 END) AS transfer_out_amt,
                COUNT(CASE WHEN sp.STATUS = 40 THEN 1 END) AS `suspended`,
                COUNT(CASE WHEN sp.STATUS = 110 THEN 1 END) AS `card_expired`,
                COUNT(CASE WHEN sp.STATUS = 120 THEN 1 END) AS `card_declined`,
                COUNT(CASE WHEN sp.STATUS = 130 THEN 1 END) AS `recharge_problem`,
                COUNT(CASE WHEN sp.STATUS = 150 THEN 1 END) AS `card_approved`,
                COUNT(CASE WHEN sp.STATUS = 200 THEN 1 END) AS `credit_card_changed`,
                COUNT(CASE WHEN sp.STATUS = 201 THEN 1 END) AS `join_date_changed`,
                COUNT(DISTINCT CASE WHEN sp.STATUS IN (7, 29, 30) THEN sale.objid END) AS `distinct_terminated_discontinue_transfer_out`,
                COUNT(DISTINCT CASE WHEN sp.STATUS IN (7, 29) THEN sale.objid END) AS `distinct_terminated_discontinue`
                
            FROM ncs_index_dev.hub_{hub_id}.salepasses AS sp
            JOIN ncs_index_dev.data_hub.sites AS sites
                ON sites.hub_id = {hub_id}
                AND sp.SITE = sites.hub_site
            LEFT JOIN ncs_index_dev.hub_{hub_id}.plantype AS plan
                ON sp.PLANTYPE = plan.OBJID
            LEFT JOIN ncs_index_dev.hub_{hub_id}.sale AS sale
                ON sp.SALEID = sale.OBJID

            WHERE YEAR(sale.logdate) >= {query_year}

            GROUP BY
                sp.SITE,
                MONTH(sale.LOGDATE),
                YEAR(sale.LOGDATE),
                sp.PLANTYPE,
                plan.NAME,
                sites.site_id
            ORDER BY
                sp.SITE,
                YEAR(sale.LOGDATE),
                MONTH(sale.LOGDATE),
                sp.PLANTYPE,
                plan.NAME
        """


    # Need to use the cursor method to get data from the cluster
    # This creates an 'arrow table', so convert that to a spark 
    #DataFrame to be able to create a temp view to query
    with sql.connect(server_hostname=os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"),
                 http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token=os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query1)
            data = cursor.fetchall_arrow()
            categories_df = data.to_pandas()
            categories_df = spark.createDataFrame(categories_df)
            cursor.close()
        connection.close()

    pivot_report_names = ['ARM Headers', 
                          'ARM Plans Recharged',
                          'ARM Plan Extra Rdmd', 
                          'ARM Plans Redeemed',
                          'ARM Plans Sold',
                          'ARM Plans Terminated',
                          'Cash',
                          'Credit Card',
                          'Free Washes Redeemed',
                          'House Accounts',
                          'Paidouts',
                          'Prepaid Redeemed',
                          'Prepaid Sold',
                          'Wash Extras',
                          'Wash LPM Discounts',
                          'Wash Misc.',
                          'Wash Packages',
                          'Express Basics',
                          'Express Pkgs',
                        #   'Full Serve Basics',
                        #   'Full Serve Pkgs'
                          ] 

    # Filter the DataFrame based on the report_name
    categories_df = categories_df.filter(F.col('report_name').isin(pivot_report_names))
    # Replace spaces and special characters in the report_name column
    categories_df = categories_df.withColumn("report_name", F.regexp_replace(F.col("report_name"), "[^a-zA-Z0-9]", "_"))
    # Create an array with the two columns we want to pivot
    categories_df = categories_df.withColumn("values", F.struct(F.col("total_amount"), F.col("total_count")))
    # Pivot the DataFrame
    pivot_df = categories_df.groupBy("hub_id", "site", "month", "year").pivot("report_name").agg(F.first("values"))
    # Split the struct back into two separate columns for each report_name
    for column in pivot_df.columns:
        if column not in ["hub_id", "site", "month", "year"]:
            new_column = column.replace(" ", "_").lower() + "_amount"
            new_count_column = column.replace(" ", "_").lower() + "_count"
            pivot_df = pivot_df.withColumn(new_column, F.col(column).getItem("total_amount"))
            pivot_df = pivot_df.withColumn(new_count_column, F.col(column).getItem("total_count"))
            pivot_df = pivot_df.drop(column)
    # Rename the columns to lowercase
    categories_df = pivot_df.toDF(*[col.lower() for col in pivot_df.columns])
    # Create temp table
    categories_df.createOrReplaceTempView("temp_categories_table")

    # Getting membership values based on 'SalePasses' Status code
    # Used to break down membership information by package type
    with sql.connect(server_hostname=os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"),
                 http_path=os.getenv("DATABRICKS_HTTP_PATH"),
                 access_token=os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query2)
            data = cursor.fetchall_arrow()
            hub_mem_by_code_df = data.to_pandas()
            hub_mem_by_code_df = spark.createDataFrame(hub_mem_by_code_df)
            cursor.close()
        connection.close()

    hub_mem_by_code_df.createOrReplaceTempView("temp_hub_mem_by_code_table")

    # Adding some values to the GSR report data
    # This is done in a spark sql query to make use of the views made above
    rdf = spark.sql(
        f"""
            SELECT 
                mem_by_code.*,
                ((rpt_totals.arm_plans_sold_amount + rpt_totals.arm_plans_recharged_amount) + (rpt_totals.express_basics_amount + rpt_totals.express_pkgs_amount + rpt_totals.arm_plan_extra_rdmd_amount)) as total_wash_revenue,
                rpt_totals.express_basics_count as total_wash_counts,

                rpt_totals.arm_plan_extra_rdmd_amount as arm_plans_redeemed_amount,
                rpt_totals.arm_plan_extra_rdmd_count as arm_plans_redeemed_count,
                mem_by_code.arms_redeemed as arms_count_by_code,

                rpt_totals.express_basics_count - rpt_totals.arm_plan_extra_rdmd_count  as retail_counts,
                rpt_totals.express_basics_amount + rpt_totals.express_pkgs_amount + rpt_totals.arm_plan_extra_rdmd_amount as retail_revenue,
                (rpt_totals.express_basics_amount + rpt_totals.express_pkgs_amount + rpt_totals.arm_plan_extra_rdmd_amount) / (rpt_totals.express_basics_count - rpt_totals.arm_plan_extra_rdmd_count) as retail_ticket_average,


                -- mem_by_code.arms_joined / (total_wash_counts - arms_redeemed) * 100 as capture_rate,
                rpt_totals.arm_plans_sold_count / (rpt_totals.express_basics_count - rpt_totals.arm_plan_extra_rdmd_count) * 100 as capture_rate,

                -- mem_by_code.arms_sold_revenue + mem_by_code.arms_recharge_amt as arms_total_revenue,
                rpt_totals.arm_plans_sold_amount + rpt_totals.arm_plans_recharged_amount as arms_total_revenue,

                -- (mem_by_code.distinct_arms_terminated) / (arms_recharged + arms_transfer_in + mem_by_code.distinct_arms_terminated) * 100 as churn_rate,
                (rpt_totals.arm_plans_terminated_count) / (rpt_totals.arm_plans_recharged_count + rpt_totals.arm_plans_terminated_count) * 100 as churn_rate,

                -- arms_total_revenue / arms_redeemed as arm_average_ticket,
                (rpt_totals.arm_plans_sold_amount + rpt_totals.arm_plans_recharged_amount) / (rpt_totals.arm_plan_extra_rdmd_count) as arm_average_ticket,

                -- bubble bath version below
                -- mem_by_code.arms_sold_transfer + mem_by_code.arms_recharged - distinct_arms_terminated as active_arm_count,

                mem_by_code.redeem_customer_count as active_arm_count,
                mem_by_code.arms_redeemed / mem_by_code.redeem_customer_count as arm_usage_rate,
                ((rpt_totals.arm_plans_sold_amount + rpt_totals.arm_plans_recharged_amount) + (rpt_totals.express_basics_amount + rpt_totals.express_pkgs_amount + rpt_totals.arm_plan_extra_rdmd_amount)) / (rpt_totals.express_basics_count) as revenue_per_car
        


            FROM    (Select 
                        hub_id,
                        hub_site,
                        site_id,
                        month,
                        year,
                        sum(redeemed) as arms_redeemed,
                        sum(redeem_customer_count) as redeem_customer_count,
                        sum(redeemed_amt) as arms_redeemed_revenue,
                        sum(joined) as arms_joined,
                        sum(joined + transfer_in) as arms_sold_transfer,
                        sum(joined_amt + transfer_in_amt) as arms_sold_revenue,
                        sum(renewed) as arms_recharged,
                        sum(renewed_amt) as arms_recharge_amt, 
                        sum(terminated) as arms_terminated,
                        sum(discontinued) as arms_discontinued,
                        sum(discontinuing) as arms_discontinuing_nxt_month,
                        sum(transfer_out) as arms_transfer_out,
                        sum(transfer_in) as arms_transfer_in,
                        sum(distinct_terminated_discontinue) as distinct_arms_terminated

                    FROM temp_hub_mem_by_code_table
                    WHERE hub_id = {hub_id}
                        AND hub_site = {site_id}
                        AND year >= {query_year}
                    GROUP BY
                        hub_id,
                        hub_site,
                        site_id,
                        month,
                        year
                    ORDER BY
                        hub_id,
                        hub_site,
                        year,
                        month) as mem_by_code


            JOIN temp_categories_table as rpt_totals
                on  rpt_totals.hub_id = mem_by_code.hub_id
                AND rpt_totals.site = mem_by_code.hub_site
                AND rpt_totals.month = mem_by_code.month
                AND rpt_totals.year = mem_by_code.year

            WHERE rpt_totals.month IS NOT NULL
                AND rpt_totals.year IS NOT NULL

            ORDER BY
                rpt_totals.hub_id,
                rpt_totals.site,
                rpt_totals.year,
                rpt_totals.month
        """
    )

    df = rdf.toPandas()
    # Convert 'year' and 'month' to integer
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    # Convert the 'year' and 'month' fields to datetime
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    # Filter to only keep dates up to current quarter
    df = df[df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')]
    # Sort the filtered dataframe
    full_df = df.sort_values(['hub_site','date'])
    # Include column for quarter
    full_df['quarter'] = full_df['date'].dt.quarter


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

    # # If threshold necessary, include it here
    # threshold = 10
    # df = df[df[col]>threshold]

    # plot
    line_plot(df=df, col=col, ylabel=ylabel)

    # # Quarterly site analysis
    # quarterly_df = site_qoq(df=full_df.copy(), col=[col])

    # # Average across all sites
    # quarterly_avg_df = avg_qoq(df=full_df.copy(), col=[col])

    # YoY RPC
    # Calculate YoY% change
    df = full_df.copy()
    df = yoy_monthly(df=df, col=col)

    # # If threshold necessary, include it here
    # threshold = 32000
    # df = df[df[f"yoy_{col}"]<threshold]

    # define ylabel, col
    ylabel = 'YoY Change (%)'
    yoy_col = f"yoy_{col}"

    # Plot
    line_plot(df=df, col=yoy_col, ylabel=ylabel)



if st.button('Get Retail Package Distribution'):
    st.write('Generating quarterly breakdown of retail package sales, and monthly performance over past year.')
    # Retail Wash Sales breakdown
    # Getting retail package distribution
    arms_rdm_rpt = 103962  # ARM Plans Redeemed

    # FOR SGT CLEAN ONLY need to include another package report ID to get proper distribution.
    # This necessitates a change in the WHERE condition in the tot_table selection below. 
    # For other sites change the IN to an =, and get rid of the AND altogether.

    query3 = f"""
            with tot_table as (
                SELECT
                    {hub_id} as hub_id,
                    hub_sites.hub_site,
                    sale.objid as sale_id,
                    item.name,
                    item.reportcategory,
                    YEAR(sale.LOGDATE) AS year,
                    MONTH(sale.LOGDATE) AS month
                FROM
                    ncs_index_dev.hub_{hub_id}.sale AS sale
                    Inner JOIN ncs_index_dev.hub_{hub_id}.saleitems AS saleitems
                        ON sale.SITE = saleitems.SITE
                        AND sale.OBJID = saleitems.SALEID
                    Inner JOIN ncs_index_dev.hub_{hub_id}.item
                        ON saleitems.ITEM = item.OBJID
                    Inner JOIN ncs_index_dev.data_hub.sites AS hub_sites
                        ON sale.SITE = hub_sites.hub_site
                        AND {hub_id} = hub_sites.hub_id
                WHERE 
                    item.reportcategory IN {hub_dict[hub_id]['package_rpt']}
                    AND item.name != 'EXT Package Wash'
            ), 

            arm_rdm as (
                SELECT
                    {hub_id} as hub_id,
                    hub_sites.hub_site,
                    sale.objid as sale_id,
                    item.name,
                    item.reportcategory,
                    YEAR(sale.LOGDATE) AS year,
                    MONTH(sale.LOGDATE) AS month
                FROM
                    ncs_index_dev.hub_{hub_id}.sale AS sale
                    Inner JOIN ncs_index_dev.hub_{hub_id}.saleitems AS saleitems
                        ON sale.SITE = saleitems.SITE
                        AND sale.OBJID = saleitems.SALEID
                    Inner JOIN ncs_index_dev.hub_{hub_id}.item
                        ON saleitems.ITEM = item.OBJID
                    Inner JOIN ncs_index_dev.data_hub.sites AS hub_sites
                        ON sale.SITE = hub_sites.hub_site
                        AND {hub_id} = hub_sites.hub_id
                WHERE
                    item.reportcategory = {arms_rdm_rpt}

            )
            
        SELECT
            tot_table.hub_id,
            tot_table.hub_site,
            tot_table.name,
            tot_table.reportcategory,
            tot_table.year,
            tot_table.month,
            count(tot_table.sale_id) as car_counts
        FROM tot_table
        LEFT ANTI JOIN arm_rdm
            ON tot_table.sale_id = arm_rdm.sale_id

        GROUP BY
            tot_table.hub_id,
            tot_table.hub_site,
            tot_table.name,
            tot_table.reportcategory,
            tot_table.year,
            tot_table.month
        ORDER BY 
            hub_id,
            hub_site,
            tot_table.year,
            tot_table.month
        """

    with sql.connect(server_hostname=os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"),
             http_path=os.getenv("DATABRICKS_HTTP_PATH"),
             access_token=os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query3)
            data = cursor.fetchall_arrow()
            df = data.to_pandas()
            cursor.close()
        connection.close()

    df = df.dropna(subset=['year','month'])

    # Convert 'year' and 'month' to integer
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)

    # Convert the 'year' and 'month' fields to datetime
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    # Filter for desired dates
    # Use start_year - 1 for YoY calculations
    full_package_df = df[
        ((df['date'] >= pd.to_datetime(f'{start_year-1}-{start_month}-01')) & 
        (df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01')))
    ]
    # Sort the filtered dataframe
    full_package_df = full_package_df.sort_values('date')
    # Include column for quarter
    full_package_df['quarter'] = full_package_df['date'].dt.quarter
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
    df = package_distribution_plot(df=df, words=words, col=col, title=title)
    # qoq_df = qoq_package(df=df,col=col)
    # Retail package distribution over time
    monthly_package_distribution_plot(df=df, words=words, col=col)





if st.button('Get Membership Package Distribution'):
    st.write('Generating quarterly breakdown of membership package sales, and monthly performance over past year.')
    # Membership Sold Distribution
    query4 = f"""
            SELECT *,
                redeem_customer_count as active_arms 
            FROM ncs_index_dev.reports.hub_q1_memberships
            WHERE hub_id = {hub_id}
        """

    with sql.connect(server_hostname=os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME"),
             http_path=os.getenv("DATABRICKS_HTTP_PATH"),
             access_token=os.getenv("DATABRICKS_TOKEN")) as connection:

        with connection.cursor() as cursor:
            cursor.execute(query4)
            data = cursor.fetchall_arrow()
            df = data.to_pandas()
            cursor.close()
        connection.close()

    # Drop nulls
    df.dropna(inplace=True)
    # Convert 'year' and 'month' to integer
    df['year'] = df['year'].astype(int)
    df['month'] = df['month'].astype(int)
    # Convert the 'year' and 'month' fields to datetime
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    # Filter to only keep dates up to current quarter
    df = df[(df['date'] >= pd.to_datetime(f'{query_year}-{start_month}-01')) &
        (df['date'] <= pd.to_datetime(f'{end_year}-{end_month}-01'))]
    # Include quarter
    df['quarter'] = df['date'].dt.quarter
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
    df = package_distribution_plot(df=df, words=words, col=col, title=title, threshold=threshold)
    # qoq_df = qoq_package(df=df,col=col,threshold=threshold)
    # Membership change in wash package distribution over time
    monthly_package_distribution_plot(df=df, words=words, col=col, threshold=threshold)
