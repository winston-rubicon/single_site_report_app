import pandas as pd
import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from databricks import sql
import os

# Defining dictionary for package_rpt codes, etc. associated with a hub_id
hub_dict = {
        10007: {
                'package_rpt': (103958, 1000002),
                'bls_division': 'East North Central',
                'state': 'Ohio'
                },
}

# Your start and end year, month
# TODO: How to implement these programmatically? User input? Current date and do previous quarter?
start_year, start_month = 2022, 4
end_year, end_month = 2023, 6
query_year = start_year-1

# Current quarter
quarter = 2

def gsr_report_query(spark, hub_id, site_id):
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
    # DataFrame to be able to create a temp view to query

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
    
    return full_df

def retail_package_query(hub_id, site_id):
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

    return full_package_df

def membership_package_query(hub_id, site_id):
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

    return df