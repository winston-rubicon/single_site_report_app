###----- Imports needed for generating plots, etc.
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import calendar
import matplotlib.dates as mdates
import matplotlib.colors as mcolors
import numpy as np
from collections import OrderedDict
import datetime as dt
import matplotlib.ticker as ticker
from matplotlib import font_manager
import streamlit as st

font_manager.fontManager.addfont("branding/fonts/AtlasGrotesk-Regular.ttf")

font = font_manager.FontProperties(
    fname="branding/fonts/AtlasGrotesk-Regular.ttf"
)

# Define color palette, this will change at package distribution plots!
color_palette = [
    "#003264",
    "#0b75e1",
    "#87cefa",
    "#ffcb00",
    "#e7af50",
    "#c70039",
    "#800080",
    "#006400",
    "#8b4513",
    "#ffb6c1",
    "#e6e6fa",
    "#00A000",
]


# Your start and end year, month
# TODO: How to implement these programmatically? User input? Current date and do previous quarter?
start_year, start_month = 2022, 4
end_year, end_month = 2023, 6
query_year = start_year - 1

# Current quarter
quarter = 2

# Line Plot
def line_plot(df, col, ylabel):
    """
    Plotting a given column's data for all sites on a single plot, as a function of date. Assumes that the DataFrame df will need to have a groupby in date and hub site performed on it, and is a copy of the main DataFrame used throughout the notebook. Returns None, just plots the data.
    """

    # Groupby desired parameters (date, site) and plot the data you want (total_wash_counts in this case)
    ax = (
        df.groupby(["date", "hub_site"])[col]
        .first()
        .unstack()
        .plot(kind="line", color=color_palette, figsize=(10, 6))
    )

    # Set axis font parameters
    ax.set_ylabel(ylabel, color="#0b75e1", fontsize=14, fontproperties=font)
    ax.set_xlabel(None)

    # Put grid over plot
    ax.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(start=df["date"].min(), end=df["date"].max(), freq="MS")
    ax.set_xticks(date_range)
    ax.set_xticklabels(
        [
            dt.strftime("%b %Y") if dt.month == 1 else dt.strftime("%b")
            for dt in date_range
        ]
    )

    # Add gray border and vertical lines
    ax.spines["top"].set_color("lightgray"), ax.spines["right"].set_color(
        "lightgray"
    ), ax.spines["bottom"].set_color("lightgray"), ax.spines["left"].set_color(
        "lightgray"
    )
    for date in date_range:
        ax.axvline(date, color="lightgray", linestyle="-", linewidth=1)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    legend = plt.legend(
        title="Site", bbox_to_anchor=(1.02, 0.5), loc="center left", fontsize=14
    )
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14)  # Set the font size of the legend title
    plt.tight_layout()

    # st.pyplot(plt)

    # ax.get_figure().savefig("figs/" + col, dpi=300)
    return ax.get_figure()

# Bar Plot
def bar_plot(df, col=None, ylabel=None):
    """
    Calculates the fraction of retail and membership sales in the current quarter, and plots the monthly progression over the past year.
    """

    # Print out which hub_sites are present (either going to be all or one)
    print(f"Sites included: {np.sort(df['hub_site'].unique())}")

    # Get the fraction of retail and membership sales in current quarter
    retail_counts = df[(df["quarter"] == quarter) & (df["year"] == end_year)][
        "retail_counts"
    ].sum()
    arms_redeemed = df[(df["quarter"] == quarter) & (df["year"] == end_year)][
        "arms_redeemed"
    ].sum()

    retail_percentage = round((retail_counts / (retail_counts + arms_redeemed)) * 100)
    redeemed_percentage = round((arms_redeemed / (retail_counts + arms_redeemed)) * 100)

    # Print result
    print(f"Percentage of Retail Counts in Q{quarter} {end_year}: {retail_percentage}%")
    print(
        f"Percentage of Arms Redeemed in Q{quarter} {end_year}: {redeemed_percentage}%"
    )

    # Begin plotting work
    # Group data by month and year, then calculate sum of 'retail_counts' and 'arms_redeemed'
    grouped_data = (
        df.groupby(["year", "month"])[["retail_counts", "arms_redeemed"]]
        .sum()
        .reset_index()
    )

    # Replace the numerical months with abbreviated month names and concatenate it with year
    grouped_data["month"] = grouped_data.apply(
        lambda row: f"{calendar.month_abbr[int(row['month'])]} {int(row['year'])}",
        axis=1,
    )

    # Set month as index (for better x-axis labels)
    grouped_data.set_index("month", inplace=True)

    # Generate bar plot
    ax = grouped_data[["retail_counts", "arms_redeemed"]].plot(
        kind="bar", figsize=(12, 4), color=["#0b75e1", "#003264"]
    )

    # ax.set_title('Retail & Membership Wash Counts Across All Sites', color='#003264', fontsize=22, y=1.1, x=0.47, fontweight='bold')
    ax.set_ylabel("Wash Count", color="#0b75e1", fontsize=14, fontproperties=font)
    plt.xlabel(None)

    ax.set_frame_on(False)
    plt.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")
    ax.tick_params(
        left=False, bottom=False, colors="#525661", labelsize=14
    )

    plt.subplots_adjust(right=0.85)
    labels = ["Retail", "Membership"]
    ax.legend(
        labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        fontsize=14,
        prop=font
    )

    # Modify x-axis tick labels to include only the year on January
    x_labels = [
        label.split()[0] if label.split()[0] != "Jan" else label
        for label in grouped_data.index
    ]
    ax.set_xticklabels(x_labels)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    # ax.get_figure().savefig("figs/retail_membership_sales.png", dpi=300)
    return ax.get_figure()


# Package Distribution Plots
def package_distribution_plot(df, words, col, title, threshold=0):
    # Defining color palette
    palette = ["#003264", "#0b75e1", "#87cefa", "#ffcb00", "#e7af50", "#c70039"]

    # Generate a mapping of words to a priority order based on word list
    word_to_priority = {word: i for i, word in enumerate(words)}

    # Create a new column in the DataFrame that assigns the priority to each name
    df["priority"] = df["name"].apply(
        lambda name: min(
            [word_to_priority[word] for word in words if word in name.lower()],
            default=len(words),
        )
    )

    # Then group by 'priority' and 'name' and calculate the sum of col for each group
    df_grouped = (
        df[(df["quarter"] == quarter) & (df["year"] == end_year)]
        .groupby(["priority", "name"])[col]
        .sum()
    )

    # Normalize the col to get the percentage for each 'name'
    df_grouped = df_grouped / df_grouped.sum() * 100

    # If percentage is less than threshold, replace the 'name' with 'Other'
    df_grouped.index = pd.MultiIndex.from_tuples(
        [
            (i if p > threshold else len(words), n if p > threshold else "Other")
            for ((i, n), p) in df_grouped.items()
        ]
    )

    # Regroup the data to sum the col of 'Other'
    df_grouped = df_grouped.groupby(level=[0, 1]).sum()

    # Now you can plot your pie chart as a donut plot
    fig, ax = plt.subplots()
    p_labels = ["{:.1f}%".format(i) for i in df_grouped]
    patches, texts = ax.pie(df_grouped, labels=p_labels, colors=palette)

    # Draw a white circle at the center to create the "donut hole" effect
    centre_circle = plt.Circle((0, 0), 0.70, fc="white")
    fig.gca().add_artist(centre_circle)

    # Set the title at the center
    label = ax.annotate(
        title,
        color="#003264",
        fontsize=12,
        ha="center",
        va="center",
        xy=(0, 0),
        weight="bold",
        fontproperties=font,
    )

    # Add a legend
    legend = ax.legend(
        patches,
        labels=[name for _, name in df_grouped.index],
        loc="lower center",
        bbox_to_anchor=(0.5, -0.25),
        title=None,
        title_fontsize=12,
        prop={"family": font},
    )

    legend.get_frame().set_facecolor("#f5f5f5")
    legend.get_frame().set_linewidth(0)

    # Set the font for the legend labels
    for text in legend.get_texts():
        text.set_fontproperties(font)

    for text in texts:
        text.set_color("#003264")
        text.set_weight("bold")
        text.set_fontproperties(font)

    plt.tight_layout()

    # plt.savefig("figs/" + col, dpi=300)
    return df, fig


def monthly_package_distribution_plot(df, words, col=None, threshold=0):
    # Defining color palette
    palette = ["#003264", "#0b75e1", "#87cefa", "#ffcb00", "#e7af50", "#c70039"]

    # Filter the data based on the names containing the words
    df_filtered = df[df["name"].str.contains("|".join(words), case=False)]

    # Generate a mapping of words to a priority order based on your list
    word_to_priority = {word: i for i, word in enumerate(words)}

    # Assign priority to names based on the highest priority word they contain
    df_filtered["priority"] = df_filtered["name"].apply(
        lambda name: min(
            [word_to_priority[word] for word in words if word in name.lower()],
            default=len(words),
        )
    )

    # Sort the DataFrame by the priority column
    df_filtered = df_filtered.sort_values("priority")

    # Group by date and name
    grouped = df_filtered.groupby(["date", "name"]).sum().reset_index()

    # Calculate total car_count for each month
    total_monthly_count = grouped.groupby("date")[col].sum()

    # Divide the car_count of each name type by the total count of the month to get the distribution
    grouped["distribution"] = grouped.apply(
        lambda row: (row[col] / total_monthly_count[row["date"]]) * 100, axis=1
    )

    # Exclude 'name' types with distribution below the threshold in the given time range
    grouped = grouped[grouped["distribution"] > threshold]

    # Exclude 'name' types with less than 2 unique months of data
    grouped = grouped[grouped.groupby("name")["date"].transform("nunique") > 1]

    # Get the order of names based on word priority
    name_order = grouped.groupby("name")["priority"].min().sort_values().index

    # Plotting the distribution over time

    # Unstack and drop NaNs
    grouped_unstacked = (
        grouped.groupby(["date", "name"])["distribution"]
        .first()
        .unstack()
        .dropna(axis=1, how="any")
    )

    # Reorder the columns based on name_order
    grouped_unstacked = grouped_unstacked[name_order]

    ax = grouped_unstacked.plot(kind="line", color=palette, figsize=(10, 6))

    ax.set_ylabel("Distribution (%)", color="#0b75e1", fontsize=14, fontproperties=font)
    ax.set_xlabel(None)

    ax.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(
        start=grouped_unstacked.index.min(),
        end=grouped_unstacked.index.max(),
        freq="MS",
    )
    ax.set_xticks(date_range)
    ax.set_xticklabels(
        [
            dt.strftime("%b %Y") if dt.month == 1 else dt.strftime("%b")
            for dt in date_range
        ],
        fontproperties=font,
    )

    # Set the x-axis limits to exclude ticks before the first month and after the last month
    ax.set_xlim(date_range[0], date_range[-1])

    # Add gray border and vertical lines
    ax.spines["top"].set_color("lightgray")
    ax.spines["right"].set_color("lightgray")
    ax.spines["bottom"].set_color("lightgray")
    ax.spines["left"].set_color("lightgray")

    for date in date_range:
        ax.axvline(date, color="lightgray", linestyle="-", linewidth=1)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    legend = plt.legend(
        name_order, bbox_to_anchor=(1.02, 0.5), loc="center left", fontsize=14
    )
    plt.tight_layout()

    # ax.get_figure().savefig("figs/" + col + "over_time", dpi=300)
    return ax.get_figure()

def popular_days(df):
    # Ensure the weekdays are in correct order
    df['day_of_week'] = pd.Categorical(df['day_of_week'], categories=
        ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday', 'Sunday'],
        ordered=True)

    # Define start and end dates for filtering
    start_date = pd.to_datetime(f'{end_year}-{start_month}-01')
    end_date = pd.to_datetime(f'{end_year}-{end_month}-01')

    # Filter data between start and end dates for the specific hub_id
    df_filtered= df[(df['date'] >= start_date) & (df['date'] <= end_date)]

    # Then group by 'day_of_week' and 'hub_site' and calculate the mean of 'day_of_week_counts' for each group
    df_grouped = df_filtered.groupby(['day_of_week', 'hub_site'])['day_of_week_counts'].mean().reset_index()

    # Create the grouped bar plot
    plt.figure(figsize=(15, 9))

    # Apply brand style
    sns.set(style='whitegrid')
    bar = sns.barplot(data=df_grouped, x='day_of_week', y='day_of_week_counts', hue='hub_site', palette=color_palette)

    # Set title and labels with brand colors and style
    # plt.title('Quarter 1 2023 Average Wash Counts By Day of Week', color='#003264', fontsize=22, y=1.1, fontweight='bold')
    plt.ylabel('Average Counts', color='#0b75e1', fontsize=14, fontproperties=font)
    plt.xlabel(None)

    # Modify grid and ticks appearance
    plt.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')
    bar.tick_params(left=False, bottom=False, colors='#525661')

    # For x-ticks
    for label in bar.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    # For y-ticks
    for label in bar.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    # Move the legend to the right side of the plot
    legend = plt.legend(title='Site', bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14)  

    return df_filtered, bar.get_figure()

def popular_hours(hours_df, threshold=10):
    # Define start and end dates for filtering
    start_date = pd.to_datetime(f'{end_year}-{start_month}-01')
    end_date = pd.to_datetime(f'{end_year}-{end_month}-01')

    # Filter data between start and end dates for the specific hub_id
    df_filtered= hours_df[(hours_df['date'] >= start_date) & (hours_df['date'] <= end_date)]

    # Then group by 'hour' and 'hub_site' and calculate the mean of 'hour_counts' for each group
    df_grouped = df_filtered.groupby(['hour', 'hub_site'])['hour_counts'].mean().reset_index()

    # Filter rows where 'hour_counts' is above the threshold
    df_grouped = df_grouped[df_grouped['hour_counts'] >= threshold]

    # Create a list with sorted hours in desired format
    hour_order = [f"{i % 12 if i % 12 != 0 else 12}{('am', 'pm')[i // 12]}" for i in range(24)]

    # Convert the 'hour' column to string format, then map it to the new format
    df_grouped['hour'] = df_grouped['hour'].map(dict(zip(range(24), hour_order)))

    # Create the grouped bar plot
    plt.figure(figsize=(15, 9))

    # Apply brand style
    sns.set(style='whitegrid')
    bar = sns.barplot(data=df_grouped, x='hour', y='hour_counts', hue='hub_site', palette=color_palette)

    # Set title and labels with brand colors and style
    plt.ylabel('Average Counts', color='#0b75e1', fontsize=14, fontproperties=font)
    plt.xlabel(None)

    # Modify grid and ticks appearance
    plt.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')
    bar.tick_params(left=False, bottom=False, colors='#525661')

    # For x-ticks
    for label in bar.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)
    # For y-ticks
    for label in bar.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)
    # Move the legend to the right side of the plot
    legend = plt.legend(title='Site', bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14)  
    return bar.get_figure()

def optimal_weather_days(df):
    # Group data by hub_site and calculate the sum of 'precip_avg_days_above_15' and 'snowfall_avg_days_above_1'
    grouped_data = df.groupby(['hub_site', 'year', 'month'])['prcp_snow_days_above_threshold'].sum().reset_index()

    # Calculate the total number of days in each month
    grouped_data['total_days'] = grouped_data.apply(lambda row: calendar.monthrange(int(row['year']), int(row['month']))[1], axis=1)

    # Calculate the addition of the two columns and then subtract from total days to get non-precipitation or non-snowfall days
    grouped_data['non_precip_snow_days'] = grouped_data['total_days'] - grouped_data['prcp_snow_days_above_threshold']

    # Convert year and month into a single datetime column
    grouped_data['date'] = pd.to_datetime(grouped_data[['year', 'month']].assign(day=1))

    # Create 'Month Year' label for x-axis
    grouped_data['Month_Year'] = grouped_data['date'].dt.strftime('%b')
    grouped_data.loc[grouped_data['date'].dt.month == 1, 'Month_Year'] = grouped_data['date'].dt.strftime('%b %Y')

    # Sort the values by date
    grouped_data = grouped_data.sort_values(by='date')

    # Convert 'year' and 'month' to datetime and set as index
    grouped_data['date'] = pd.to_datetime(grouped_data[['year', 'month']].assign(day=1))
    grouped_data.set_index('date', inplace=True)

    # Create the line plot
    # Custom color palette
    weather_color_palette = sns.color_palette(color_palette)
    ax = grouped_data.groupby(['date','hub_site'])['non_precip_snow_days'].first().unstack().plot(kind='line', color=weather_color_palette, figsize=(10, 6))

    # ax.set_title('Optimal Wash Days by Site', color='#003264', fontsize=22, y=1.1, fontweight='bold')
    ax.set_ylabel('Days', color='#0b75e1', fontsize=14, fontproperties=font)
    ax.set_xlabel(None)

    ax.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(start=grouped_data.index.min(), end=grouped_data.index.max(), freq='MS')
    ax.set_xticks(date_range)
    ax.set_xticklabels([dt.strftime('%b %Y') if dt.month == 1 else dt.strftime('%b') for dt in date_range])
    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)
        label.set_rotation(45)
    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)

    # Add gray border and vertical lines
    ax.spines['top'].set_color('lightgray')
    ax.spines['right'].set_color('lightgray')
    ax.spines['bottom'].set_color('lightgray')
    ax.spines['left'].set_color('lightgray')

    for date in date_range:
        ax.axvline(date, color='lightgray', linestyle='-', linewidth=1)

    legend = plt.legend(title='Site', bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14)  

    return grouped_data, ax.get_figure()

def washes_per_optimal_day(df):
    # Group data by hub_site, year, and month and calculate the sum of 'car_counts', 'precip_avg_days_above_15', and 'snowfall_avg_days_above_1'
    grouped_data = df.groupby(['hub_site', 'year', 'month', 'date'])[['car_counts', 'prcp_snow_days_above_threshold']].sum().reset_index()

    # Calculate the number of optimal days in each month
    grouped_data['optimal_days'] = grouped_data.apply(lambda row: calendar.monthrange(int(row['year']), int(row['month']))[1] - row['prcp_snow_days_above_threshold'], axis=1)

    # Calculate car washes per optimal day
    grouped_data['car_washes_per_day'] = grouped_data['car_counts'] / grouped_data['optimal_days']

    # Convert the date back to string format for easier plotting in bar chart
    grouped_data['date_str'] = grouped_data['date'].dt.strftime('%b-%Y')

    # Pivot the dataframe to wide format suitable for bar plot
    pivot_df = grouped_data.pivot(index='date', columns='hub_site', values='car_washes_per_day')

    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    pivot_df.plot(kind='bar', ax=ax, color=color_palette, width=0.8)

    # ax.set_title('Car Washes per Optimal Day by Site', color='#003264', fontsize=22, y=1.1, fontweight='bold')
    ax.set_ylabel('Car Washes per Optimal Day', color='#0b75e1', fontsize=14, fontproperties=font)
    ax.set_xlabel(None)
    ax.grid(which='major', color='#525661', linestyle=(0, (1, 10)), axis='y')

    # Add gray border and vertical lines
    ax.spines['top'].set_color('lightgray')
    ax.spines['right'].set_color('lightgray')
    ax.spines['bottom'].set_color('lightgray')
    ax.spines['left'].set_color('lightgray')

    # Format x-axis labels
    date_range = pd.date_range(start=f'{start_year}-{start_month}-01', end=pd.to_datetime(f'{end_year}-{end_month}-01') + pd.offsets.MonthBegin(1), freq='M')
    ax.set_xticks(range(len(date_range)))
    ax.set_xticklabels([dt.strftime('%b %Y') if dt.month == 1 else dt.strftime('%b') for dt in date_range], rotation=45)
    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)
        label.set_rotation(45)
    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)

    # Create the legend
    legend = plt.legend(title='Site', bbox_to_anchor=(1.02, 0.5), loc='center left', fontsize=14)
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14) 

    return grouped_data, ax.get_figure()

# Econ Plots
def econ_plot(nat_df, div_df, nat_y, div_y, ylabel, div_label):
    # Plotting
    ax = nat_df.plot(
        x="date",
        y=nat_y,
        kind="line",
        color=color_palette,
        figsize=(10, 6),
        label="National",
    )
    div_df.plot(x="date", y=div_y, ax=ax, label=div_label)

    ax.set_ylabel(ylabel, color="#0b75e1", fontsize=14, fontproperties=font)
    ax.set_xlabel(None)

    ax.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")

    ax.set_xlim(div_df["date"].min(), div_df["date"].max())

    # Generate a DateFormatter for your x-axis labels and set it as the formatter for the major locator
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())

    # Set x-ticks to every month in the filtered data
    date_range = pd.date_range(
        start=div_df["date"].min(), end=div_df["date"].max(), freq="MS"
    )
    ax.set_xticks(date_range)
    ax.set_xticklabels(
        [
            dt.strftime("%b %Y") if dt.month == 1 else dt.strftime("%b")
            for dt in date_range
        ],
        fontproperties=font,
    )

    # Add gray border and vertical lines
    ax.spines["top"].set_color("lightgray"), ax.spines["right"].set_color(
        "lightgray"
    ), ax.spines["bottom"].set_color("lightgray"), ax.spines["left"].set_color(
        "lightgray"
    )
    for date in date_range:
        ax.axvline(date, color="lightgray", linestyle="-", linewidth=1)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    legend = plt.legend(bbox_to_anchor=(1.02, 0.5), loc="center left", fontsize=14)
    legend.get_title().set_fontproperties(font)
    legend.get_title().set_fontsize(14)  # Set the font size of the legend title

    # st.pyplot(plt)
    return

# YoY Monthly
def yoy_monthly(df, col):
    """
    Updates the given DataFrame with a column of YoY percent changes based on the given DataFrame (df) and column (col). Assumes that the data is in monthly form so that a month to month comparison is given for YoY. Returns the passed DataFrame.
    """

    df[f"yoy_{col}"] = df.groupby("hub_site")[col].pct_change(periods=12) * 100
    df.reset_index(inplace=True, drop=True)

    # drop sites with no values
    # df.dropna(subset=[f"yoy_{col}"], inplace=True)

    return df[
        (
            (df["date"] >= pd.to_datetime(f"{start_year}-{start_month}-01"))
            & (df["date"] <= pd.to_datetime(f"{end_year}-{end_month}-01"))
        )
    ]

# Site QoQ
def site_qoq(df, col: list, average=True):
    """
    Displays and returns the DataFrame containing the desired Quarter over Quarter (QoQ) percent change, as indicated by col. col MUST BE a list in order for the function to work! This allows for multiple columns to be chosen at a time. Analysis done at the hub site level.
    """
    if average:
        # Calculate average for each quarter
        quarterly_df = (
            df.groupby(["hub_site", "year", "quarter"])[col].mean().reset_index()
        )

    else:
        quarterly_df = (
            df.groupby(["hub_site", "year", "quarter"])[col].sum().reset_index()
        )

    for col in col:
        # Calculate QoQ change

        quarterly_df["qoq_" + col] = (
            quarterly_df.groupby("hub_site")[col].pct_change() * 100
        )

        # Calculate quarterly YoY
        quarterly_df["yoy_quarterly_" + col] = (
            quarterly_df.groupby("hub_site")[col].pct_change(periods=4) * 100
        )

    # Only keep relevant quarter's data
    quarterly_df = quarterly_df[
        (quarterly_df["quarter"] == quarter) & (quarterly_df["year"] == end_year)
    ]

    # Return df
    return quarterly_df

# Average QoQ
def avg_qoq(df, col: list, average=True):
    """
    Displays and returns the DataFrame containing the desired Quarter over Quarter (QoQ) percent change, as indicated by col. col MUST BE a list in order for the function to work! This allows for multiple columns to be chosen at a time. Analysis done with average value across all sites.
    """
    if average:
        # Calculate average for each quarter
        quarterly_df = df.groupby(["year", "quarter"])[col].mean().reset_index()

    else:
        quarterly_df = df.groupby(["year", "quarter"])[col].sum().reset_index()

    for col in col:
        # Calculate QoQ change

        quarterly_df["qoq_" + col] = quarterly_df[col].pct_change() * 100

        # Calculate quarterly YoY
        quarterly_df["yoy_quarterly_" + col] = (
            quarterly_df[col].pct_change(periods=4) * 100
        )

    # Only keep relevant quarter's data
    quarterly_df = quarterly_df[
        (quarterly_df["quarter"] == quarter) & (quarterly_df["year"] == end_year)
    ]

    # Display df
    # display(quarterly_df)

    # Return df
    return quarterly_df

# Package QoQ
def qoq_package(df, col=None, threshold=0):
    # Group by date and name
    grouped = df.groupby(["year", "quarter", "name"]).sum().reset_index()

    # Calculate total car_count for each month
    total_quarterly_count = grouped.groupby(["year", "quarter"])[col].transform("sum")

    grouped["distribution"] = grouped[col] / total_quarterly_count

    # Exclude 'name' types with distribution below the threshold in the given time range
    grouped = grouped[grouped["distribution"] > threshold / 100]

    # Calculate the quarterly increase in distributions for each name
    grouped["qoq"] = grouped.groupby("name")["distribution"].pct_change() * 100
    grouped["qoq_yoy"] = (
        grouped.groupby("name")["distribution"].pct_change(periods=4) * 100
    )

    # Display only current quarter
    # display(grouped[(grouped['quarter']==quarter) & (grouped['year']==end_year)])

    # Return all data
    return grouped