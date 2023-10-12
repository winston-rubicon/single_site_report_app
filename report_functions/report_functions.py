import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib import font_manager
import seaborn as sns
import calendar
import datetime as dt

### TODO: Need some sort of function for reading in the data on AWS.
### For now will assume that the file name is passed in to the program either by the trigger on AWS
### Or by user input for the desired month, year desired.
filename = "fake_data/10_2023.json"
with open(filename, "r") as f:
    data = json.load(f)

# Register AtlasGrotesk font
font_manager.fontManager.addfont("branding/fonts/AtlasGrotesk-Regular.ttf")
font = font_manager.FontProperties(fname="branding/fonts/AtlasGrotesk-Regular.ttf")
font_manager.fontManager.addfont("branding/fonts/AtlasGrotesk-Bold.ttf")
font_bold = font_manager.FontProperties(fname="branding/fonts/AtlasGrotesk-Bold.ttf")

# Define color palette, this will change at package distribution plots!
color_palette = [
    "#0b75e1",
    "#003264",
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

# Get the month labels for plotting, make sure they are sorted in order
# this will be essential for ensuring plots are correctly made
month_year_list = sorted(
    data["total_wash_counts"].keys(),
    key=lambda x: (int(x.split("_")[1]), int(x.split("_")[0])),
)
# TODO: will current year, month come from filename? User input?
current_month = int(month_year_list[-1].split("_")[0])
current_year = int(month_year_list[-1].split("_")[1])

# Now create the labels that will be used for all plots that span the twelve month period up to the latest month of data
# Convert the sorted month-year strings to the desired format
month_labels = []
prev_year = None
for date in month_year_list:
    month, year = date.split("_")
    month_name = calendar.month_abbr[int(month)]
    if month_name == "Jan":
        month_labels.append(f"{month_name} {current_year}")
    else:
        month_labels.append(month_name)


### Now begin the plotting functions


# Line Plot
def line_plot(col, ylabel):
    """
    Plots the data that will be plotted as a line plot function of month for the year leading up to the current month the report is generated.
    """

    # First get the appropriate y data
    ydata = [data[col][month_year] for month_year in month_year_list]

    # Create a figure
    fig, ax = plt.subplots(figsize=(10, 6))

    # Plot the data
    ax.plot(month_labels, ydata)

    # Set axis font parameters
    plt.ylabel(ylabel, color=color_palette[0], fontproperties=font)
    ax.yaxis.label.set_fontsize(14)
    ax.set_xlabel(None)

    # Put grid over plot
    ax.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")

    # Add gray border and vertical lines
    ax.spines["top"].set_color("lightgray"), ax.spines["right"].set_color(
        "lightgray"
    ), ax.spines["bottom"].set_color("lightgray"), ax.spines["left"].set_color(
        "lightgray"
    )
    for month in month_labels:
        ax.axvline(month, color="lightgray", linestyle="-", linewidth=1)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    plt.tight_layout()

    return fig


# Bar Plot
def bar_plot(cols, ylabel, legend_labels=['blah']):
    """
    For now only used to plot the retail/membership breakdown as a function of month of the year (assumes all 12 months), maybe more bar plots in the future?
    """

    # First get the appropriate y data
    ydata0 = [data[cols[0]][month_year] for month_year in month_year_list]

    # create fig, ax
    fig, ax = plt.subplots(figsize=(12, 3))
    # Generate bar plot
    width = 0.4
    plt.bar(
        month_labels,
        ydata0,
        width=width,
        color=color_palette[0],
        label=legend_labels[0],
    )
    if len(cols)>1:
        ydata1 = [data[cols[1]][month_year] for month_year in month_year_list]
        plt.bar(
            [i + width for i in range(len(month_labels))],
            ydata1,
            width=width,
            color=color_palette[1],
            label=legend_labels[1],
        )

    # Axis label properties
    ax.set_ylabel(ylabel, color=color_palette[0], fontproperties=font)
    ax.yaxis.label.set_fontsize(14)
    plt.xlabel(None)

    ax.set_frame_on(False)
    plt.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")
    ax.tick_params(left=False, bottom=False, colors="#525661", labelsize=14)

    plt.subplots_adjust(right=0.85)
    legend = ax.legend(
        legend_labels, loc="center left", bbox_to_anchor=(1.02, 0.5), fontsize=14, prop=font
    )

    if len(legend_labels)==1:
        legend.set_visible(False)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    plt.tight_layout()

    return fig

# Package Distribution Plots
def package_distribution_plot(col, title, threshold=0):
    '''
    Create ring plots for the desired package breakdown (retail or membership)
    '''
    package_names = data[col].keys()
    package_data = [data[col][package] for package in package_names]

    # Now you can plot your pie chart as a donut plot
    fig, ax = plt.subplots()
    p_labels = ["{:.1f}%".format(data[col][package]) for package in package_names]
    patches, texts = ax.pie(package_data, labels=p_labels, colors=color_palette)

    # Draw a white circle at the center to create the "donut hole" effect
    centre_circle = plt.Circle((0, 0), 0.70, fc="white")
    fig.gca().add_artist(centre_circle)

    # Set the title at the center
    label = ax.annotate(
        title,
        color="#003264",
        ha="center",
        va="center",
        xy=(0, 0),
        fontproperties=font_bold,
    )
    label.set_fontsize(fontsize=12)

    # Add a legend
    legend = ax.legend(
        patches,
        labels=package_names,
        loc="lower center",
        bbox_to_anchor=(0.5, -0.25),
        title=None,
        prop={"family": font},
    )

    legend.get_frame().set_facecolor("#f5f5f5")
    legend.get_frame().set_linewidth(0)

    # Set the font for the legend labels
    for text in legend.get_texts():
        text.set_fontproperties(font)

    for text in texts:
        text.set_color("#003264")
        text.set_fontproperties(font_bold)

    plt.tight_layout()

    return fig