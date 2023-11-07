import matplotlib.pyplot as plt
from matplotlib import font_manager
import calendar
import plotly.graph_objects as go
from io import BytesIO
import boto3
import os
import json

filename = os.environ.get('FILENAME')
bucket_name = os.environ.get('BUCKET_NAME')
s3 = boto3.client('s3')
s3_object = s3.get_object(Bucket=bucket_name, Key=filename)
file_content = s3_object['Body'].read().decode('utf-8')
data = json.loads(file_content)


# bucket_name = "ncs-washindex-single-site-reports-815867481426"
# filename = "fake_data/9_2023.json"
# with open(filename, "r") as f:
#     data = json.load(f)


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
    data["total_wash_count"].keys(),
    key=lambda x: (int(x.split("_")[1]), int(x.split("_")[0])),
)

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


### Some general use functions
def save_plot(fig):
    """Saving plots generated in app to buffers in order to make pdf without saving files"""
    buffer = BytesIO()
    plt.savefig(buffer, format="png", dpi=300)
    buffer.seek(0)
    return buffer


def save_to_s3(bucket_name, file_key, pdf):
    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Upload BytesIO object to S3
    s3.upload_fileobj(pdf, bucket_name, file_key)


### Now begin the plotting functions
# Line Plot
def line_plot(col, ylabel, legend_labels=None):
    """
    Plots the data that will be plotted as a line plot function of month for the year leading up to the current month the report is generated.
    Assumes single column (key), with possibility for categories within this heading depending on legend_labels
    """
    # Create a figure
    fig, ax = plt.subplots(figsize=(10, 6))

    # If legend labels exist loop over all labels to be plotted
    if legend_labels is not None:
        for i, label in enumerate(data[col].keys()):
            # First get the appropriate y data
            ydata = [data[col][label][month_year] for month_year in month_year_list]

            # Plot the data
            ax.plot(month_labels, ydata, color=color_palette[i], label=col)
        ax.legend(
            labels=legend_labels,
            fontsize=14,
            prop=font,
            bbox_to_anchor = (1.02,0.5)
        )

    else:
        ydata = [data[col][month_year] for month_year in month_year_list]

        # Plot the data
        ax.plot(month_labels, ydata, color=color_palette[0])

        # Set axis font parameters
        plt.ylabel(ylabel, color=color_palette[0], fontproperties=font)
        ax.yaxis.label.set_fontsize(14)
        ax.set_xlabel(None)

    # Set axis font parameters
    plt.ylabel(ylabel, color=color_palette[0], fontproperties=font)
    ax.yaxis.label.set_fontsize(14)
    ax.set_xlabel(None)

    ax.set_xlim(left=0, right=len(month_year_list)-1)

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


# Line Plot
def multi_line_plot(cols, ylabel, legend_labels):
    """
    Plots the data that will be plotted as a line plot function of month for the year leading up to the current month the report is generated.
    Assumes cols is a list of keys so that all data corresponding to each key will be plotted on same axis
    """
    # Create a figure
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Loop over outer keys
    for i,col in enumerate(cols):
        ydata = []
        # Loop over inner keys (month-year typically)
        # Done this way in case traffic data hasn't been updated
        month_labels = data[col].keys()
        for key in month_labels:
            ydata.append(data[col][key])

        # Plot the data
        ax.plot(month_labels, ydata, color=color_palette[i], label=legend_labels[i])

    ax.legend(
            labels=legend_labels,
            fontsize=14,
            prop=font,
            bbox_to_anchor=(1.02,0.5)
        )

    # Set axis font parameters
    plt.ylabel(ylabel, color=color_palette[0], fontproperties=font)
    ax.yaxis.label.set_fontsize(14)
    ax.set_xlabel(None)

    ax.set_xlim(left=0, right=len(month_labels)-1)

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
def year_bar_plot(cols, ylabel, legend_labels=["blah"]):
    """
    For now only used to plot the retail/membership breakdown as a function of month of the year (assumes all 12 months), maybe more bar plots in the future?
    """

    # First get the appropriate y data
    ydata0 = [data[cols[0]][month_year] for month_year in month_year_list]

    # create fig, ax
    fig, ax = plt.subplots(figsize=(24, 6))
    # Generate bar plot
    width = 0.4 if len(legend_labels) == 2 else 0.8
    plt.bar(
        month_labels,
        ydata0,
        width=width,
        color=color_palette[0],
        label=legend_labels[0],
    )
    if len(cols) > 1:
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
    ax.yaxis.label.set_fontsize(20)
    plt.xlabel(None)

    ax.set_frame_on(False)
    plt.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")
    ax.tick_params(left=False, bottom=False, colors="#525661", labelsize=20)

    plt.subplots_adjust(right=0.85)
    legend = ax.legend(
        legend_labels,
        loc="center left",
        bbox_to_anchor=(1.02, 0.5),
        prop=font,
    )
    for label in legend.get_texts():
        label.set_fontsize(16)

    if len(legend_labels) == 1:
        legend.set_visible(False)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(20)  # size you want
        label.set_rotation(45)  # rotation angle

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(20)  # size you want

    plt.tight_layout()

    return fig


def variable_bar_plot(col, ylabel):
    """
    Variable here refers to the x-axis labels being any format, but expected to be in order in json.
    """
    xdata = data[col].keys()

    # Assuming that xdata is the properly formatted labels and they are in order
    ydata = [data[col][label] for label in xdata]

    # create fig, ax
    fig, ax = plt.subplots(figsize=(10, 6))

    # Generate bar plot
    plt.bar(
        xdata,
        ydata,
        color=color_palette[0],
    )

    # Axis label properties
    ax.set_ylabel(ylabel, color=color_palette[0], fontproperties=font)
    ax.yaxis.label.set_fontsize(14)
    plt.xlabel(None)

    ax.set_frame_on(False)
    plt.grid(which="major", color="#525661", linestyle=(0, (1, 10)), axis="y")
    ax.tick_params(left=False, bottom=False, colors="#525661", labelsize=14)

    # For x-ticks
    for label in ax.xaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    # For y-ticks
    for label in ax.yaxis.get_ticklabels():
        label.set_fontproperties(font)
        label.set_size(14)  # size you want

    plt.tight_layout()

    return fig


# Package Distribution Plots
def package_distribution_plot(col, title, num_packages):
    """
    Create ring plots for the desired package breakdown (retail or membership)
    """
    package_names = data[col].keys()
    package_data = data[col].values()

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

    # Add a legend, where it goes will depend on the number of packages being plotted
    # This is passed in in case of differences between mem/retail
    if num_packages<4:
        legend = ax.legend(
            handles=patches,
            labels=package_names,
            loc="lower center",
            bbox_to_anchor=(0.5, -0.25),
            title=None,
            prop={"family": font},
        )
    else:
        legend = ax.legend(
            handles=patches,
            labels=package_names,
            loc='lower center',
            bbox_to_anchor=(0.5, -0.35),
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


def wash_index_score(value):
    max_value = 150
    fig = go.Figure(
        go.Indicator(
            mode="gauge",
            value=value,
            domain={"x": [0, 1], "y": [0.3, 1]},
            gauge={
                "axis": {
                    "range": [0, max_value],
                    "tickvals": [100],
                    "ticktext": ["100"],
                    "tickfont": {
                        "color": "#003264",
                        "size": 28,
                        "family": "Atlas Grotesk",
                    },
                    "tickwidth": 2,
                },
                "steps": [
                    {"range": [0, value], "color": "#0b75e1"},
                    {"range": [value, max_value], "color": "#F5F5F5"},
                ],
                "bar": {"color": "#0b75e1"},
                "borderwidth": 0,
                "threshold": {
                    "line": {"color": "#ffcb00", "width": 6},
                    "thickness": 1,
                    "value": 100,
                },
            },
        )
    )

    fig.add_trace(
        go.Indicator(
            mode="number",
            value=value,
            number={
                "font": {"size": 50, "family": "Atlas Grotesk", "color": "#0b75e1"}
            },
            domain={"x": [0, 1], "y": [0.45, 0.7]},
        )
    )

    fig.update_layout(
        paper_bgcolor="white",
        width=600,  # Specify the width
        height=400,  # Specify the height
        margin=dict(
            l=10,  # left margin
            r=10,  # right margin
            b=10,  # bottom margin
            t=10,  # top margin
            pad=0,  # padding
        ),
        annotations=[
            dict(
                x=0.5,
                y=0.45,
                showarrow=False,
                text="Wash Count Score",
                font_size=22,
                font_family="Atlas Grotesk",
                font_color="#003264",
                xref="paper",
                yref="paper",
            ),
        ],
    )

    return fig
