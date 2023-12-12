###----- Imports
import json
from report_functions import report_functions as rf
from report_functions import pdf_generator as pg
import plotly.io as pio
from io import BytesIO
import os
import boto3
import us

filename = os.environ.get('FILENAME')
bucket_name = os.environ.get('BUCKET_NAME')
s3 = boto3.client('s3')
s3_object = s3.get_object(Bucket=bucket_name, Key=filename)
file_content = s3_object['Body'].read().decode('utf-8')
data = json.loads(file_content)

# bucket_name = "ncs-washindex-single-site-reports-815867481426"
# filename = "fake_data/10_2023_fake.json"
# with open(filename, "r") as f:
#     data = json.load(f)

site_number = data["site_number"]

# Current month, year
month_year = filename.split("/")[-1].split(".")[0]
current_month = int(month_year.split("_")[0])
current_year = int(month_year.split("_")[1])

# Regional info
state = us.states.lookup(data['site_state'])
division = f"{data['division'][f'{current_month}_{current_year}']} Division"
region = data['region'][f'{current_month}_{current_year}']

# Dictionary that will be used in pdf generation code
plots_for_pdf = {}

# Brand color palette
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

###----- Generating figures, storing in buffers in dictionary

### Wash Counts
# Define column, ylabel to be used for plot
col = "total_wash_count"
ylabel = "Number of Washes"
legend_labels = [f'Site {site_number}', 'Avg of Other Sites']
# Plot, save fig to buffer and put in dictionary
try:
    fig = rf.multi_line_plot(cols=[col,'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
wash_counts_fig = rf.save_plot(fig)
plots_for_pdf["total_wash_counts"] = wash_counts_fig

### Revenue Per Car
col = "revenue_per_car"
ylabel = "Revenue Per Car ($)"
try:
    fig = rf.multi_line_plot(cols=[col,'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
rpc_fig = rf.save_plot(fig)
plots_for_pdf["revenue_per_car"] = rpc_fig

### Retail vs. Membership Sales
cols = ["retail_wash_count", "membership_wash_count"]
ylabel = "Number of Washes"
legend_labels = ["Retail", "Membership"]
fig = rf.year_bar_plot(cols=cols, ylabel=ylabel, legend_labels=legend_labels)
retail_memberships_plot = rf.save_plot(fig)
plots_for_pdf["retail_membership_distribution"] = retail_memberships_plot

### Membership RPC
col = "membership_rpc"
ylabel = "Average Revenue ($)"
legend_labels = [f'Site {site_number}', 'Avg of Other Sites']
try:
    fig = rf.multi_line_plot(cols=[col,'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
membership_rpc_plot = rf.save_plot(fig)
plots_for_pdf["membership_rpc"] = membership_rpc_plot

### Retail RPC
col = "retail_rpc"
ylabel = "Average Revenue ($)"
try:
    fig = rf.multi_line_plot(cols=[col, 'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
retail_rpc_plot = rf.save_plot(fig)
plots_for_pdf["retail_rpc"] = retail_rpc_plot

### Churn Rate
col = "churn_rate"
ylabel = "Churn %"
try:
    fig = rf.multi_line_plot(cols=[col, 'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
churn_plot = rf.save_plot(fig)
plots_for_pdf["churn_rate"] = churn_plot

### Capture Rate
col = "capture_rate"
ylabel = "Capture %"
try:
    fig = rf.multi_line_plot(cols=[col, 'hub_avg_'+col], ylabel=ylabel, legend_labels=legend_labels)
except:
    fig = rf.line_plot(col=col, ylabel=ylabel)
capture_plot = rf.save_plot(fig)
plots_for_pdf["capture_rate"] = capture_plot

### Popular Days
col = "popular_days"
ylabel = "Average Counts"
fig = rf.variable_bar_plot(col=col, ylabel=ylabel)
popular_days_plot = rf.save_plot(fig)
plots_for_pdf["popular_days"] = popular_days_plot

### Popular Hours
col = "popular_hours"
ylabel = "Average Counts"
fig = rf.variable_bar_plot(col=col, ylabel=ylabel)
popular_hours_plot = rf.save_plot(fig)
plots_for_pdf["popular_hours"] = popular_hours_plot

### Optimal Weather Days
col = "optimal_weather_days"
ylabel = "Days"
# fig = rf.line_plot(col=col, ylabel=ylabel)
fig = rf.year_bar_plot(cols=[col], ylabel=ylabel)
optimal_weather_days_plot = rf.save_plot(fig)
plots_for_pdf["optimal_weather_days"] = optimal_weather_days_plot

### Washes Per Optimal Weather day
col = "washes_per_optimal_day"
ylabel = "Car Washes Per Optimal Day"
# fig = rf.line_plot(col=col, ylabel=ylabel)
fig = rf.year_bar_plot(cols=[col], ylabel=ylabel)
washes_per_optimal_day_plot = rf.save_plot(fig)
plots_for_pdf["washes_per_optimal_day"] = washes_per_optimal_day_plot

##### Package Distributions
if len(data['retail_package_distribution'].keys()) >= len(data['membership_package_distribution'].keys()):
    num_packages = len(data['retail_package_distribution'].keys())
    color_dict = dict(zip(data['retail_package_distribution'].keys(), color_palette))
else:
    num_packages = len(data['membership_package_distribution'].keys())
    color_dict = dict(zip(data['membership_package_distribution'].keys(), color_palette))
### Retail Package Distribution
col = "retail_package_distribution"
title = "Retail Package\nDistribution"
fig = rf.package_distribution_plot(col=col, title=title, num_packages=num_packages, color_dict=color_dict)
retail_package_plot = rf.save_plot(fig)
plots_for_pdf["retail_package_distribution"] = retail_package_plot

### Retail package distribution over time
col = "monthly_retail_package_distribution"
ylabel = "Distribution (%)"
legend_labels = data[col].keys()
fig = rf.line_plot(col=col, ylabel=ylabel, legend_labels=legend_labels)
retail_monthly_package_plot = rf.save_plot(fig)
plots_for_pdf["retail_monthly_package_distribution"] = retail_monthly_package_plot

### Membership Package Distribution
col = "membership_package_distribution"
title = "Active Membership \nDistribution"
fig = rf.package_distribution_plot(col=col, title=title, num_packages=num_packages, color_dict=color_dict)
membership_package_plot = rf.save_plot(fig)
plots_for_pdf["membership_package_distribution"] = membership_package_plot

### Membership package distribution over time
col = "monthly_membership_package_distribution"
ylabel = "Distribution (%)"
legend_labels = data[col].keys()
fig = rf.line_plot(col=col, ylabel=ylabel, legend_labels=legend_labels)
membership_monthly_package_plot = rf.save_plot(fig)
plots_for_pdf[
    "membership_monthly_package_distribution"
] = membership_monthly_package_plot

### Wash Index Score
value = data["wash_index_score"]["score"]
fig = rf.wash_index_score(value)
# Have to save plotly graphs in special manner
wash_index_score = BytesIO()
pio.write_image(fig, wash_index_score, format="png")
plots_for_pdf["wash_index_score"] = wash_index_score

### CPI YoY
cols = ['cpi_yoy_region', 'cpi_yoy_national']
ylabel = 'Percent Change (%)'
legend_labels = [division, 'U.S. City Average']
fig = rf.multi_line_plot(cols=cols, ylabel=ylabel, legend_labels=legend_labels)
cpi_plot = rf.save_plot(fig)
plots_for_pdf['cpi_yoy'] = cpi_plot

### Unemployment
cols = ['region_unemployment', 'national_unemployment']
ylabel = 'Rate (%)'
legend_labels = [state, 'National']
fig = rf.multi_line_plot(cols=cols, ylabel=ylabel, legend_labels=legend_labels)
unemploy_plot = rf.save_plot(fig)
plots_for_pdf['unemployment'] = unemploy_plot

### Traffic
cols = ['traffic_yoy_regional', 'traffic_yoy_national']
ylabel = 'Percent Change (%)'
legend_labels = [state, 'National']
fig = rf.multi_line_plot(cols=cols, ylabel=ylabel, legend_labels=legend_labels)
traffic_plot = rf.save_plot(fig)
plots_for_pdf['traffic'] = traffic_plot

### Gas Prices
cols = ['gas_regional', 'gas_national']
ylabel = 'Price ($)'
legend_labels = [division, 'U.S. City Average']
fig = rf.multi_line_plot(cols=cols, ylabel=ylabel, legend_labels=legend_labels)
gas_plot = rf.save_plot(fig)
plots_for_pdf['gas'] = gas_plot


###------------------------- INPROGRESS - Pie chart for feature importances
color_dict = dict(zip(data['feature_importances'].keys(), color_palette))
fig = rf.package_distribution_plot(col='feature_importances', title=None, num_packages=None, color_dict=color_dict)
feat_plot = rf.save_plot(fig)
plots_for_pdf['feature_importances'] = feat_plot
###-------------------------

pdf_class = pg.SingleSiteReport(
    plot_dict=plots_for_pdf,
    data_dict=data,
    current_year=current_year,
    current_month=current_month,
)

pdf = pdf_class.return_pdf()

# rf.save_to_s3(
#     bucket_name,
#     f"{data['hub_id']}/{site_number}/reports/{data['hub_name'].replace(' ','_')}_Site_{site_number}_monthly_report_{current_month}_{current_year}.pdf",
#     pdf,
# )


### Saving to file locally, comment out when going to production
file_path = 'test.pdf'
# Write the BytesIO content to a file
with open(file_path, 'wb') as file:
    file.write(pdf.getvalue())