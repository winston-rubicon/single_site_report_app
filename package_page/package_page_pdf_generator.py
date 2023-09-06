import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Register custom font
pdfmetrics.registerFont(TTFont('AtlasGrotesk', '../.streamlit/fonts/atlas_grotesk_web_regular_regular.ttf'))

# Defining on-brand colors
navy = colors.Color(0/255., 50/255., 100/255.)
cobalt = colors.Color(11/255., 117/255., 225/255.)
lightgrey = colors.Color(245/255., 245/255., 245/255.)
skyblue = colors.Color(135/255., 206/255., 250/255.)
digitalgold = colors.Color(255/255., 203/255., 0/255.)
printgold = colors.Color(231/255., 175/255., 80/255.)
white = colors.Color(255/255., 255/255., 255/255.)

# Create PDF
width, height = letter
c = canvas.Canvas("../pdfs/package_page_test.pdf", pagesize=letter)

#-----------------------------------------------------------------------------------------


### Header
# TODO: make a function that creates the header, input will be the text
# Page title
c.setFillColor(cobalt)
c.setFont("AtlasGrotesk", 14)
c.drawString(.4*inch, height-.5*inch, 'RETAIL VS MEMBERSHIP')

# NCS logo
c.drawImage("../ncs_logo.png", 5.5*inch, height-.5*inch, width=inch, height=.25*inch)

# Text beside logo
c.setFillColor(cobalt)
c.setFont("AtlasGrotesk", 9)
c.drawString(6.6*inch, height-.5*inch, 'WASH INDEX REPORT')

# Line separating header and rest of text 
c.setStrokeColor(lightgrey)
c.setLineWidth(1)
c.line(.4*inch, height-.6*inch, width-.4*inch, height-.6*inch)


#-----------------------------------------------------------------------------------------


### Plotting the total distributions for the quarter. Two plots evenly spaced on one line
# Define plot height and width
plot_width = 4*inch
plot_height = 3*inch

# Define x and y for plot 1
plot_x = width/2. - plot_width
plot_y = height-4.*inch

# Draw plot 1
c.drawImage("active_arms.png", plot_x, plot_y, width=plot_width, height=plot_height)

# Draw plot 2
c.drawImage("car_counts.png", width -0.5*inch -plot_width, plot_y, width=plot_width, height=plot_height)

# Now onto title, determine centre (euro spelling!) position of title
title_x = width/2.
title_y = plot_y + plot_height +  .1*inch

# Set font for title
c.setFont('AtlasGrotesk', 14)
c.setFillColor(navy)

# Draw title
c.drawCentredString(title_x, title_y, "Quarter Package Distributions")


#-----------------------------------------------------------------------------------------


### First monthly plot
# Define x and y for plot
plot_x = .4*inch
plot_y = height-7.3*inch

# Define plot height and width. These will be constant for all plots(?)
plot_width = 5*inch
plot_height = 3*inch

# Draw plot
c.drawImage("active_armsover_time.png", plot_x, plot_y, width=plot_width, height=plot_height)

# Now onto title, determine center position of title
title_x = plot_x + plot_width/2.
title_y = plot_y + plot_height

# Set font for title
c.setFont('AtlasGrotesk', 14)
c.setFillColor(navy)

# Draw title
c.drawCentredString(title_x, title_y, "Membership Package Distribution Over Time")

# Get top of plot for reference later
plot_top = plot_y + plot_height


#-----------------------------------------------------------------------------------------


### Second monthly plot
# Definey for plot
plot_x = .4*inch
plot_y = height-10.6*inch

# Define plot height and width. These will be constant for all plots(?)
plot_width = 5*inch
plot_height = 3*inch

# Draw plot
c.drawImage("car_countsover_time.png", plot_x, plot_y, width=plot_width, height=plot_height)

# Now onto title, determine center position of title
title_x = plot_x + plot_width/2.
title_y = plot_y + plot_height

# Set font for title
c.setFont('AtlasGrotesk', 14)
c.setFillColor(navy)

# Draw title
c.drawCentredString(title_x, title_y, "Retail Package Distribution Over Time")

# Get bottom of plot for later reference
plot_bottom = plot_y


#-----------------------------------------------------------------------------------------


### Colored text box alongside monthly distributions
c.setFillColor(lightgrey)

# Define x, y, width, height of box
box_x = plot_x + plot_width + 0.5*inch
box_y = plot_bottom + 0.75*inch
box_width = 2*inch
box_height = plot_top - plot_bottom - 1.25*inch
c.rect(box_x, box_y, box_width, box_height, fill=1)

# Preparing for writing the text


#-----------------------------------------------------------------------------------------


### Footer
c.setFillColor(cobalt)
c.setFont('AtlasGrotesk', 8)
c.drawString(.4*inch, .25*inch - 8, 'Car Wash Name')


#-----------------------------------------------------------------------------------------


# Finalize the PDF
c.save()