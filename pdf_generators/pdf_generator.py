import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

def internal_page():
	# Register custom font
	pdfmetrics.registerFont(TTFont('AtlasGrotesk', 'branding/fonts/atlas_grotesk_web_regular_regular.ttf'))

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
	c = canvas.Canvas("pdfs/internal_page_test.pdf", pagesize=letter)


	#-----------------------------------------------------------------------------------------


	### Header
	# TODO: make a function that creates the header, input will be the text

	# Page title
	c.setFillColor(cobalt)
	c.setFont("AtlasGrotesk", 14)
	c.drawString(.4*inch, height-.5*inch, 'SITE ANALYSIS')

	# NCS logo
	c.drawImage("branding/ncs_logo.png", 5.5*inch, height-.5*inch, width=inch, height=.25*inch)

	# Text beside logo
	c.setFillColor(cobalt)
	c.setFont("AtlasGrotesk", 9)
	c.drawString(6.6*inch, height-.5*inch, 'WASH INDEX REPORT')

	# Line separating header and rest of text 
	c.setStrokeColor(lightgrey)
	c.setLineWidth(1)
	c.line(.4*inch, height-.6*inch, width-.4*inch, height-.6*inch)


	#-----------------------------------------------------------------------------------------


	### First plot
	# Function for drawing the first plot? For now values are hard-coded in, 
	# so it would have to be two separate functions.
	# Should the text alongside it be in the same function???

	# Define x and y for plot, makes determining title position easier. 
	plot_x = .4*inch
	plot_y = height-4.5*inch

	# Define plot height and width. These will be constant for all plots(?)
	plot_width = 5*inch
	plot_height = 3*inch

	#TODO: need to programmatically pull figure names so internal page can be repeated
	# Draw plot
	c.drawImage("figs/revenue_per_car.png", plot_x, plot_y, width=plot_width, height=plot_height)

	# Now onto title, determine center position of title
	title_x = plot_x + plot_width/2.
	title_y = plot_y + plot_height +  .1*inch

	# Set font for title
	c.setFont('AtlasGrotesk', 14)
	c.setFillColor(navy)

	# Draw title
	c.drawCentredString(title_x, title_y, "Revenue Per Car")


	#-----------------------------------------------------------------------------------------


	### Text"box" alongside plot
	# This could be a function called inside of the plotting function, since its position will 
	# depend on the plot's position

	# Determine x and y of title string
	box_x = plot_x + plot_width + 0.4*inch
	box_y = plot_y + plot_height - 0.5*inch

	# Change font size and color
	c.setFont("AtlasGrotesk", 14)
	c.setFillColor(cobalt)
	c.drawString(box_x, box_y, "Q3 Average RPC")

	# Revert font size and color back to default
	c.setFont("AtlasGrotesk", 12)
	c.setFillColor(navy)

	# Insert remaining text on the right
	c.drawString(box_x, box_y - 0.5*inch, "Data point 1: 4")
	c.drawString(box_x, box_y - 1.0*inch, "Data point 2: 9")
	c.drawString(box_x, box_y - 1.5*inch, "Data point 3: 16")


	#-----------------------------------------------------------------------------------------


	### Second plot
	# Function for drawing the first plot? For now values are hard-coded in, 
	# so it would have to be two separate functions.
	# Should the text alongside it be in the same function???

	# Just need to define y in this case. Plot itself will be 3 inches, allow space for the title as well
	plot_y -= 4.0*inch

	#TODO: need to programmatically pull figure names so internal page can be repeated
	# Draw plot
	c.drawImage("figs/yoy_revenue_per_car.png", plot_x, plot_y, width=plot_width, height=plot_height)

	# Now onto title, determine center position of title
	title_y = plot_y + plot_height +  .1*inch

	# Set font for title
	c.setFont('AtlasGrotesk', 14)
	c.setFillColor(navy)

	# Draw title
	c.drawCentredString(title_x, title_y, "YoY Revenue Per Car")


	#-----------------------------------------------------------------------------------------
    
	### Text"box" alongside plot 2
	# This could be a function called inside of the plotting function, since its position will 
	# depend on the plot's position

	# Determine y
	box_y = plot_y + plot_height - 0.5*inch

	# Change font size and color
	c.setFont("AtlasGrotesk", 14)
	c.setFillColor(cobalt)
	c.drawString(box_x, box_y, "Quarterly RPC % Change")

	# Revert font size and color back to default
	c.setFont("AtlasGrotesk", 12)
	c.setFillColor(navy)

	# Insert remaining text on the right
	c.drawString(box_x, box_y - 0.5*inch, "Data point 1: 4")
	c.drawString(box_x, box_y - 1.0*inch, "Data point 2: 9")
	c.drawString(box_x, box_y - 1.5*inch, "Data point 3: 16")


	#-----------------------------------------------------------------------------------------


	# Draw colored text box at the bottom
	c.setFillColor(cobalt)
	c.rect(0.5*inch, 0.5*inch, 7.5*inch, 1.5*inch, fill=1)
	# The box is 7" wide, 1" tall, and .5" from the sides
	# Preparing for writing the text
	c.setFillColor(white)
	text_size = 10
	c.setFont("AtlasGrotesk", text_size)
	# Going to default to three lines of text, shouldn't be too difficult to automate
	lines = ['- this is line 1', '- this is line 2', '- this is line 3']
	# All will start at same x, a loop will handle the change in y
	line_x = 1.0*inch
	line_y = 2.0*inch - 0.5*inch + .8*text_size
	for line in lines:
		# Draw line
		c.drawString(line_x, line_y, line)
		# Shift down a third of the box, and half the text size.
		line_y = line_y - 0.5*inch + .8*text_size


	#-----------------------------------------------------------------------------------------


	### Footer
	c.setFillColor(cobalt)
	c.setFont('AtlasGrotesk', 8)
	c.drawString(.4*inch, .25*inch - 8, 'Car Wash Name')


	# Finalize the PDF
	c.save()

def package_page():
    # Register custom font
    pdfmetrics.registerFont(TTFont('AtlasGrotesk', 'branding/fonts/atlas_grotesk_web_regular_regular.ttf'))

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
    c = canvas.Canvas("pdfs/package_page_test.pdf", pagesize=letter)

    #-----------------------------------------------------------------------------------------


    ### Header
    # TODO: make a function that creates the header, input will be the text
    # Page title
    c.setFillColor(cobalt)
    c.setFont("AtlasGrotesk", 14)
    c.drawString(.4*inch, height-.5*inch, 'RETAIL VS MEMBERSHIP')

    # NCS logo
    c.drawImage("branding/ncs_logo.png", 5.5*inch, height-.5*inch, width=inch, height=.25*inch)

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
    c.drawImage("figs/active_arms.png", plot_x, plot_y, width=plot_width, height=plot_height)

    # Draw plot 2
    c.drawImage("figs/car_counts.png", width -0.5*inch -plot_width, plot_y, width=plot_width, height=plot_height)

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
    c.drawImage("figs/active_armsover_time.png", plot_x, plot_y, width=plot_width, height=plot_height)

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
    c.drawImage("figs/car_countsover_time.png", plot_x, plot_y, width=plot_width, height=plot_height)

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