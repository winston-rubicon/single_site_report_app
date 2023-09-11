### Jotting down ideas for what a class that handles the pdf generation would look like

import matplotlib.pyplot as plt

# Imports for creating pdfs
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# Imports for combining pdfs
from PyPDF2 import PdfReader, PdfWriter
import glob


class PageWriter():

	def __init__(self, page_name):
		# Would open the page to be open at this point, define the height and width and whatnot.
		self.page = canvas.Canvas(page_name, pagesize=letter)
		self.width, self.height = letter

		# Maybe the header font size/color, regular font size/color... could all be initialized here
		# Also define all of the brand colors here
		# Defining on-brand colors
		self.navy = colors.Color(0/255., 50/255., 100/255.)
		self.cobalt = colors.Color(11/255., 117/255., 225/255.)
		self.lightgrey = colors.Color(245/255., 245/255., 245/255.)
		self.skyblue = colors.Color(135/255., 206/255., 250/255.)
		self.digitalgold = colors.Color(255/255., 203/255., 0/255.)
		self.printgold = colors.Color(231/255., 175/255., 80/255.)
		self.white = colors.Color(255/255., 255/255., 255/255.)


	# def internal_page():
	# 	# Can put the code necessary for an internal page
	# 	# Does it even make sense to do this or does it just make sense to make the class with all of the annoying stuff initialized?


	# def package_page():
	# 	# Is it worth it if this is just going to be made once? Maybe yes just to have it all in one place?

	# def wash_index_page():
	# 	# This page will be so much easier than in the word document!

	def header(self, header_text):
		# Helper functions like this will be nice and easy!
		# Page title
		self.page.setFillColor(self.cobalt)
		self.page.setFont("AtlasGrotesk", self.header_size)
		self.page.drawString(.4*inch, self.height-.5*inch, header_text)

		# NCS logo
		self.page.drawImage("../ncs_logo.png", 5.5*inch, self.height-.5*inch, width=inch, height=.25*inch)

		# Text beside logo
		self.page.setFillColor(cobalt)
		self.page.setFont("AtlasGrotesk", 9)
		self.page.drawString(6.6*inch, self.height-.5*inch, 'WASH INDEX REPORT')

		# Line separating header and rest of text 
		self.page.setStrokeColor(self.lightgrey)
		self.page.setLineWidth(1)
		self.page.line(.4*inch, self.height-.6*inch, self.width-.4*inch, self.height-.6*inch)


	def combine_pdfs(self):
		# List of PDFs to combine
		pdf_files = glob.glob('pdfs/*.pdf')
	
		# Create a PdfWriter object
		pdf_writer = PdfWriter()

		# Loop through the list of PDFs
		for pdf_file in pdf_list:
			# Create a PdfReader object
			pdf_reader = PdfReader(pdf_file)
			
			# Add each page in the current PDF to the writer object
			for page_num in range(len(pdf_reader.pages)):
				page = pdf_reader.pages[page_num]
				pdf_writer.add_page(page)

		# Create the output PDF
		with open(output_pdf, "wb") as out_pdf_file:
			pdf_writer.write(out_pdf_file)

		# Output PDF file name
		output_pdf = "output/combined_test.pdf"

		# Combine PDFs
		combine_pdfs(sorted(pdf_files), output_pdf)









