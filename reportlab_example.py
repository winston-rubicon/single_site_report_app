from reportlab.graphics.shapes import Drawing, Line, LineShape
from reportlab.lib.colors import Color
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import inch, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Frame,
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# Register custom fonts
pdfmetrics.registerFont(
    TTFont("AtlasGrotesk", "branding/fonts/AtlasGrotesk-Regular.ttf")
)
pdfmetrics.registerFont(
    TTFont("AtlasGrotesk-Black", "branding/fonts/AtlasGrotesk-Black.ttf")
)
pdfmetrics.registerFont(
    TTFont("AtlasGrotesk-Thin", "branding/fonts/AtlasGrotesk-Thin.ttf")
)
pdfmetrics.registerFont(
    TTFont("AtlasGrotesk-Bold", "branding/fonts/AtlasGrotesk-Bold.ttf")
)


class FooterCanvas(canvas.Canvas):
    def __init__(
        self,
        *args,
        header_text="Market Evaluation",
        wash_name,
        wash_address,
        date="Quarter 3 2023",
        site_id=1,
        address="213 Elm St, Shelbyville, OH, 44444",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.pages = []
        self.width, self.height = letter
        self.wash_name = wash_name.upper()
        self.wash_address = wash_address.upper()
        self.header_text = header_text
        # TODO: any processing needed to make this Quarter x Year?
        self.date = date
        # TODO: get site_id, address from input
        self.site_id = site_id
        self.address1 = address.split(",")[0]
        self.address2 = (
            address.split(",")[1].strip()
            + ", "
            + address.split(",")[2].strip()
            + " "
            + address.split(",")[3].strip()
        )

        # Define some colors
        self.navy = Color(0 / 255.0, 50 / 255.0, 100 / 255.0)
        self.cobalt = Color(11 / 255.0, 117 / 255.0, 225 / 255.0)
        self.lightgrey = Color(245 / 255.0, 245 / 255.0, 245 / 255.0)
        self.white = Color(255 / 255.0, 255 / 255.0, 255 / 255.0)

    def showPage(self):
        self.pages.append(dict(self.__dict__))
        super()._startPage()

    def save(self):
        page_count = len(self.pages)
        for page in self.pages:
            self.__dict__.update(page)
            if self._pageNumber == 1:
                self.cover_page()
            else:
                self.draw_canvas()
            super().showPage()
        super().save()

    def cover_page(self):
        self.saveState()

        self.drawImage(
            "branding/WashIndexReport_Cover.png",
            0,
            1.6 * inch,
            width=self.width,
            height=self.height - 1.5 * inch,
            preserveAspectRatio=True,
            mask="auto",
        )

        self.drawImage(
            "branding/NCS_White.png",
            0.8 * inch,
            self.height - 1.5 * inch,
            width=1.7 * inch,
            height=0.5 * inch,
            preserveAspectRatio=True,
            mask="auto",
        )

        # Draw transparent box over image
        transparency = (
            0.6  # 0 to 1 where 1 is completely opaque and 0 is completely transparent
        )
        self.setFillColor(self.cobalt)
        self.setStrokeColor(self.cobalt)
        self.setStrokeAlpha(transparency)
        self.setFillAlpha(transparency)  # Set transparency
        rect_bottom = 275
        rect_width = 475
        rect_height = 290
        self.roundRect(0, rect_bottom, rect_width, rect_height, 10, fill=1, stroke=1)

        # Adjust font for text
        self.setFillAlpha(1)
        self.setFillColor(self.white)
        self.setFont("AtlasGrotesk-Black", 44)
        self.drawString(inch, rect_bottom + 2.5 * inch, "Wash Index")
        self.setFont("AtlasGrotesk-Thin", 28)
        self.setFillAlpha(0.5)
        self.drawString(inch + 5, rect_bottom + 2.5 * inch - 40, "R E P O R T")
        # Draw short line
        self.setStrokeColor(self.white)
        self.setStrokeAlpha(0.5)
        self.setLineWidth(1)
        self.line(
            inch + 5, rect_bottom + 1.5 * inch, 2 * inch, rect_bottom + 1.5 * inch
        )
        # Date text
        self.setFillAlpha(1)
        self.setFont("AtlasGrotesk", 20)
        self.drawString(inch + 5, rect_bottom + inch, f"{self.date}")

        # Car wash logo, address, etc. below cover image
        # TODO: pull car wash logo programmatically
        self.drawImage(
            "branding/NCS.png",
            0.5 * inch,
            0.5 * inch,
            width=2 * inch,
            height=0.6 * inch,
            mask="auto",
        )

        # Footer
        self.setFont("AtlasGrotesk", 7)
        # Car wash name, address
        self.setFillColor(self.navy)
        self.drawString(
            30, 15, f"{self.wash_name} \u2022 {self.wash_address} \u2022 {self.date}"
        )
        # Site number, full address
        self.setStrokeColor(self.cobalt)
        self.setStrokeAlpha(1)
        self.line(
            self.width - 0.5 * inch, 0.5 * inch, self.width - 0.5 * inch, 1.05 * inch
        )

        self.setFillColor(self.navy)
        self.setFont("AtlasGrotesk-Bold", 10)
        site_width = stringWidth(f"Site {self.site_id}", "AtlasGrotesk-Bold", 10)
        self.drawString(
            self.width - 0.5 * inch - site_width - 10,
            0.95 * inch,
            f"Site {self.site_id}",
        )
        self.setFont("AtlasGrotesk", 10)
        add1_width = stringWidth(self.address1, "AtlasGrotesk", 10)
        add2_width = stringWidth(self.address2, "AtlasGrotesk", 10)
        self.drawString(
            self.width - 0.5 * inch - add1_width - 10, 0.725 * inch, self.address1
        )
        self.drawString(
            self.width - 0.5 * inch - add2_width - 10, 0.525 * inch, self.address2
        )

        self.restoreState()

    def draw_canvas(self):
        page = "%s" % (self._pageNumber)
        x = 0.4 * inch
        self.saveState()

        # For line below header text
        self.setStrokeColor(self.lightgrey)
        self.setLineWidth(1)

        # Two types of headers
        if self._pageNumber == 2 or self._pageNumber == 7:
            # Large header, text below logo
            # Draw line
            self.line(
                30, self.height - 0.9 * inch, self.width - 30, self.height - 0.9 * inch
            )
            # Change font for header text
            self.setFont("AtlasGrotesk-Bold", 28)
            self.setFillColor(self.cobalt)
            # Capitalize first letters of header text in this case
            self.drawString(30, self.height - 0.9 * inch + 10, self.header_text.title())
            # Draw logo on rhs
            self.drawImage(
                "branding/NCS.png",
                self.width - 1.3 * inch,
                self.height - 0.65 * inch,
                width=inch,
                height=0.25 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
            # Draw text below logo
            self.setFont("AtlasGrotesk-Thin", 8)
            self.setFillColor(self.navy)
            self.drawString(6.8 * inch, self.height - 0.8 * inch, "WASH INDEX REPORT")
            # Draw sprocket
            self.drawImage(
                "branding/GreySprocket.png",
                self.width - 30 - 0.3 * inch,
                self.height - 0.9 * inch - 0.4 * inch,
                width=0.3 * inch,
                height=0.3 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
        else:
            # Small header, text beside logo
            # Draw line
            self.line(
                30, self.height - 0.6 * inch, self.width - 30, self.height - 0.6 * inch
            )
            # Draw logo on rhs
            self.drawImage(
                "branding/NCS.png",
                5.83 * inch,
                self.height - 0.5 * inch,
                width=inch,
                height=0.25 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
            # Change font for text beside logo
            self.setFont("AtlasGrotesk-Black", 8)
            self.setFillColor(self.navy)
            # Draw text beside logo
            self.drawString(6.8 * inch, self.height - 0.5 * inch, "WASH INDEX REPORT")
            # Keep font size but change color for lhs text
            self.setFillColor(self.cobalt)
            # Draw header text
            self.drawString(30, self.height - 0.5 * inch, self.header_text.upper())
            # Draw sprocket
            self.drawImage(
                "branding/GreySprocket.png",
                self.width - 30 - 0.3 * inch,
                self.height - 0.6 * inch - 0.5 * inch,
                width=0.3 * inch,
                height=0.3 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )

        # Footer
        self.line(30, 30, self.width - 30, 30)
        self.setFont("AtlasGrotesk", 7)
        # Page number
        self.setFillColor(self.cobalt)
        self.drawString(self.width - x - 10, 15, page)
        # Car wash name, address
        self.setFillColor(self.navy)
        self.drawString(
            30,
            15,
            f"{self.wash_name} \u2022 {self.address1.upper()} \u2022 {self.date.upper()}",
        )

        self.restoreState()


# Wrapper function for FooterCanvas
def create_footer_canvas_wrapper(wash_name, wash_address):
    def footer_canvas_wrapper(filename, *args, **kwargs):
        return FooterCanvas(
            filename, wash_name=wash_name, wash_address=wash_address, **kwargs
        )

    return footer_canvas_wrapper


class PDFPSReporte:
    def __init__(self, path, wash_name="car wash", wash_address="elm st"):
        self.path = path
        self.styleSheet = getSampleStyleSheet()
        self.elements = []
        self.wash_name = wash_name.upper()
        self.wash_address = wash_address.upper()
        self.custom_wrapper = create_footer_canvas_wrapper(
            self.wash_name, self.wash_address
        )
        self.width, self.height = letter

        # Defining on-brand colors
        self.navy = Color(0 / 255.0, 50 / 255.0, 100 / 255.0)
        self.cobalt = Color(11 / 255.0, 117 / 255.0, 225 / 255.0)
        self.lightgrey = Color(245 / 255.0, 245 / 255.0, 245 / 255.0)
        self.skyblue = Color(135 / 255.0, 206 / 255.0, 250 / 255.0)
        self.digitalgold = Color(255 / 255.0, 203 / 255.0, 0 / 255.0)
        self.printgold = Color(231 / 255.0, 175 / 255.0, 80 / 255.0)
        self.white = Color(255 / 255.0, 255 / 255.0, 255 / 255.0)
        # Need hex codes for XML formatting
        self.hex_navy = self.navy.hexval()[2:]
        self.hex_cobalt = self.cobalt.hexval()[2:]
        self.hex_skyblue = self.skyblue.hexval()[2:]

        # Some commonly used Paragraph styles
        self.plot_title_style = ParagraphStyle(
            "plot_title",
            fontName="AtlasGrotesk-Bold",
            fontSize=14,
            textColor=self.navy,
            alightnment=TA_CENTER,
        )
        self.blue_textbox = TableStyle(
            [
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ROUNDEDCORNERS", [10, 10, 10, 10]),
                ("TOPPADDING", (0,0), (-1,-1), 15),
                ("RIGHTPADDING", (0,0), (-1,-1), 15),
                ("BOTTOMPADDING", (0,0), (-1,-1), 15),
                ("LEFTPADDING", (0,0), (-1,-1), 15),
            ]
        )
        self.grey_textbox = ParagraphStyle(
            "grey_textbox",
            fontName="AtlasGrotesk",
            fontSize=8,
            textColor=self.navy,
            alightnment=TA_CENTER,
            backColor=self.lightgrey,
            borderPadding=(6, 2, 6, 2),
        )

        self.firstPage()
        self.two_plot_box_below_page()

        # Build
        self.doc = SimpleDocTemplate(path, pagesize=letter)
        self.doc.multiBuild(self.elements, canvasmaker=self.custom_wrapper)

    def firstPage(self):
        # Handled by cover page above, for reasons
        self.elements.append(PageBreak())

    def img_paragraph_table(
        self,
        plot_name="figs/revenue_per_car.png",
        plot_title="Revenue Per Car",
        box_text="testing",
    ):
        """
        Create Table flowable with image and textbox side-by-side
        Text will be defined in the fnction calling because of possible XML formatting required in using different colors in the same paragraph.
        TODO: Decide if this has inputs for positioning, might be useful for repeat applications, but may be cumbersome in practice
        """

        xml_text_test = f"""<font face="AtlasGrotesk-Bold" textcolor="#{self.hex_cobalt}">Monthly Membership<br/> Variance: 1.1%<br/>
            Yearly Membership<br/> Variance: 23%<br/></font>
            <font face="AtlasGrotesk" textcolor="#{self.hex_navy}">
            Site 1 sales are weighted heavily on retail over membership. In September, 
            ticket sales were 9% greater than regional retail sales, and 15% greater nationally. 
            Site 1 membership accounts are 17% less than regional memberships, and 10% less than national.</font>"""
        # Create plot title
        # title = Paragraph(plot_title, self.plot_title_style)
        # Create plot
        plot = Image(plot_name, width=5 * inch, height=3 * inch, mask="auto")
        # Create text beside plot
        text = Paragraph(xml_text_test, self.grey_textbox)
        # Lists to put in table
        first_row = [plot_title, ""]
        second_row = [plot, text]
        # Define col widths to allow for Paragraph to fill up space
        col_widths = [5 * inch, 2 * inch]
        # Table's formatted as follows: [[flowable, flowable,...],
        #                                [flowable, flowable,...],...]
        table = Table([first_row, second_row], colWidths=col_widths)
        # Formatting plot title with table attributes to allow text to be centered
        table.setStyle(
            TableStyle(
                [
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("HALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("TEXTCOLOR", (0, 0), (-1, -1), self.navy),
                    ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 14),
                ]
            )
        )
        self.elements.append(table)

    def two_plot_box_below_page(self, text="Insights"):
        test_text = f"""<font face="AtlasGrotesk-Bold" size=14>Insights</font><br/><br/>
        Site 1 volume decreased 16% from the previous month with a year to date average of 8,952
washes per month. This site's average monthly wash count is approximately 5% less than
the regional average and 1% greater than the national average. Site 1 revenue per car gained
3.6% from last month with a difference of +$1.08 per car regionally, and +$2.15 nationally.
"""

        self.elements.append(Spacer(1, 10))
        # First plot
        self.img_paragraph_table()
        self.elements.append(Spacer(1, 10))
        self.img_paragraph_table()
        self.elements.append(Spacer(1, 30))
        para = Paragraph(
            test_text,
            ParagraphStyle(
                "blue_textbox",
                fontName="AtlasGrotesk",
                fontSize=10,
                textColor=self.white,
                backColor=self.cobalt,
                borderPadding=15,
            ),
        )
        insights = Table([[para]], colWidths=[7 * inch])
        insights.setStyle(self.blue_textbox)
        self.elements.append(insights)
        self.elements.append(PageBreak())


if __name__ == "__main__":
    report = PDFPSReporte("psreport.pdf")