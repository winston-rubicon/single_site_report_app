from reportlab.lib.colors import Color
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import inch, letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import (
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    ListFlowable,
    ListItem,
)
from io import BytesIO
import pandas as pd
import report_functions.report_functions_orig as rf
import report_functions.sql_queries as sq

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
        address_df,
        date="Quarter 3 2023",
        address="213 Elm St, Shelbyville, OH, 44444",
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.pages = []
        self.width, self.height = letter
        self.site_id = address_df['hub_site'].unique()[0]
        self.wash_name = address_df['hub_name'].values[0]
        self.header_text = header_text
        # TODO: any processing needed to make this Quarter x Year?
        self.date = date
        self.address1 = address_df['address'].values[0]
        self.address2 = f"{address_df['city'].values[0]}, {address_df['state'].values[0]} {address_df['zip'].values[0]}"

        # Define some colors
        self.navy = Color(0 / 255.0, 50 / 255.0, 100 / 255.0)
        self.cobalt = Color(11 / 255.0, 117 / 255.0, 225 / 255.0)
        self.lightgrey = Color(245 / 255.0, 245 / 255.0, 245 / 255.0)
        self.white = Color(255 / 255.0, 255 / 255.0, 255 / 255.0)
        self.grey = Color(142.0 / 255.0, 142.0 / 255.0, 142.0 / 255.0)

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
        self.drawImage(
            "branding/NCS_Cobalt.png",
            0.5 * inch,
            0.5 * inch,
            width=2 * inch,
            height=0.6 * inch,
            mask="auto",
        )

        # Footer
        self.setFont("AtlasGrotesk", 7)
        # Car wash name, address
        self.setFillColor(self.grey)
        self.drawString(
            30, 15, f"{self.wash_name.upper()} \u2022 {self.address1.upper()} \u2022 {self.date.upper()}"
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

        if self._pageNumber < 7:
            self.header_text = "Market Evaluation"
        elif self._pageNumber < 10:
            self.header_text = "Index"

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
                "branding/NCS_Cobalt.png",
                self.width - 1.3 * inch,
                self.height - 0.65 * inch,
                width=inch,
                height=0.25 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
            # Draw text below logo
            self.setFont("AtlasGrotesk-Thin", 8)
            self.setFillColor(self.grey)
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
                "branding/NCS_Cobalt.png",
                5.83 * inch,
                self.height - 0.5 * inch,
                width=inch,
                height=0.25 * inch,
                preserveAspectRatio=True,
                mask="auto",
            )
            # Change font for text beside logo
            self.setFont("AtlasGrotesk", 8)
            self.setFillColor(self.grey)
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
        self.setFillColor(self.grey)
        self.drawString(
            30,
            15,
            f"{self.wash_name.upper()} \u2022 {self.address1.upper()} \u2022 {self.date.upper()}",
        )

        self.restoreState()


# Wrapper function for FooterCanvas
def create_footer_canvas_wrapper(address_df):
    def footer_canvas_wrapper(filename, *args, **kwargs):
        return FooterCanvas(
            filename, address_df=address_df, **kwargs
        )

    return footer_canvas_wrapper


class PDFPSReporte:
    def __init__(
        self,
        plot_dict,
        full_df,
        mem_df,
        retail_df,
        days_df,
        hours_df,
        weather_grouped_df,
        weather_wash_df,
        hub_id=1,
        site_id=1,
        current_year=2023,
        quarter=2,
        path="psreport.pdf",
        wash_name="car wash",
        wash_address="elm st",
    ):
        self.plot_dict = plot_dict
        self.full_df = full_df
        self.mem_df = mem_df
        self.retail_df = retail_df
        self.days_df = days_df
        self.hours_df = hours_df
        self.weather_grouped_df = weather_grouped_df
        self.weather_wash_df = weather_wash_df
        self.hub_id = hub_id
        self.site_id = site_id
        self.current_year = current_year
        self.quarter = quarter
        self.path = path
        self.elements = []

        self.address_df = sq.get_address(hub_id=self.hub_id, site_id=self.site_id)

        self.custom_wrapper = create_footer_canvas_wrapper(self.address_df)
        self.width, self.height = letter

        self.current_df = self.full_df[
            (self.full_df["quarter"] == self.quarter)
            & (self.full_df["year"] == self.current_year)
        ]

        # Defining on-brand colors
        self.navy = Color(0 / 255.0, 50 / 255.0, 100 / 255.0)
        self.cobalt = Color(11 / 255.0, 117 / 255.0, 225 / 255.0)
        self.lightgrey = Color(245 / 255.0, 245 / 255.0, 245 / 255.0)
        self.skyblue = Color(135 / 255.0, 206 / 255.0, 250 / 255.0)
        self.digitalgold = Color(255 / 255.0, 203 / 255.0, 0 / 255.0)
        self.printgold = Color(231 / 255.0, 175 / 255.0, 80 / 255.0)
        self.white = Color(255 / 255.0, 255 / 255.0, 255 / 255.0)
        self.grey = Color(142.0 / 255.0, 142.0 / 255.0, 142.0 / 255.0)
        # Need hex codes for XML formatting
        self.hex_navy = self.navy.hexval()[2:]
        self.hex_cobalt = self.cobalt.hexval()[2:]
        self.hex_skyblue = self.skyblue.hexval()[2:]
        self.hex_grey = "8e8e8e"

        # Some commonly used Table/Paragraph styles
        self.rounded_corners = TableStyle(
            [
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("ROUNDEDCORNERS", [10, 10, 10, 10]),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("BACKGROUND", (0, 0), (-1, -1), self.lightgrey),
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

        # Making the PDF
        self.firstPage()
        self.two_plot_box_below_page(
            plots=[
                self.plot_dict["total_wash_counts"],
                self.plot_dict["revenue_per_car"],
            ],
            plot_titles=["Total Volume", "Revenue Per Car"],
            page=2,
        )
        self.membership_vs_retail()
        self.package_distribution()
        self.two_plot_box_below_page(
            plots=[plot_dict["churn_rate"], plot_dict["capture_rate"]],
            plot_titles=["Churn Rate", "Capture Rate"],
            page=5,
        )
        self.popular_days_hours()
        self.wash_index()

        # Build
        self.pdf = BytesIO()
        self.doc = SimpleDocTemplate(self.pdf, pagesize=letter)
        self.doc.multiBuild(self.elements, canvasmaker=self.custom_wrapper)
        self.pdf.seek(0)

    def bulleted_text(self, title_text, bullets: list):
        """
        Returns ListFlowable of text to be rendered as multi-colored bulleted list,
        with descriptive title. body_text must be a list of bullets to use.
        """
        colors = [self.hex_navy, self.hex_skyblue, self.hex_cobalt]
        formatted_title = Paragraph(
            f"""<font face=AtlasGrotesk-Bold size=10 color="#{self.hex_navy}">{title_text}</font><br/><br/>"""
        )

        bullet_items = []

        for color, bullet in zip(colors, bullets):
            bullet_paragraph = Paragraph(
                f"""<font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{bullet}</font>""",
                ParagraphStyle("blah"),
            )
            bullet_item = ListItem(
                bullet_paragraph,
                bulletColor=f"#{color}",
                bulletFontSize=24,
                bulletOffsetY=10,
            )
            bullet_items.append(bullet_item)

        bullet_list = ListFlowable(bullet_items, bulletType="bullet")
        return [formatted_title, bullet_list]

    def firstPage(self):
        # Handled by cover page above, for reasons
        self.elements.append(PageBreak())

    def img_paragraph_table(
        self,
        plot="figs/revenue_per_car.png",
        plot_title="Revenue Per Car",
        box_text="testing",
        plot_height=3 * inch,
        aspect_ratio=5.0 / 3.0,
    ):
        """
        Create Table flowable with image and textbox side-by-side
        Text will be defined in the function calling because of possible XML formatting required in using different colors in the same paragraph.
        """

        # Create plot
        plot_width = plot_height * aspect_ratio
        plot = Image(plot, width=plot_width, height=plot_height, mask="auto")
        # Create text beside plot
        inner_table = Table([[box_text]], colWidths=[2 * inch])
        inner_table.setStyle(self.rounded_corners)
        # Lists to put in table
        first_row = [plot_title, "", inner_table]
        second_row = [plot, "", ""]
        # Define col widths to allow for Table to fill up space
        col_widths = [5 * inch, 20, 2 * inch]
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
                    ("SPAN", (2, 0), (2, -1)),
                ]
            )
        )
        self.elements.append(table)

    def two_plot_box_below_page(self, plots, plot_titles, text="Insights", page=2):
        if page == 2:
            avg_vol = round(self.current_df["total_wash_counts"].mean())
            avg_rev = round(self.current_df["total_wash_revenue"].mean())
            qoq_vol_rev = rf.site_qoq(
                self.full_df, ["total_wash_counts", "total_wash_revenue"]
            )
            qoq_vol = qoq_vol_rev["qoq_total_wash_counts"].values[0]
            qoq_rev = qoq_vol_rev["qoq_total_wash_revenue"].values[0]

            first_box_title = "Quarter Average Volume"
            first_box_text = [f"Site {self.site_id}: {avg_vol:,}"]
            first_box = self.bulleted_text(first_box_title, first_box_text)

            second_box_title = "Quarter Average Revenue"
            second_box_text = [f"Site {self.site_id}: ${avg_rev:,}"]
            second_box = self.bulleted_text(second_box_title, second_box_text)

            lower_box = f"""
            <font face="AtlasGrotesk-Bold" size=14>Insights</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total wash volume changed by {round(qoq_vol, 1)}% compared to last quarter</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total revenue changed by {round(qoq_rev, 1)}%</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Some regional and national comparison</font><br/>
            """

        elif page == 5:
            avg_churn = round(self.current_df["churn_rate"].mean(), 1)
            avg_capture = round(self.current_df["capture_rate"].mean(), 1)
            qoq_churn_capture = rf.site_qoq(
                self.full_df, col=["churn_rate", "capture_rate"]
            )
            qoq_churn = qoq_churn_capture["qoq_churn_rate"].values[0]
            qoq_capture = qoq_churn_capture["qoq_capture_rate"].values[0]

            first_box_title = "Quarter Average Churn Rate"
            first_box_text = [f"Site {self.site_id}: {avg_churn}%"]
            first_box = self.bulleted_text(first_box_title, first_box_text)

            second_box_title = "Quarter Average Capture Rate"
            second_box_text = [f"Site {self.site_id}: {avg_capture}%"]
            second_box = self.bulleted_text(second_box_title, second_box_text)
            # TODO: fill in lower text box, if necessary
            lower_box = f"""
            <font face="AtlasGrotesk-Bold" size=14>Insights</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Average churn rate changed by {round(qoq_churn, 1)}% compared to last quarter</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Average capture rate changed by {round(qoq_capture, 1)}%</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Some regional and national comparison</font><br/>
            """

        self.elements.append(Spacer(1, 10))
        # First plot
        self.img_paragraph_table(
            plot=plots[0], plot_title=plot_titles[0], box_text=first_box
        )
        self.elements.append(Spacer(1, 10))
        self.img_paragraph_table(
            plot=plots[1], plot_title=plot_titles[1], box_text=second_box
        )
        self.elements.append(Spacer(1, 30))
        para = Paragraph(
            lower_box,
            ParagraphStyle(
                "blue_textbox",
                fontName="AtlasGrotesk",
                fontSize=10,
                textColor=self.white,
                backColor=self.cobalt,
                borderPadding=10,
                leading=20,
            ),
        )
        insights = Table([[para]], colWidths=[7 * inch])
        insights.setStyle(self.rounded_corners)
        self.elements.append(insights)
        self.elements.append(PageBreak())

    def membership_vs_retail(self):
        table_title = "Membership Distribution"
        table_style = TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TEXTCOLOR", (0, 0), (-1, -1), self.navy),
                ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 14),
            ]
        )
        plot = Image(
            self.plot_dict["retail_membership_distribution"],
            width=6 * inch,
            height=2 * inch,
        )
        table = Table([[table_title], [plot]])
        table.setStyle(table_style)
        self.elements.append(table)

        self.elements.append(Spacer(1, 20))

        # Set text for average membership rpc
        mem_rpc = round(self.current_df["arm_average_ticket"].mean(), 2)
        title = "Quarter Average Membership RPC"
        text = [f"Site {self.site_id}: ${mem_rpc}"]
        bullets = self.bulleted_text(title, text)

        self.img_paragraph_table(
            plot=self.plot_dict["membership_rpc"],
            plot_title="Membership Revenue Per Car",
            box_text=bullets,
            plot_height=2.5 * inch,
        )

        self.elements.append(Spacer(1, 10))

        # Set text for average retail rpc
        retail_rpc = round(self.current_df["retail_ticket_average"].mean(), 2)
        title = "Quarter Average Retail RPC"
        text = [f"Site {self.site_id}: ${retail_rpc}"]
        bullets = self.bulleted_text(title, text)

        self.img_paragraph_table(
            plot=self.plot_dict["retail_rpc"],
            plot_title="Retail Revenue Per Car",
            box_text=bullets,
            plot_height=2.5 * inch,
        )

        self.elements.append(PageBreak())

    def package_distribution(self):
        img_width = 4 * inch
        retail_packages = Image(
            self.plot_dict["retail_package_distribution"],
            width=img_width,
            height=3 * inch,
        )
        membership_packages = Image(
            self.plot_dict["membership_package_distribution"],
            width=img_width,
            height=3 * inch,
        )

        package_table = Table(
            [[retail_packages, membership_packages]],
            colWidths=[img_width, img_width],
        )
        self.elements.append(package_table)

        # Creating a table with four rows in first column and one row in second column as an all-encompassing textbox
        img_height = 2.5 * inch
        img_width = img_height * (5.0 / 3.0)

        membership_packages_vs_time = Image(
            self.plot_dict["membership_monthly_package_distribution"],
            width=img_width,
            height=img_height,
        )
        retail_packages_vs_time = Image(
            self.plot_dict["retail_monthly_package_distribution"],
            width=img_width,
            height=img_height,
        )

        # Pulling out max/min quarterly package shifts
        qoq_mem = rf.qoq_package(
            self.mem_df.drop(columns=["date"]), col="active_arms", threshold=5
        )
        max_mem_shift = qoq_mem["qoq"].max()
        max_mem_package = qoq_mem[qoq_mem["qoq"] == max_mem_shift]["name"].values[0]
        min_mem_shift = qoq_mem["qoq"].min()
        min_mem_package = qoq_mem[qoq_mem["qoq"] == min_mem_shift]["name"].values[0]

        qoq_retail = rf.qoq_package(
            self.retail_df.drop(columns=["date"]), col="car_counts"
        )
        max_retail_shift = qoq_retail["qoq"].max()
        max_retail_package = qoq_retail[qoq_retail["qoq"] == max_retail_shift][
            "name"
        ].values[0]
        min_retail_shift = qoq_retail["qoq"].min()
        min_retail_package = qoq_retail[qoq_retail["qoq"] == min_retail_shift][
            "name"
        ].values[0]

        # Defining a Table with a Paragraph in order to be able to use rounded corners
        # as well as XML formatting in the text
        text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Quarterly Notable Package Distribution Shifts</font><br/><br/>
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}"><u>Membership</u></font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_mem_package}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">+{round(max_mem_shift,1)}%</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_mem_package}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">{round(min_mem_shift,1)}%</font><br/><br/>
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}"><u>Retail</u></font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_retail_package}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">+{round(max_retail_shift,1)}%</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_retail_package}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">{round(min_retail_shift,1)}%</font><br/>
        """
        para = Paragraph(text, self.grey_textbox)
        inner_table = Table([[para]], colWidths=[2 * inch])
        inner_table.setStyle(self.rounded_corners)
        row1 = [
            "Membership Package Distribution Over Time",
            "",
            inner_table,
        ]  # Has to go in top left of spanned cells
        row2 = [membership_packages_vs_time, "", ""]
        row3 = ["Retail Package Distribution Over Time", "", ""]
        row4 = [retail_packages_vs_time, "", ""]

        table = Table(
            [row1, row2, row3, row4], colWidths=[img_width, 0.5 * inch, 2 * inch]
        )
        table.setStyle(
            TableStyle(
                [
                    ("TEXTCOLOR", (0, 0), (0, -1), self.navy),
                    ("TEXTCOLOR", (1, 0), (1, -1), self.cobalt),
                    ("ALIGN", (1, 0), (1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (0, -1), 12),
                    ("SPAN", (2, 0), (2, -1)),
                ]
            )
        )

        self.elements.append(table)

        self.elements.append(PageBreak())

    def popular_days_hours(self):
        # Getting most/least popular days of the quarter
        avg_days_df = (
            self.days_df.groupby(["day_of_week", "hub_site", "quarter", "year"])
            .mean()
            .reset_index()
        )
        max_day = avg_days_df.max()["day_of_week"]
        max_day_count = avg_days_df.max()["day_of_week_counts"]
        min_day = avg_days_df.min()["day_of_week"]
        min_day_count = avg_days_df.min()["day_of_week_counts"]
        days_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Most/Least Popular Days on Average in Current Quarter</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_day}: {round(max_day_count)}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_day}: {round(min_day_count)}</font><br/>
        """
        days_text = Paragraph(days_text, self.grey_textbox)

        # Getting most/least popular hours of the quarter
        avg_hours_df = (
            self.hours_df.groupby(["hour", "hub_site", "quarter", "year"])
            .mean()
            .reset_index()
        )
        avg_hours_df = avg_hours_df[
            (avg_hours_df["quarter"] == self.quarter)
            & (avg_hours_df["year"] == self.current_year)
            & (avg_hours_df["hub_site"] == self.site_id)
            & (avg_hours_df["hour_counts"] > 5)
        ]
        max_hour = avg_hours_df.max()["hour"]
        max_hour = (
            str(max_hour) + " AM" if max_hour <= 12 else str(max_hour % 12) + " PM"
        )
        max_hour_count = avg_hours_df.max()["hour_counts"]
        min_hour = avg_hours_df.min()["hour"]
        min_hour = (
            str(min_hour) + " AM" if min_hour <= 12 else str(min_hour % 12) + " PM"
        )
        min_hour_count = avg_hours_df.min()["hour_counts"]
        hours_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Most/Least Popular Hours on Average in Current Quarter</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_hour}: {round(max_hour_count)}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_hour}: {round(min_hour_count)}</font><br/>
        """
        hours_text = Paragraph(hours_text, self.grey_textbox)

        self.elements.append(Spacer(1, 0.75 * inch))
        self.img_paragraph_table(
            plot=self.plot_dict["popular_days"],
            plot_title="Washes Per Day",
            box_text=days_text,
        )
        self.elements.append(Spacer(1, inch))
        self.img_paragraph_table(
            plot=self.plot_dict["popular_hours"],
            plot_title="Washes Per Hour",
            box_text=hours_text,
        )
        self.elements.append(PageBreak())

    def wash_index(self):
        # Simple table with index score on left, text with numbers on right
        # TODO: need to make wash score plot in app
        index_img = Image("figs/wash_score.png", width=3.25 * inch, height=2 * inch)
        text = f"""<font face="AtlasGrotesk-Bold" size=10 textcolor="#{self.hex_navy}">Projected Wash Count</font><br/>
<font face="AtlasGrotesk-Bold" size=14 textcolor="#{self.hex_cobalt}">8576</font><br/><br/>
<font face="AtlasGrotesk-Bold" size=10 textcolor="#{self.hex_navy}">Actual Wash Count</font><br/>
<font face="AtlasGrotesk-Bold" size=14 textcolor="#{self.hex_cobalt}">8267</font>"""
        para = Paragraph(text, ParagraphStyle("space_after", leading=18))
        table = Table(
            [["", index_img, "", para]],
            colWidths=[0.5 * inch, 3.5 * inch, 1 * inch, 3 * inch],
        )
        table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "MIDDLE")]))
        self.elements.append(table)
        self.elements.append(Spacer(1, 10))

        # Optimal Days/Wash per day text
        total_optimal_weater_days = self.weather_grouped_df[
            (self.weather_grouped_df["quarter"] == self.quarter)
            & (self.weather_grouped_df["year"] == self.current_year)
            & (self.weather_grouped_df["hub_site"] == self.site_id)
        ]["non_precip_snow_days"].sum()
        optimal_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Optimal Car Wash Days</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">Quarter {self.quarter} {self.current_year} experienced approximately {total_optimal_weater_days} days
        of optimal car wash weather</font><br/>
        """
        optimal_text = Paragraph(optimal_text, self.grey_textbox)

        self.weather_wash_df["quarter"] = self.weather_wash_df["date"].dt.quarter
        total_optimal_day_washes = self.weather_wash_df[
            (self.weather_wash_df["quarter"] == self.quarter)
            & (self.weather_wash_df["year"] == self.current_year)
            & (self.weather_wash_df["hub_site"] == self.site_id)
        ]["car_washes_per_day"].mean()
        optimal_wash_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Wash Count per Optimal Car Wash Day</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">Site {self.site_id} washed approximately {round(total_optimal_day_washes)} cars per optimal wash day in Quarter {self.quarter} {self.current_year}.</font><br/>
        """
        optimal_wash_text = Paragraph(optimal_wash_text, self.grey_textbox)

        self.img_paragraph_table(
            plot=self.plot_dict["optimal_weather_days"],
            plot_title="Optimal Car Wash Days",
            plot_height=2.75 * inch,
            box_text=optimal_text,
        )
        self.elements.append(Spacer(1, 10))
        self.img_paragraph_table(
            plot=self.plot_dict["washes_per_optimal_day"],
            plot_title="Washes Per Optimal Car Wash Day",
            box_text=optimal_wash_text,
        )
        self.elements.append(PageBreak())

    def return_pdf(self):
        return self.pdf