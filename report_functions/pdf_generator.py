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
from calendar import month_name

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

# need explanation dict for feature importance breakdown
explanation_dict = {
    "prcp_total_average": "Average precipitation for the month",
    "tmin_average": "Average daily low temperature",
    "tmax_average": "Average daily high temperature",
    "cpi_all_items": "CPI Value for all items",
    "prcp_days_above_15": "Number of days with rain above 1.5 millimeters",
    "months_since_opening": "Number of months the site has been open",
    "snow_total_average": "Total average snowfall for the month",
    "snow_days_above_1": "Number of days where snowfall was greater than one millimeter",
    "snwd_total_average": "Average snow depth for the month",
    "snwd_days_above_15": "Number of days with snow depth above 15 millimeters",
    "one_month_prev_count": "Total washes in previous month",
    "two_month_prev_count": "Total washes two months prior",
    "three_month_prev_count": "Total washes three months prior",
    "last_year_prev_count": "Total washes in same month in previous year",
    "state_unemployment": "Current month's state unemployment rate",
    "gas_per_gallon": "Current month's average gas price across BLS division",
    "tmax_days_above_340": "Number of days with temperatures above 93F",
    "tmin_days_below_0": "Number of days with temperatures below 32F",
    "annual": "Annual seasonality",
    "quarterly": "Quarterly seasonality",
}


class FooterCanvas(canvas.Canvas):
    def __init__(
        self,
        *args,
        header_text="Market Evaluation",
        data_dict,
        current_month,
        current_year,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.pages = []
        self.width, self.height = letter
        self.site_number = data_dict["site_number"]
        self.hub_name = data_dict["hub_name"]
        self.date = f"{month_name[current_month]} {current_year}"
        self.header_text = header_text
        self.year = current_year
        self.month = current_month
        self.address1 = data_dict["site_address"]
        self.address2 = f"{data_dict['site_city']}, {data_dict['site_state']} {data_dict['site_zip']}"

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
            30,
            15,
            f"{self.hub_name.upper()} \u2022 {self.address1.upper()} \u2022 {self.date.upper()}",
        )
        # Site number, full address
        self.setStrokeColor(self.cobalt)
        self.setStrokeAlpha(1)
        self.line(
            self.width - 0.5 * inch, 0.5 * inch, self.width - 0.5 * inch, 1.05 * inch
        )

        self.setFillColor(self.navy)
        self.setFont("AtlasGrotesk-Bold", 10)
        site_width = stringWidth(f"Site {self.site_number}", "AtlasGrotesk-Bold", 10)
        self.drawString(
            self.width - 0.5 * inch - site_width - 10,
            0.95 * inch,
            f"Site {self.site_number}",
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
            f"{self.hub_name.upper()} \u2022 {self.address1.upper()} \u2022 {self.date.upper()}",
        )

        self.restoreState()


# Wrapper function for FooterCanvas
def create_footer_canvas_wrapper(
    data_dict,
    current_month,
    current_year,
):
    def footer_canvas_wrapper(filename, *args, **kwargs):
        return FooterCanvas(
            filename,
            data_dict=data_dict,
            current_month=current_month,
            current_year=current_year,
            **kwargs,
        )

    return footer_canvas_wrapper


class SingleSiteReport:
    def __init__(
        self,
        plot_dict,
        data_dict,
        current_month,
        current_year,
    ):
        self.plot_dict = plot_dict
        self.data = data_dict
        self.current_year = current_year
        self.current_month = current_month
        self.current_year_month = f"{self.current_month}_{self.current_year}"
        self.month_name = month_name[current_month]
        self.site_number = self.data["site_number"]
        self.elements = []
        self.custom_wrapper = create_footer_canvas_wrapper(
            data_dict=self.data,
            current_month=self.current_month,
            current_year=self.current_year,
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
        self.grey = Color(142.0 / 255.0, 142.0 / 255.0, 142.0 / 255.0)
        # Need hex codes for XML formatting
        self.hex_navy = self.navy.hexval()[2:]
        self.hex_cobalt = self.cobalt.hexval()[2:]
        self.hex_skyblue = self.skyblue.hexval()[2:]
        self.hex_grey = "8e8e8e"
        self.hex_gold = "ffcb00"
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
        self.optimal_weather_page()
        self.econ_page()
        self.traffic_page()

        # Build
        self.pdf = BytesIO()
        self.doc = SimpleDocTemplate(self.pdf, pagesize=letter, bottomMargin=0)
        self.doc.multiBuild(self.elements, canvasmaker=self.custom_wrapper)
        self.pdf.seek(0)

    def bulleted_text(self, bullets: list, colors=None, title_text=None, text_size=8):
        """
        Returns ListFlowable of text to be rendered as multi-colored bulleted list,
        with descriptive title. bullets must be a list of bullets to use.
        """
        if colors is None:
            colors = [self.hex_cobalt, self.hex_navy, self.hex_skyblue, self.hex_gold]
        if title_text is not None:
            formatted_title = Paragraph(
                f"""<font face=AtlasGrotesk-Bold size={text_size+2} color="#{self.hex_navy}">{title_text}</font><br/><br/>"""
            )

        bullet_items = []

        for color, bullet in zip(colors, bullets):
            bullet_paragraph = Paragraph(
                f"""<font face=AtlasGrotesk size={text_size} color="#{self.hex_navy}">{bullet}</font>""",
                ParagraphStyle("blah", leading=1.5 * text_size),
            )
            bullet_item = ListItem(
                bullet_paragraph,
                bulletColor=f"#{color}",
                bulletFontSize=3 * text_size,
                bulletOffsetY=1.25 * text_size,
            )
            bullet_items.append(bullet_item)

        bullet_list = ListFlowable(bullet_items, bulletType="bullet")
        if title_text is not None:
            ret = [formatted_title, bullet_list]
        else:
            ret = [bullet_list]
        return ret

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
            avg_vol = self.data["ytd_avg_washes"]
            first_box_title = "12 Month Average Volume"
            first_box_text = [f"Site {self.site_number}: {avg_vol:,}"]
            first_box = self.bulleted_text(
                title_text=first_box_title, bullets=first_box_text
            )

            avg_rev = self.data["ytd_avg_rpc"]
            second_box_title = "12 Month Average Revenue Per Car"
            second_box_text = [f"Site {self.site_number}: ${avg_rev:,}"]
            second_box = self.bulleted_text(
                title_text=second_box_title, bullets=second_box_text
            )

            mom_vol = self.data["mom_washes"]
            mom_rev = self.data["mom_rpc"]

            lower_box = f"""
            <font face="AtlasGrotesk-Bold" size=14>Insights</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Total wash volume changed by {round(mom_vol, 1)}% compared to last month</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;RPC changed by {round(mom_rev, 1)}%</font><br/>
            """

        elif page == 5:
            avg_churn = self.data["ytd_avg_churn"]
            first_box_title = "12 Month Average Churn Rate"
            first_box_text = [f"Site {self.site_number}: {avg_churn}%"]
            first_box = self.bulleted_text(
                title_text=first_box_title, bullets=first_box_text
            )

            avg_capture = self.data["ytd_avg_capture"]
            second_box_title = "12 Month Average Capture Rate"
            second_box_text = [f"Site {self.site_number}: {avg_capture}%"]
            second_box = self.bulleted_text(
                title_text=second_box_title, bullets=second_box_text
            )

            mom_churn = self.data["mom_churn"]
            mom_capture = self.data["mom_capture"]
            lower_box = f"""
            <font face="AtlasGrotesk-Bold" size=14>Insights</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Churn rate changed by {round(mom_churn, 1)}% compared to last month</font><br/>
            <font face="AtlasGrotesk" size=10>
            &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Capture rate changed by {round(mom_capture, 1)}%</font><br/>
            """

        self.elements.append(Spacer(1, 10))
        # First plot
        self.img_paragraph_table(
            plot=plots[0], plot_title=plot_titles[0], box_text=first_box
        )
        self.elements.append(Spacer(1, 40))
        self.img_paragraph_table(
            plot=plots[1], plot_title=plot_titles[1], box_text=second_box
        )
        self.elements.append(Spacer(1, 20))
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
        table_title = "Wash Distribution"
        table_style = TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TEXTCOLOR", (0, 0), (-1, -1), self.navy),
                ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 14),
                ("ALIGN", (0, 0), (-1, 0), "CENTER"),
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

        self.elements.append(Spacer(1, 40))

        # Set text for average membership rpc
        mem_rpc = round(self.data["ytd_avg_mem_rpc"], 2)
        title = "12 Month Average Membership RPC"
        text = [f"Site {self.site_number}: ${mem_rpc}"]
        bullets = self.bulleted_text(title_text=title, bullets=text)

        self.img_paragraph_table(
            plot=self.plot_dict["membership_rpc"],
            plot_title="Membership Revenue Per Car",
            box_text=bullets,
            plot_height=2.5 * inch,
        )

        self.elements.append(Spacer(1, 10))

        # Set text for average retail rpc
        retail_rpc = round(self.data["ytd_avg_retail_rpc"], 2)
        title = "12 Month Average Retail RPC"
        text = [f"Site {self.site_number}: ${retail_rpc}"]
        bullets = self.bulleted_text(title_text=title, bullets=text)

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

        # Pulling out max/min monthly package shifts
        # membership
        max_mom_mem, min_mom_mem = self.data["mom_membership_package"].keys()
        max_mem_shift = self.data["mom_membership_package"][max_mom_mem]
        min_mem_shift = self.data["mom_membership_package"][min_mom_mem]
        # retail
        max_mom_retail, min_mom_retail = self.data["mom_retail_package"].keys()
        max_retail_shift = self.data["mom_retail_package"][max_mom_retail]
        min_retail_shift = self.data["mom_retail_package"][min_mom_retail]
        # Defining a Table with a Paragraph in order to be able to use rounded corners
        # as well as XML formatting in the text
        text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Quarterly Notable Package Distribution Shifts</font><br/><br/>
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}"><u>Membership</u></font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_mom_mem}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">+{round(max_mem_shift,1)}%</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_mom_mem}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">{round(min_mem_shift,1)}%</font><br/><br/>
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}"><u>Retail</u></font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_mom_retail}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_cobalt}">+{round(max_retail_shift,1)}%</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_mom_retail}</font><br/>
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
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (0, -1), 12),
                    ("SPAN", (2, 0), (2, -1)),
                    ("ROWHEIGHT", (0, 2), (-1, 2), 120),
                ]
            )
        )

        self.elements.append(table)

        self.elements.append(PageBreak())

    def popular_days_hours(self):
        # Getting most/least popular days of the quarter
        max_day, min_day = self.data["popular_days_extrema"].keys()
        max_day_count = self.data["popular_days_extrema"][max_day]
        min_day_count = self.data["popular_days_extrema"][min_day]
        days_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Most/Least Popular Days on Average in {self.month_name} {self.current_year}</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_day}: {round(max_day_count):,}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_day}: {round(min_day_count):,}</font><br/>
        """
        days_text = Paragraph(days_text, self.grey_textbox)

        # Getting most/least popular hours of the quarter
        max_hour, min_hour = self.data["popular_hours_extrema"].keys()
        max_hour_count = self.data["popular_hours_extrema"][max_hour]
        min_hour_count = self.data["popular_hours_extrema"][min_hour]
        hours_text = f"""
        <font face=AtlasGrotesk-Bold size=10 color="#{self.hex_cobalt}">Most/Least Popular Hours on Average in {self.month_name} {self.current_year}</font><br/><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{max_hour}: {round(max_hour_count):,}</font><br/>
        <font face=AtlasGrotesk size=8 color="#{self.hex_navy}">{min_hour}: {round(min_hour_count):,}</font><br/>
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

    def feature_importances_table(self, main_feat="weather"):
        table = Table(
            [
                [main_feat.capitalize(), ""],
                [
                    "Fraction of Prediction:",
                    f"{self.data[f'{main_feat}_shap']['total_frac']}%",
                ],
                [
                    "Most Import Features:",
                    f"{explanation_dict[self.data[f'{main_feat}_shap']['feat1'][0]]}, {self.data[f'{main_feat}_shap']['feat1'][1]}%",
                ],
                [
                    "",
                    f"{explanation_dict[self.data[f'{main_feat}_shap']['feat2'][0]]}, {self.data[f'{main_feat}_shap']['feat2'][1]}%",
                ],
            ],
            colWidths=[
                stringWidth("Fraction of Prediction:", "AtlasGrotesk", 12) + 12,
                480 - stringWidth("Fraction of Prediction:", "AtlasGrotesk", 12),
            ],
        )
        table.setStyle(
            TableStyle(
                [
                    # Row 1: left-aligned, bold, size 14
                    ("ALIGN", (0, 0), (-1, 0), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, 0), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (-1, 0), 14),
                    ("TEXTCOLOR", (0, 0), (-1, 0), self.cobalt),
                    # Row 2, Cell 1: regular, size 12, center-aligned
                    ("FONTNAME", (0, 1), (0, -1), "AtlasGrotesk"),
                    ("FONTSIZE", (0, 1), (0, -1), 12),
                    # Rows 2 and 3, Cells 2 and 3: regular, size 10, center-aligned
                    ("FONTNAME", (1, 1), (-1, 2), "AtlasGrotesk"),
                    ("FONTSIZE", (1, 1), (-1, 2), 10),
                    ("TEXTCOLOR", (0, 1), (-1, -1), self.navy),
                    # Some specific formatting for beautification
                    ("BOTTOMPADDING", (0, 0), (0, -1), 5),
                    ("TOPPADDING", (0, 0), (0, 0), 6),
                    ("LEFTPADDING", (0, 0), (0, -1), 12),
                    ("VALIGN", (1, 0), (1, -1), "BOTTOM"),
                    ("BACKGROUND", (0, 0), (-1, -1), self.lightgrey),
                    ("ROUNDEDCORNERS", [10, 10, 10, 10]),
                ]
            )
        )

        return table

    def wash_index(self):
        self.elements.append(Spacer(1, 20))
        # Simple table with index score on left, text with numbers on right
        index_img = Image(
            self.plot_dict["wash_index_score"], width=3.25 * inch, height=2.25 * inch
        )
        predicted_counts = self.data["wash_index_score"]["predicted_counts"]
        actual_counts = self.data["wash_index_score"]["actual_counts"]
        text = f"""<br/><font face="AtlasGrotesk-Bold" size=10 textcolor="#{self.hex_navy}">Projected Wash Count</font><br/>
<font face="AtlasGrotesk-Bold" size=14 textcolor="#{self.hex_cobalt}">{predicted_counts:,}</font><br/><br/>
<font face="AtlasGrotesk-Bold" size=10 textcolor="#{self.hex_navy}">Actual Wash Count</font><br/>
<font face="AtlasGrotesk-Bold" size=14 textcolor="#{self.hex_cobalt}">{actual_counts:,}</font>"""
        para = Paragraph(text, ParagraphStyle("space_after", leading=18))
        table = Table(
            [["", index_img, "", para]],
            colWidths=[1 * inch, 3.5 * inch, 1 * inch, 3 * inch],
        )
        table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, 0), "TOP")]))
        self.elements.append(table)
        # self.elements.append(Spacer(1, 10))

        # Changing up the layout of the feature importances section.
        # Creating a 'graphic' (table) that will show the contribution of the four main categories
        # to the prediction, and then a textbox below describing the effect(?)
        ### Side-by-side - think column is better
        # feat_table = Table(
        #     [
        #         [
        #             f"Weather: ",
        #             f"{self.data['weather_shap']['total_frac']}%",
        #             f"Historic: ",
        #             f"{self.data['historic_shap']['total_frac']}%",
        #         ],
        #         [
        #             f"Economic: ",
        #             f"{self.data['economic_shap']['total_frac']}%",
        #             f"Seasonal: ",
        #             f"{self.data['seasonal_shap']['total_frac']}%",
        #         ],
        #     ]
        # )

        ### Column
        # feat_table = Table(
        #     [
        #         [f"Weather: ", f"{self.data['weather_shap']['total_frac']}%"],
        #         [
        #             f"Historic: ",
        #             f"{self.data['historic_shap']['total_frac']}%",
        #         ],
        #         [f"Economic: ", f"{self.data['economic_shap']['total_frac']}%"],
        #         [
        #             f"Seasonal: ",
        #             f"{self.data['seasonal_shap']['total_frac']}%",
        #         ],
        #     ]
        # )

        ### This works for either column or side-by-side
        # feat_table.setStyle(
        #     TableStyle(
        #         [  # First Column
        #             ("TEXTCOLOR", (0, 0), (0, -1), self.cobalt),
        #             ("FONTNAME", (0, 0), (0, -1), "AtlasGrotesk-Bold"),
        #             ("FONTSIZE", (0, 0), (0, -1), 14),
        #             # Third Column
        #             ("TEXTCOLOR", (2, 0), (2, -1), self.cobalt),
        #             ("FONTNAME", (2, 0), (2, -1), "AtlasGrotesk-Bold"),
        #             ("FONTSIZE", (2, 0), (2, -1), 14),
        #             # Second Column
        #             ("TEXTCOLOR", (1, 0), (1, -1), self.navy),
        #             ("FONTNAME", (1, 0), (1, -1), "AtlasGrotesk"),
        #             ("FONTSIZE", (1, 0), (1, -1), 12),
        #             # Fourth Column
        #             ("TEXTCOLOR", (3, 0), (3, -1), self.navy),
        #             ("FONTNAME", (3, 0), (3, -1), "AtlasGrotesk"),
        #             ("FONTSIZE", (3, 0), (3, -1), 12),
        #             # All
        #             ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        #             ("TOPPADDING", (0, 0), (-1, -1), 6),
        #             ("LEFTPADDING", (0, 0), (-1, -1), 12),
        #             ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        #             ("BACKGROUND", (0, 0), (-1, -1), self.lightgrey),
        #             ("ROUNDEDCORNERS", [10, 10, 10, 10]),
        #         ]
        #     )
        # )

        ### Trying out pie chart
        feat_chart = Image(
            self.plot_dict["feature_importances"]["fig"],
            width=4 * inch,
            height=3 * inch,
        )

        color_dict = self.plot_dict["feature_importances"]["colors"]
        feat_legend = Table(
            [
                self.bulleted_text(
                    bullets=list(color_dict.keys()),
                    colors=[color.strip("#") for color in color_dict.values()],
                    text_size=12,
                )
            ], colWidths=[1.25*inch]
        )
        feat_legend.setStyle(
            TableStyle(
                [
                    ("ROUNDEDCORNERS", [10, 10, 10, 10]),
                    ("BACKGROUND", (0, 0), (-1, -1), self.lightgrey),
                ]
            )
        )

        table = Table(
            [
                ["Feature Contribution to the Prediction", ""],
                [feat_chart, ""],
                ["", ""],
                ["", feat_legend],
                ["", ""],
                ["", ""],
            ],
            colWidths=[4 * inch, 1.25 * inch],
        )
        table.setStyle(
            TableStyle(
                [
                    ("TEXTCOLOR", (0, 0), (0, 0), self.navy),
                    ("FONTNAME", (0, 0), (0, 0), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (0, 0), 14),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("SPAN", (0, 0), (-1, 0)),
                    ("SPAN", (0, 1), (0, -1)),
                    ("BOTTOMPADDING", (1, 3), (1, 3), 6),
                ]
            )
        )
        self.elements.append(table)

        self.elements.append(Spacer(1, 5))

        ### Creating a temporary textbox that will briefly describe each factor. Below will be for later reports.
        # text = f"""
        #     <font face="AtlasGrotesk-Bold" size=14>Understanding the Feature Contributions</font><br/>
        #     <font face="AtlasGrotesk" size=10>
        #     In predicting the wash counts for the current month, the above features contributed the respective fraction to the prediction.
        #     Weather affected the prediction by {self.data['weather_shap']['total_frac']}%, while previous performance ('Historic') had an effect of
        #     {self.data['historic_shap']['total_frac']}%.
        #       </font><br/>
        #     """
        # # Description of above
        # para = Paragraph(
        #     text,
        #     ParagraphStyle(
        #         "blue_textbox",
        #         fontName="AtlasGrotesk",
        #         fontSize=10,
        #         textColor=self.white,
        #         backColor=self.cobalt,
        #         borderPadding=20,
        #         leading=20,
        #     ),
        # )
        # insights = Table([[para]], colWidths=[7 * inch])
        # insights.setStyle(self.rounded_corners)

        img_size = 15
        img_list = [
            Image("branding/weather_icon.png", img_size, img_size, mask="auto"),
            Image("branding/historic_icon.png", img_size, img_size, mask="auto"),
            Image("branding/economic_icon.png", img_size, img_size, mask="auto"),
            Image("branding/seasonal_icon.png", img_size, img_size, mask="auto"),
        ]

        text_parastyle = ParagraphStyle(
            "grey_textbox",
            fontName="AtlasGrotesk",
            fontSize=10,
            textColor=self.navy,
            backColor=self.lightgrey,
            borderPadding=10,
            leading=15,
        )

        text_list = [
            Paragraph(
                f"""<font face="AtlasGrotesk-Bold">Weather</font> - Covers a range of local weather variables, such as temperature and precipitation, collected from nearby stations.<br/>""",
                text_parastyle,
            ),
            Paragraph(
                f"""<font face="AtlasGrotesk-Bold">Historic</font> - Utilizes data from the site's past, including historical trends and patterns.<br/>""",
                text_parastyle,
            ),
            Paragraph(
                f"""<font face="AtlasGrotesk-Bold">Economic</font> - Integrates local economic indicators, like the Consumer Price Index (CPI) and unemployment rates, among other metrics.<br/>""",
                text_parastyle,
            ),
            Paragraph(
                f"""<font face="AtlasGrotesk-Bold">Seasonal</font> - Captures patterns related to different times of the year, recognizing shifts in trends due to seasons and periodic events.""",
                text_parastyle,
            ),
        ]

        img_text_table = [[img, text] for img, text in zip(img_list, text_list)]

        text = Paragraph(
            "Understanding the Features",
            ParagraphStyle(
                "name", fontName="AtlasGrotesk-Bold", fontSize=14, textColor=self.navy
            ),
        )

        insights = Table(
            [
                ["Understanding the Features", ""],
                img_text_table[0],
                img_text_table[1],
                img_text_table[2],
                img_text_table[3],
            ],
            colWidths=[40, 6.5 * inch],
        )

        insights.setStyle(self.rounded_corners)
        insights.setStyle(
            TableStyle(
                [
                    ("SPAN", (0, 0), (-1, 0)),
                    ("ALIGN", (0, 0), (0, 0), ("LEFT")),
                    ("FONTNAME", (0, 0), (0, 0), "AtlasGrotesk-Bold"),
                    ("FONTSIZE", (0, 0), (0, 0), 14),
                    ("TEXTCOLOR", (0, 0), (0, 0), self.navy),
                    ("ALIGN", (0, 1), (0, -1), ("CENTER")),
                    ("VALIGN", (0, 1), (0, -1), ("MIDDLE")),
                    ("TOPPADDING", (0, 1), (0, -1), 5),
                ]
            )
        )

        # text = f"""
        #     <font face="AtlasGrotesk-Bold" size=14>Understanding the Features</font><br/><br/>
        #     <font face="AtlasGrotesk" size=10>
        #     <font face="AtlasGrotesk-Bold">Weather</font> - Covers a range of local weather variables, such as temperature and precipitation, collected from nearby stations.<br/>
        #     <font face="AtlasGrotesk-Bold">Historic</font> - Utilizes data from the site's past, including historical trends and patterns.<br/>
        #     <font face="AtlasGrotesk-Bold">Economic</font> - Integrates local economic indicators, like the Consumer Price Index (CPI) and unemployment rates, among other metrics.<br/>
        #     <font face="AtlasGrotesk-Bold">Seasonal</font> - Captures patterns related to different times of the year, recognizing shifts in trends due to seasons and periodic events.
        #     </font><br/>
        #     """
        # Description of above
        # para = Paragraph(
        #     text,
        #     ParagraphStyle(
        #         "grey_textbox",
        #         fontName="AtlasGrotesk",
        #         fontSize=10,
        #         textColor=self.navy,
        #         backColor=self.lightgrey,
        #         borderPadding=20,
        #         leading=15,
        #     ),
        # )
        # insights = Table([[para]], colWidths=[7 * inch])

        self.elements.append(insights)

        # # Feature Importances
        # weather_table = self.feature_importances_table("weather")
        # econ_table = self.feature_importances_table("economic")
        # historic_table = self.feature_importances_table("historic")
        # seasonal_table = self.feature_importances_table("seasonal")
        # table = Table(
        #     [
        #         ["Key Factors Driving Prediction"],
        #         [""],
        #         [weather_table],
        #         [econ_table],
        #         [historic_table],
        #         [seasonal_table],
        #     ]
        # )
        # table.setStyle(
        #     TableStyle(
        #         [
        #             ("ALIGN", (0, 0), (0, 0), "LEFT"),
        #             ("FONTNAME", (0, 0), (0, 0), "AtlasGrotesk-Bold"),
        #             ("FONTSIZE", (0, 0), (0, 0), 18),
        #             ("TEXTCOLOR", (0, 0), (0, 0), self.navy),
        #             ("TOPPADDING", (0, 2), (-1, -1), 12),
        #             ("BOTTOMPADDING", (0, 2), (-1, -1), 12),
        #             ("LEFTPADDING", (0, 0), (-1, -1), 12),
        #             ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        #         ]
        #     )
        # )
        # self.elements.append(table)

        self.elements.append(PageBreak())

    def img_blue_textbox_below(
        self, table_title, img, text, img_height=3 * inch, img_width=6 * inch
    ):
        self.elements.append(Spacer(1, 10))
        table_style = TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TEXTCOLOR", (0, 0), (-1, -1), self.navy),
                ("FONTNAME", (0, 0), (-1, -1), "AtlasGrotesk-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 14),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ]
        )
        plot = Image(
            img,
            width=img_width,
            height=img_height,
        )
        table = Table([[table_title], [plot]])
        table.setStyle(table_style)
        self.elements.append(table)

        para = Paragraph(
            text,
            ParagraphStyle(
                "blue_textbox",
                fontName="AtlasGrotesk",
                fontSize=10,
                textColor=self.white,
                backColor=self.cobalt,
                borderPadding=20,
                leading=20,
            ),
        )

        insights = Table([[para]], colWidths=[7 * inch])
        insights.setStyle(self.rounded_corners)
        insights.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (0, 0), "AtlasGrotesk-Bold"),
                    ("TEXTCOLOR", (0, 0), (0, 0), self.navy),
                    ("FONTSIZE", (0, 0), (0, 0), 14),
                ]
            )
        )
        self.elements.append(insights)

    def optimal_weather_page(self):
        # First is Optimal Wash Days
        table_title = "Optimal Car Wash Days"
        total_optimal_weather_days = round(
            self.data["optimal_weather_days"][self.current_year_month]
        )
        optimal_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>Optimal Car Wash Days</font><br/>
            <font face="AtlasGrotesk" size=10>
            {self.month_name} {self.current_year} experienced approximately {total_optimal_weather_days} days
            of optimal car wash weather
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title,
            self.plot_dict["optimal_weather_days"],
            optimal_text,
            img_height=2.85 * inch,
        )

        self.elements.append(Spacer(1, 20))

        # Now Washes per optimal wash day
        table_title = "Washes Per Optimal Car Wash Day"
        total_optimal_day_washes = self.data["washes_per_optimal_day"][
            self.current_year_month
        ]
        optimal_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>Wash Count per Optimal Car Wash Day</font><br/>
            <font face="AtlasGrotesk" size=10>
            Site {self.site_number} washed approximately {round(total_optimal_day_washes)}
              cars per optimal wash day in {self.month_name} {self.current_year}
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title,
            self.plot_dict["washes_per_optimal_day"],
            optimal_text,
            img_height=2.85 * inch,
        )

        self.elements.append(PageBreak())

    def econ_page(self):
        # First is CPI information
        table_title = "CPI Year-Over-Year Change"
        current_month_region_cpi = round(
            self.data["cpi_yoy_region"][self.current_year_month], 1
        )
        cpi_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>Consumer Price Index</font><br/>
            <font face="AtlasGrotesk" size=10>
            The regional CPI YoY in {self.month_name} {self.current_year} is {current_month_region_cpi}%.
             This is a change of {round(self.data['cpi_mom_region'],1)}% compared to last month,
              and a {round(self.data['cpi_pct_ch_reg_nat'],1)}% difference from the national CPI YoY.
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title, self.plot_dict["cpi_yoy"], cpi_text, img_height=2.85 * inch
        )

        self.elements.append(Spacer(1, 20))

        # Now Unemployment
        table_title = "Unemployment Rate"
        greater_less = "greater" if self.data["unemploy_pct_ch_reg_nat"] > 0 else "less"
        u_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>Unemployment Rate</font><br/>
            <font face="AtlasGrotesk" size=10>
            {self.month_name} {self.current_year} saw a change of {round(self.data['unemploy_mom_region'],1)}% compared to last month.
             This month's state unemployment is {abs(round(self.data['unemploy_pct_ch_reg_nat'],1))}% {greater_less} than the national rate.
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title, self.plot_dict["unemployment"], u_text, img_height=2.85 * inch
        )

        self.elements.append(PageBreak())

    def traffic_page(self):
        # First is miles traveled information
        table_title = "Monthly Miles Traveled Year-Over-Year Change"
        greater_less = "greater" if self.data["traffic_pct_ch_reg_nat"] > 0 else "less"
        traffic_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>State Monthly Miles Traveled</font><br/>
            <font face="AtlasGrotesk" size=10>
            The state's miles traveled  YoY changed {round(self.data['traffic_mom_regional'])}% compared to last month, and is
             {abs(round(self.data['traffic_pct_ch_reg_nat'],1))}% {greater_less} than the national YoY.
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title, self.plot_dict["traffic"], traffic_text, img_height=2.85 * inch
        )

        self.elements.append(Spacer(1, 20))

        # Gas Prices
        table_title = "Monthly Unleaded Standard Gas Price"
        greater_less = "greater" if self.data["gas_pct_ch_reg_nat"] > 0 else "less"
        gas_text = f"""
            <font face="AtlasGrotesk-Bold" size=14>Gas Prices</font><br/>
            <font face="AtlasGrotesk" size=10>
            State average gas prices saw a change of {round(self.data['gas_mom_regional'],1)}% compared to last month, going to ${round(self.data['gas_regional'][self.current_year_month],2)}.
             This is {abs(round(self.data['gas_pct_ch_reg_nat'],1))}% {greater_less} than the U.S. city average of ${round(self.data['gas_national'][self.current_year_month],2)}.
              </font><br/>
            """
        self.img_blue_textbox_below(
            table_title, self.plot_dict["gas"], gas_text, img_height=2.85 * inch
        )

        self.elements.append(PageBreak())

    def return_pdf(self):
        return self.pdf
