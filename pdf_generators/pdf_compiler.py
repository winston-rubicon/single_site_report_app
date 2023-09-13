from PyPDF2 import PdfReader, PdfWriter
import glob

def combine_pdfs(pdf_list, output_pdf):
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
    return None


def save_pdf():
    # List of PDFs to combine
    pdf_files = glob.glob('pdfs/*.pdf')

    # Output PDF file name
    output_pdf = "pdfs/combined_test.pdf"

    # Combine PDFs
    combine_pdfs(sorted(pdf_files), output_pdf)
    return output_pdf