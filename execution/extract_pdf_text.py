from PyPDF2 import PdfReader
import io

def extract_text_from_pdf(pdf_content):
    if isinstance(pdf_content, bytes):
        pdf_stream = io.BytesIO(pdf_content)
    else:
        pdf_stream = pdf_content
    
    reader = PdfReader(pdf_stream)
    text = ""
    
    for page in reader.pages:
        text += page.extract_text() + "\n"
    
    return text