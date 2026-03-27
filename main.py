import os
import time
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from dotenv import load_dotenv

# Libs do Google
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.generativeai as genai
import gspread

# Libs de Processamento
import fitz  # PyMuPDF

# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================
load_dotenv()
TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)
PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")

# Modelo Gratuito do Google AI Studio
GEMINI_MODEL_NAME = "gemini-1.5-flash" 
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# ==========================================
# INICIALIZAÇÃO
# ==========================================
def init_services():
    # Google Workspace (Sheets e Drive)
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Google AI Studio (Gemini)
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel(GEMINI_MODEL_NAME)
    
    return sheet, drive_service, model

def download_file_from_drive(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    file_path = TMP_DIR / file_name
    with open(file_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
    return file_path

def split_multipage_pdf(pdf_path: Path):
    single_page_pdfs = []
    try:
        doc = fitz.open(pdf_path)
        if doc.page_count <= 1: return [pdf_path]
        print(f"    -> Dividindo PDF '{pdf_path.name}' ({doc.page_count} pgs)...")
        for i in range(doc.page_count):
            new_doc = fitz.open()
            new_doc.insert_pdf(doc, from_page=i, to_page=i)
            new_path = pdf_path.with_name(f"{pdf_path.stem}_pag{i+1}{pdf_path.suffix}")
            new_doc.save(new_path)
            new_doc.close()
            single_page_pdfs.append(new_path)
        doc.close()
        pdf_path.unlink()
        return single_page_pdfs
    except Exception as e:
        print(f"    [ERRO PDF SPLIT] {e}")
        return [pdf_path]

# ==========================================
# EXTRAÇÃO DE DADOS
# ==========================================
def process_with_ai(model, text):
    if not PROMPT_PATH.exists(): return None
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        system_prompt = f.read()
    
    try:
        # Chamada oficial para o Gemini
        response = model.generate_content(f"{system_prompt}\n\nTEXTO DA NF:\n{text}")
        
        # Limpeza de markdown de código JSON
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        return None
    except Exception as e:
        print(f"    [ERRO GEMINI] {e}")
        return None

def process_with_xml(xml_path):
    try:
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            xml_content = f.read()
        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//infNFe')
        if infNFe is None: return None
        
        # Mapeamento fixo do XML para JSON (Exatamente como o anterior)
        dados_json = {
            "Dados da NF": {
                "Data": infNFe.findtext('.//ide/dhEmi', '')[:10],
                "Número da NF": infNFe.findtext('.//ide/nNF', ''),
                "Chave de Acesso da NF-E": infNFe.get('Id', '').replace('NFe', ''),
                "Natureza da operação": infNFe.findtext('.//ide/natOp', '')
            },
            "Campos do destinatário": {
                "Nome/Razao Social": infNFe.findtext('.//dest/xNome', ''),
                "CNPJ/CPF": infNFe.findtext('.//dest/CNPJ', '') or infNFe.findtext('.//dest/CPF', ''),
                "Endereço": infNFe.findtext('.//dest/enderDest/xLgr', ''),
                "Bairro/Distrito": infNFe.findtext('.//dest/enderDest/xBairro', ''),
                "CEP": infNFe.findtext('.//dest/enderDest/CEP', ''),
                "Municipio": infNFe.findtext('.//dest/enderDest/xMun', ''),
                "UF": infNFe.findtext('.//dest/enderDest/UF', ''),
                "Inscrição Estadual": infNFe.findtext('.//dest/IE', '')
            },
            "Valor total da Nota Fiscal": infNFe.findtext('.//total/ICMSTot/vNF', ''),
            "Transportador": {
                "Razao Social": infNFe.findtext('.//transporta/transporta/xNome', '') or infNFe.findtext('.//transporta/xNome', ''),
                "Quantidade": infNFe.findtext('.//vol/qVol', ''),
                "Especie": infNFe.findtext('.//vol/esp', '')
            },
            "Faturas": [],
            "Produtos":[],
            "Dados adicionais": {"Informações complementares": infNFe.findtext('.//infAdic/infCpl', '')}
        }
        for dup in infNFe.findall('.//cobr/dup'):
            dados_json["Faturas"].append({ "Data de vencimento": dup.findtext('dVenc', ''), "Valor": dup.findtext('vDup', '') })
        for det in infNFe.findall('.//det'):
            prod = det.find('prod')
            if prod is not None:
                dados_json["Produtos"].append({ "Cod. Produto": prod.findtext('cProd', ''), "Descrição do prod/serv.": prod.findtext('xProd', ''), "NCM": prod.findtext('NCM', ''), "CFOP": prod.findtext('CFOP', ''), "UN": prod.findtext('uCom', ''), "QUANT": prod.findtext('qCom', ''), "V. UNITARIO": prod.findtext('vUnCom', ''), "V. TOTAL": prod.findtext('vProd', '') })
        return dados_json
    except Exception: return None

# ==========================================
# PIPELINE DE EXECUÇÃO
# ==========================================
def run_pipeline(drive_service, model, sheet):
    for item in TMP_DIR.glob('*'): item.unlink()

    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    drive_files = results.get('files', [])
    
    print(f"[{time.strftime('%H:%M:%S')}] Vasculhando pasta... {len(drive_files)} arquivos encontrados.")
    if not drive_files: return

    drive_file_map = {} 
    drive_ids_to_delete = set()
    processed_nf_numbers = set()

    # --- FASE 1: TRIAGEM E ZIP ---
    for f in drive_files:
        name_lower = f['name'].lower()
        if not name_lower.endswith(('.pdf', '.xml', '.zip')):
            print(f"    [LIXO] '{f['name']}' removido.")
            drive_ids_to_delete.add(f['id'])
            continue

        if name_lower.endswith('.zip'):
            print(f"    [ZIP] Extraindo '{f['name']}'...")
            try:
                zip_path = download_file_from_drive(drive_service, f['id'], f['name'])
                with zipfile.ZipFile(zip_path, 'r') as z: z.extractall(TMP_DIR)
                zip_path.unlink()
                drive_ids_to_delete.add(f['id'])
            except Exception as e: print(f"    [ERRO ZIP] {e}")

    # Baixa PDFs e XMLs
    for f in drive_files:
        if f['name'].lower().endswith(('.pdf', '.xml')):
            try:
                local_path = download_file_from_drive(drive_service, f['id'], f['name'])
                if local_path.suffix.lower() == '.pdf':
                    for spdf in split_multipage_pdf(local_path): drive_file_map[spdf] = f['id']
                else: drive_file_map[local_path] = f['id']
            except Exception as e: print(f"    [ERRO DOWNLOAD] {f['name']}: {e}")

    # --- FASE 2: XML (Verdade) ---
    for xml_path in TMP_DIR.glob('*.xml'):
        dados = process_with_xml(xml_path)
        if dados:
            nf_num = dados.get("Dados da NF", {}).get("Número da NF")
            if nf_num:
                # Lógica de linha para o Google Sheets (Exatamente como o anterior)
                d_nf=dados.get("Dados da NF",{}); dest=dados.get("Campos do destinatário",{}); fat=dados.get("Faturas",[]); transp=dados.get("Transportador",{}); prod=dados.get("Produtos",[]); adic=dados.get("Dados adicionais",{})
                fat1=fat[0] if fat else {}; fat2=fat[1] if len(fat)>1 else {}; p1=prod[0] if prod else {}
                row=[d_nf.get("Data",""),d_nf.get("Número da NF",""),d_nf.get("Chave de Acesso da NF-E",""),d_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),p1.get("Cod. Produto",""),p1.get("Descrição do prod/serv.",""),p1.get("NCM",""),p1.get("CST",""),p1.get("CFOP",""),p1.get("UN",""),p1.get("QUANT",""),p1.get("V. UNITARIO",""),p1.get("V. TOTAL",""),p1.get("BC ICMS",""),p1.get("V ICMS",""),p1.get("V IPI",""),p1.get("A ICMS",""),p1.get("A IPI",""),adic.get("Informações complementares","")]
                sheet.append_row(row)
                print(f"    [XML SUCESSO] NF '{nf_num}' salva.")
                processed_nf_numbers.add(nf_num)
                drive_ids_to_delete.add(drive_file_map.get(xml_path))

    # --- FASE 3: PDF (IA) ---
    for pdf_path in TMP_DIR.glob('*.pdf'):
        text = fitz.open(pdf_path)[0].get_text() # Pega texto da página única
        dados = process_with_ai(model, text)
        if dados:
            nf_num = dados.get("Dados da NF", {}).get("Número da NF")
            if nf_num and nf_num in processed_nf_numbers:
                print(f"    [IGNORADO] PDF '{pdf_path.name}' duplicata da NF '{nf_num}'.")
                drive_ids_to_delete.add(drive_file_map.get(pdf_path))
            elif nf_num:
                # Mesma lógica de row para Sheets
                d_nf=dados.get("Dados da NF",{}); dest=dados.get("Campos do destinatário",{}); fat=dados.get("Faturas",[]); transp=dados.get("Transportador",{}); prod=dados.get("Produtos",[]); adic=dados.get("Dados adicionais",{})
                fat1=fat[0] if fat else {}; fat2=fat[1] if len(fat)>1 else {}; p1=prod[0] if prod else {}
                row=[d_nf.get("Data",""),d_nf.get("Número da NF",""),d_nf.get("Chave de Acesso da NF-E",""),d_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),p1.get("Cod. Produto",""),p1.get("Descrição do prod/serv.",""),p1.get("NCM",""),p1.get("CST",""),p1.get("CFOP",""),p1.get("UN",""),p1.get("QUANT",""),p1.get("V. UNITARIO",""),p1.get("V. TOTAL",""),p1.get("BC ICMS",""),p1.get("V ICMS",""),p1.get("V IPI",""),p1.get("A ICMS",""),p1.get("A IPI",""),adic.get("Informações complementares","")]
                sheet.append_row(row)
                print(f"    [PDF SUCESSO] NF '{nf_num}' salva via Gemini.")
                processed_nf_numbers.add(nf_num)
                drive_ids_to_delete.add(drive_file_map.get(pdf_path))

    # --- FASE 4: LIMPEZA ---
    if drive_ids_to_delete:
        print(f"  -> Limpando {len(drive_ids_to_delete)} arquivos do Drive...")
        for d_id in drive_ids_to_delete:
            if d_id:
                try: drive_service.files().delete(fileId=d_id).execute()
                except: pass

# ==========================================
# LOOP PRINCIPAL
# ==========================================
def main():
    print("Conectando ao Google e Gemini Gratuito...")
    try:
        sheet, drive_service, model = init_services()
        print("\nMonitoramento iniciado (Loop 15s). Pressione Ctrl+C para parar.\n")
        while True:
            try: run_pipeline(drive_service, model, sheet)
            except Exception as e: print(f"[{time.strftime('%H:%M:%S')}] Erro: {e}")
            time.sleep(15)
    except Exception as e:
        print(f"FALHA NA INICIALIZAÇÃO: {e}")

if __name__ == "__main__": main()