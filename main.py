import os
import time
import json
import re
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from dotenv import load_dotenv

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread
import fitz  # PyMuPDF
from openai import OpenAI

# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================
load_dotenv()
TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)
PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
LLM_MODEL = "google/gemini-2.5-flash"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# ==========================================
# INICIALIZAÇÃO E FUNÇÕES AUXILIARES
# ==========================================
def init_google_services():
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    drive_service = build('drive', 'v3', credentials=creds)
    return sheet, drive_service

def init_openrouter():
    return OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENROUTER_API_KEY"))

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
        if doc.page_count <= 1:
            return [pdf_path]
        
        print(f"    -> Dividindo PDF '{pdf_path.name}' em {doc.page_count} páginas...")
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
        print(f"    [ERRO] Falha ao dividir PDF {pdf_path.name}: {e}")
        return [pdf_path]

def get_nf_number_from_xml(xml_path):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        # Remove namespace para busca
        xml_str = ET.tostring(root, encoding='unicode')
        xml_str = re.sub(r'\sxmlns="[^"]+"', '', xml_str, count=1)
        root = ET.fromstring(xml_str)
        return root.findtext('.//infNFe/ide/nNF')
    except Exception:
        return None

def extract_text_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

def process_with_ai(client, text):
    if not PROMPT_PATH.exists():
        print(f"    Erro: Prompt não encontrado em {PROMPT_PATH}")
        return None
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        system_prompt = f.read()
    response = client.chat.completions.create(model=LLM_MODEL, messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": text}
    ])
    raw_output = response.choices[0].message.content
    json_string = re.sub(r'```json\s*|\s*```', '', raw_output).strip()
    try:
        return json.loads(json_string)
    except Exception:
        return None

def process_with_xml(xml_path):
    try:
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            xml_content = f.read()
        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//infNFe')
        if infNFe is None: return None
        
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
            "Dados adicionais": {
                "Informações complementares": infNFe.findtext('.//infAdic/infCpl', '')
            }
        }
        for dup in infNFe.findall('.//cobr/dup'):
            dados_json["Faturas"].append({ "Data de vencimento": dup.findtext('dVenc', ''), "Valor": dup.findtext('vDup', '') })
        for det in infNFe.findall('.//det'):
            prod = det.find('prod')
            if prod is not None:
                dados_json["Produtos"].append({ "Cod. Produto": prod.findtext('cProd', ''), "Descrição do prod/serv.": prod.findtext('xProd', ''), "NCM": prod.findtext('NCM', ''), "CFOP": prod.findtext('CFOP', ''), "UN": prod.findtext('uCom', ''), "QUANT": prod.findtext('qCom', ''), "V. UNITARIO": prod.findtext('vUnCom', ''), "V. TOTAL": prod.findtext('vProd', '') })
        return dados_json
    except Exception:
        return None

# ==========================================
# FLUXO PRINCIPAL
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    for item in TMP_DIR.glob('*'): item.unlink()

    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    drive_files = results.get('files', [])
    
    print(f"[{time.strftime('%H:%M:%S')}] Vasculhando pasta... Encontrados {len(drive_files)} arquivos.")
    
    if not drive_files: return

    drive_file_map = {} 
    drive_ids_to_delete = set()
    processed_nf_numbers = set()

    # --- FASE 1: FILTRAGEM E PREPARAÇÃO ---
    print(f"  -> Iniciando triagem de arquivos...")
    for f in drive_files:
        name_lower = f['name'].lower()
        
        # Se for LIXO (nem PDF, nem XML, nem ZIP), marca para apagar e ignora
        if not name_lower.endswith(('.pdf', '.xml', '.zip')):
            print(f"    [LIXO] Arquivo '{f['name']}' ignorado e marcado para exclusão.")
            drive_ids_to_delete.add(f['id'])
            continue

        # Se for ZIP, descompacta
        if name_lower.endswith('.zip'):
            print(f"    [ZIP] Descompactando '{f['name']}'...")
            try:
                zip_path = download_file_from_drive(drive_service, f['id'], f['name'])
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(TMP_DIR)
                zip_path.unlink()
                drive_ids_to_delete.add(f['id'])
            except Exception as e:
                print(f"    [ERRO ZIP] Sem permissão ou erro ao baixar ZIP: {e}")

    # Baixa arquivos PDF e XML soltos
    for f in drive_files:
        name_lower = f['name'].lower()
        if name_lower.endswith(('.pdf', '.xml')):
            try:
                local_path = download_file_from_drive(drive_service, f['id'], f['name'])
                if local_path.suffix.lower() == '.pdf':
                    # Divide PDF se tiver várias páginas
                    split_pdfs = split_multipage_pdf(local_path)
                    for spdf in split_pdfs: drive_file_map[spdf] = f['id'] 
                else:
                    drive_file_map[local_path] = f['id']
            except Exception as e:
                print(f"    [ERRO] Sem permissão para baixar '{f['name']}': {e}")

    # --- FASE 2: PROCESSAMENTO XML (Prioridade) ---
    xml_files = list(TMP_DIR.glob('*.xml'))
    for xml_path in xml_files:
        dados_json = process_with_xml(xml_path)
        if dados_json:
            nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
            if nf_number:
                # Mapeamento e Envio
                d_nf=dados_json.get("Dados da NF",{}); dest=dados_json.get("Campos do destinatário",{}); fat=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); prod=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                fat1=fat[0] if fat else {}; fat2=fat[1] if len(fat)>1 else {}; p1=prod[0] if prod else {}
                row=[d_nf.get("Data",""),d_nf.get("Número da NF",""),d_nf.get("Chave de Acesso da NF-E",""),d_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),p1.get("Cod. Produto",""),p1.get("Descrição do prod/serv.",""),p1.get("NCM",""),p1.get("CST",""),p1.get("CFOP",""),p1.get("UN",""),p1.get("QUANT",""),p1.get("V. UNITARIO",""),p1.get("V. TOTAL",""),p1.get("BC ICMS",""),p1.get("V ICMS",""),p1.get("V IPI",""),p1.get("A ICMS",""),p1.get("A IPI",""),adic.get("Informações complementares","")]
                sheet.append_row(row)
                print(f"    [SUCESSO XML] NF '{nf_number}' salva.")
                processed_nf_numbers.add(nf_number)
                drive_ids_to_delete.add(drive_file_map.get(xml_path))

    # --- FASE 3: PROCESSAMENTO PDF (Preenchimento) ---
    pdf_files = list(TMP_DIR.glob('*.pdf'))
    for pdf_path in pdf_files:
        pdf_text = extract_text_from_pdf(pdf_path)
        dados_json = process_with_ai(ai_client, pdf_text)
        if dados_json:
            nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
            if nf_number and nf_number in processed_nf_numbers:
                print(f"    [IGNORADO] PDF '{pdf_path.name}' é duplicata da NF '{nf_number}'.")
                drive_ids_to_delete.add(drive_file_map.get(pdf_path))
            elif nf_number:
                # Envio (mesma lógica do row)
                d_nf=dados_json.get("Dados da NF",{}); dest=dados_json.get("Campos do destinatário",{}); fat=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); prod=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                fat1=fat[0] if fat else {}; fat2=fat[1] if len(fat)>1 else {}; p1=prod[0] if prod else {}
                row=[d_nf.get("Data",""),d_nf.get("Número da NF",""),d_nf.get("Chave de Acesso da NF-E",""),d_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),p1.get("Cod. Produto",""),p1.get("Descrição do prod/serv.",""),p1.get("NCM",""),p1.get("CST",""),p1.get("CFOP",""),p1.get("UN",""),p1.get("QUANT",""),p1.get("V. UNITARIO",""),p1.get("V. TOTAL",""),p1.get("BC ICMS",""),p1.get("V ICMS",""),p1.get("V IPI",""),p1.get("A ICMS",""),p1.get("A IPI",""),adic.get("Informações complementares","")]
                sheet.append_row(row)
                print(f"    [SUCESSO PDF] NF '{nf_number}' salva via IA.")
                processed_nf_numbers.add(nf_number)
                drive_ids_to_delete.add(drive_file_map.get(pdf_path))

    # --- FASE 4: LIMPEZA TOTAL ---
    if drive_ids_to_delete:
        print(f"  -> Fase 4: Limpando {len(drive_ids_to_delete)} arquivos do Google Drive...")
        for drive_id in drive_ids_to_delete:
            if drive_id:
                try: drive_service.files().delete(fileId=drive_id).execute()
                except Exception as e: print(f"    [ERRO LIMPEZA] Não foi possível deletar {drive_id}: {e}")

def main():
    print("Inicializando conexões (Google Workspace e OpenRouter)...")
    sheet, drive_service = init_google_services()
    ai_client = init_openrouter()
    print("\nMonitoramento iniciado. Pressione Ctrl+C para parar.\n")
    while True:
        try: run_pipeline(drive_service, ai_client, sheet)
        except Exception as e: print(f"[{time.strftime('%H:%M:%S')}] Erro: {e}")
        time.sleep(15)

if __name__ == "__main__": main()