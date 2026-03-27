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
SCOPES =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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
    single_page_pdfs =[]
    try:
        doc = fitz.open(pdf_path)
        if doc.page_count <= 1:
            return [pdf_path]
        
        print(f"  -> Dividindo PDF '{pdf_path.name}' em {doc.page_count} páginas...")
        for i, page in enumerate(doc):
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
        print(f"[ERRO] Falha ao dividir PDF {pdf_path.name}: {e}")
        return [pdf_path]

def get_nf_number_from_xml(xml_path):
    try:
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            xml_content = f.read()
        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content)
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
        print(f"Erro: Arquivo de prompt não encontrado em {PROMPT_PATH}")
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
    except Exception as e:
        print(f"Erro ao parsear JSON da IA: {e}")
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
    except Exception as e:
        print(f"Erro ao parsear XML nativamente: {e}")
        return None

# ==========================================
# FLUXO PRINCIPAL
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    for item in TMP_DIR.glob('*'):
        item.unlink()

    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    
    results = drive_service.files().list(
        q=query, 
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    drive_files = results.get('files',[])
    
    print(f"[{time.strftime('%H:%M:%S')}] Vasculhando pasta... Encontrados {len(drive_files)} arquivos visíveis para o robô.")
    
    if not drive_files:
        return

    drive_file_map = {} 
    print(f"  -> Iniciando preparação dos arquivos...")

    for f in drive_files:
        if f['name'].lower().endswith('.zip'):
            zip_path = download_file_from_drive(drive_service, f['id'], f['name'])
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(TMP_DIR)
            zip_path.unlink()
            drive_service.files().delete(fileId=f['id']).execute()

    drive_files_after_zip = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get('files',[])
    for f in drive_files_after_zip:
        if not f['name'].lower().endswith(('.pdf', '.xml')):
            continue
        local_path = download_file_from_drive(drive_service, f['id'], f['name'])
        if local_path.suffix.lower() == '.pdf':
            split_pdfs = split_multipage_pdf(local_path)
            for spdf in split_pdfs:
                drive_file_map[spdf] = f['id'] 
        else:
            drive_file_map[local_path] = f['id']

    processed_nf_numbers = set()
    drive_ids_to_delete = set()

    xml_files = list(TMP_DIR.glob('*.xml'))
    if xml_files:
        print("  -> Fase 2: Processando arquivos XML (prioridade máxima)...")
        for xml_path in xml_files:
            try:
                dados_json = process_with_xml(xml_path)
                if dados_json:
                    nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                    if nf_number:
                        dados_nf=dados_json.get("Dados da NF", {}); dest=dados_json.get("Campos do destinatário",{}); faturas=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); produtos=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                        fat1=faturas[0] if faturas else {}; fat2=faturas[1] if len(faturas) > 1 else {}; prod1=produtos[0] if produtos else {}
                        row=[dados_nf.get("Data",""),dados_nf.get("Número da NF",""),dados_nf.get("Chave de Acesso da NF-E",""),dados_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),prod1.get("Cod. Produto",""),prod1.get("Descrição do prod/serv.",""),prod1.get("NCM",""),prod1.get("CST",""),prod1.get("CFOP",""),prod1.get("UN",""),prod1.get("QUANT",""),prod1.get("V. UNITARIO",""),prod1.get("V. TOTAL",""),prod1.get("BC ICMS",""),prod1.get("V ICMS",""),prod1.get("V IPI",""),prod1.get("A ICMS",""),prod1.get("A IPI",""),adic.get("Informações complementares","")]
                        sheet.append_row(row)
                        print(f"    [XML SUCESSO] NF '{nf_number}' salva no Sheets.")
                        processed_nf_numbers.add(nf_number)
                        drive_ids_to_delete.add(drive_file_map.get(xml_path))
            except Exception as e:
                print(f"    [ERRO XML] Falha ao processar '{xml_path.name}': {e}")
    
    pdf_files = list(TMP_DIR.glob('*.pdf'))
    if pdf_files:
        print("  -> Fase 3: Processando arquivos PDF...")
        for pdf_path in pdf_files:
            try:
                pdf_text = extract_text_from_pdf(pdf_path)
                dados_json = process_with_ai(ai_client, pdf_text)

                if dados_json:
                    nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                    if nf_number and nf_number in processed_nf_numbers:
                        print(f"    [PDF DUPLICADO] NF '{nf_number}' já processada via XML. Ignorando.")
                        drive_ids_to_delete.add(drive_file_map.get(pdf_path))
                    elif nf_number:
                        dados_nf=dados_json.get("Dados da NF", {}); dest=dados_json.get("Campos do destinatário",{}); faturas=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); produtos=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                        fat1=faturas[0] if faturas else {}; fat2=faturas[1] if len(faturas) > 1 else {}; prod1=produtos[0] if produtos else {}
                        row=[dados_nf.get("Data",""),dados_nf.get("Número da NF",""),dados_nf.get("Chave de Acesso da NF-E",""),dados_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),prod1.get("Cod. Produto",""),prod1.get("Descrição do prod/serv.",""),prod1.get("NCM",""),prod1.get("CST",""),prod1.get("CFOP",""),prod1.get("UN",""),prod1.get("QUANT",""),prod1.get("V. UNITARIO",""),prod1.get("V. TOTAL",""),prod1.get("BC ICMS",""),prod1.get("V ICMS",""),prod1.get("V IPI",""),prod1.get("A ICMS",""),prod1.get("A IPI",""),adic.get("Informações complementares","")]
                        sheet.append_row(row)
                        print(f"    [PDF SUCESSO] NF '{nf_number}' salva no Sheets.")
                        processed_nf_numbers.add(nf_number)
                        drive_ids_to_delete.add(drive_file_map.get(pdf_path))
                    else:
                        print(f"    [AVISO PDF] IA não conseguiu extrair o número da NF de '{pdf_path.name}'.")
            except Exception as e:
                print(f"    [ERRO PDF] Falha ao processar '{pdf_path.name}': {e}")

    if drive_ids_to_delete:
        print(f"  -> Fase 4: Limpando {len(drive_ids_to_delete)} arquivos processados do Google Drive...")
        for drive_id in drive_ids_to_delete:
            if drive_id:
                try:
                    drive_service.files().delete(fileId=drive_id).execute()
                except Exception as e:
                    print(f"    [ERRO LIMPEZA] Falha ao deletar arquivo {drive_id}: {e}")

def main():
    print("Inicializando conexões (Google Workspace e OpenRouter)...")
    sheet, drive_service = init_google_services()
    ai_client = init_openrouter()
    print("\nServiço de Notas Fiscais iniciado e monitorando o Google Drive.")
    print("Pressione Ctrl+C para interromper.\n")

    while True:
        try:
            run_pipeline(drive_service, ai_client, sheet)
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Erro no ciclo principal: {e}")
        time.sleep(15)

if __name__ == "__main__":
    main()