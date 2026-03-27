import os
import time
import json
import re
import logging
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# Libs do Google
from google import genai
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import gspread

# Libs de Processamento
import fitz  # PyMuPDF

# ==========================================
# CONFIGURAÇÃO DE LOGS
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("execucao.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================
load_dotenv()
TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)
DB_PATH = Path("processados.ndjson") 
LAST_CLEANUP_FILE = Path(".last_cleanup") # Para lembrar quando limpou a pasta

PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
ARCHIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ARCHIVE_FOLDER_ID")
GEMINI_MODEL_ID = "gemini-2.5-flash" 

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def load_processed_ids():
    if not DB_PATH.exists(): return set()
    with open(DB_PATH, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_processed_id(file_id):
    with open(DB_PATH, "a") as f:
        f.write(f"{file_id}\n")

# ==========================================
# FUNÇÃO DE LIMPEZA AUTOMÁTICA (Nova!)
# ==========================================
def cleanup_old_files(drive_service):
    """Apaga arquivos da pasta Processados que tenham mais de 10 dias. Executa 1x ao dia."""
    now = time.time()
    
    # Verifica se o arquivo de controle existe e se a última limpeza foi há menos de 24 horas
    if LAST_CLEANUP_FILE.exists():
        last_cleanup_time = LAST_CLEANUP_FILE.stat().st_mtime
        if (now - last_cleanup_time) < 86400: # 86400s = 24h
            # Ainda não deu 24h desde a última faxina, pula silenciosamente
            return

    logger.info("🕒 Hora da faxina diária! Verificando arquivos com mais de 10 dias na pasta 'Processados'...")
    
    # Define a data de corte (10 dias atrás)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=10)
    format_date = cutoff_date.isoformat().replace('+00:00', 'Z')
    
    query = f"'{ARCHIVE_FOLDER_ID}' in parents and createdTime < '{format_date}' and trashed = false"
    
    try:
        results = drive_service.files().list(
            q=query, 
            fields="files(id, name, createdTime)",
            supportsAllDrives=True
        ).execute()
        files_to_delete = results.get('files', [])

        if not files_to_delete:
            logger.info("Nenhum arquivo antigo encontrado para remoção.")
        else:
            for f in files_to_delete:
                try:
                    drive_service.files().delete(fileId=f['id']).execute()
                    logger.info(f"🗑️ Faxina: Arquivo '{f['name']}' (ID: {f['id']}) apagado permanentemente.")
                except Exception as e:
                    logger.error(f"Erro ao apagar arquivo na faxina: {e}")
        
        # Atualiza a data de modificação do arquivo para marcar que a limpeza foi feita agora
        LAST_CLEANUP_FILE.touch()
        logger.info(f"✅ Faxina diária concluída. Próxima limpeza em 24 horas.")
        
    except Exception as e:
        logger.error(f"Erro ao acessar o Drive para faxina: {e}")

# ==========================================
# UTILITÁRIOS DRIVE E SERVIÇOS
# ==========================================
def init_services():
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEET_ID).sheet1
        drive_service = build('drive', 'v3', credentials=creds)
        ai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        return sheet, drive_service, ai_client
    except Exception as e:
        logger.error(f"Erro inicialização: {e}")
        raise

def download_file_from_drive(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    file_path = TMP_DIR / file_name
    try:
        with open(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done: status, done = downloader.next_chunk()
        return file_path
    except Exception as e:
        logger.error(f"Erro download {file_name}: {e}")
        return None

def archive_file(drive_service, file_id, file_name):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        previous_parents = ",".join(file.get('parents'))
        drive_service.files().update(
            fileId=file_id,
            addParents=ARCHIVE_FOLDER_ID,
            removeParents=previous_parents,
            fields='id, parents'
        ).execute()
        logger.info(f"Arquivo '{file_name}' movido para Processados.")
    except Exception as e:
        logger.warning(f"Não foi possível mover '{file_name}'. Memória local evitará reprocessamento.")

# ==========================================
# EXTRAÇÃO E MAPEAMENTO
# ==========================================
def process_with_ai(ai_client, text):
    if not PROMPT_PATH.exists(): return None
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        system_prompt = f.read()
    try:
        response = ai_client.models.generate_content(model=GEMINI_MODEL_ID, contents=f"{system_prompt}\n\nTEXTO NF:\n{text}")
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        return json.loads(json_match.group()) if json_match else None
    except: return None

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
                "Razao Social": infNFe.findtext('.//transporta/xNome', '') or infNFe.findtext('.//transporta/transporta/xNome', ''),
                "Quantidade": infNFe.findtext('.//vol/qVol', ''),
                "Especie": infNFe.findtext('.//vol/esp', '')
            },
            "Faturas": [{"Data de vencimento": d.findtext('dVenc', ''), "Valor": d.findtext('vDup', '')} for d in infNFe.findall('.//cobr/dup')],
            "Produtos": [],
            "Dados adicionais": {"Informações complementares": infNFe.findtext('.//infAdic/infCpl', '')}
        }
        for det in infNFe.findall('.//det'):
            prod = det.find('prod')
            dados_json["Produtos"].append({
                "Cod. Produto": prod.findtext('cProd', ''), "Descrição do prod/serv.": prod.findtext('xProd', ''), "NCM": prod.findtext('NCM', ''),
                "CST": det.findtext('.//imposto//CST', ''), "CFOP": prod.findtext('CFOP', ''), "UN": prod.findtext('uCom', ''), "QUANT": prod.findtext('qCom', ''),
                "V. UNITARIO": prod.findtext('vUnCom', ''), "V. TOTAL": prod.findtext('vProd', ''), "BC ICMS": det.findtext('.//imposto/ICMS//vBC', '0'),
                "V ICMS": det.findtext('.//imposto/ICMS//vICMS', '0'), "V IPI": det.findtext('.//imposto/IPI//vIPI', '0'), "A ICMS": det.findtext('.//imposto/ICMS//pICMS', '0'), "A IPI": det.findtext('.//imposto/IPI//pIPI', '0')
            })
        return dados_json
    except: return None

def map_to_row(dados_json, source_type):
    d = dados_json.get("Dados da NF", {}); de = dados_json.get("Campos do destinatário", {}); faturas = dados_json.get("Faturas", []); t = dados_json.get("Transportador", {}); p = dados_json.get("Produtos", [{}]); a = dados_json.get("Dados adicionais", {})
    f_row = []
    for i in range(8):
        if i < len(faturas): f_row.extend([faturas[i].get("Data de vencimento", ""), faturas[i].get("Valor", "")])
        else: f_row.extend(["", ""])
    p1 = p[0] if len(p) > 0 else {}
    row = [d.get("Data", ""), d.get("Número da NF", ""), d.get("Chave de Acesso da NF-E", ""), d.get("Natureza da operação", ""), de.get("Nome/Razao Social", ""), de.get("CNPJ/CPF", ""), de.get("Endereço", ""), de.get("Bairro/Distrito", ""), de.get("CEP", ""), de.get("Municipio", ""), de.get("UF", ""), de.get("Inscrição Estadual", "")]
    row.extend(f_row)
    row.extend([dados_json.get("Valor total da Nota Fiscal", ""), t.get("Razao Social", ""), t.get("Quantidade", ""), t.get("Especie", ""), p1.get("Cod. Produto", ""), p1.get("Descrição do prod/serv.", ""), p1.get("NCM", ""), p1.get("CST", ""), p1.get("CFOP", ""), p1.get("UN", ""), p1.get("QUANT", ""), p1.get("V. UNITARIO", ""), p1.get("V. TOTAL", ""), p1.get("BC ICMS", ""), p1.get("V ICMS", ""), p1.get("V IPI", ""), p1.get("A ICMS", ""), p1.get("A IPI", ""), a.get("Informações complementares", ""), source_type])
    return row

# ==========================================
# PIPELINE
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    # 0. Faxina preventiva (Limpa pasta local e depois verifica arquivos antigos no Drive)
    for item in TMP_DIR.glob('*'): item.unlink()
    cleanup_old_files(drive_service)
    
    processed_ids = load_processed_ids()
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    drive_files = results.get('files', [])
    
    if not drive_files: return
    logger.info(f"Ciclo: {len(drive_files)} arquivos encontrados na entrada.")
    
    drive_file_map = {} 
    processed_nf_numbers = set()

    for f in drive_files:
        name, fid = f['name'], f['id']
        if fid in processed_ids: continue
        if not name.lower().endswith(('.pdf', '.xml', '.zip')):
            save_processed_id(fid)
            archive_file(drive_service, fid, name)
            continue
        if name.lower().endswith('.zip'):
            local_zip = download_file_from_drive(drive_service, fid, name)
            if local_zip:
                try:
                    with zipfile.ZipFile(local_zip, 'r') as z: z.extractall(TMP_DIR)
                    save_processed_id(fid)
                    archive_file(drive_service, fid, name)
                except: logger.error(f"Erro ZIP {name}")

    for f in drive_files:
        if f['name'].lower().endswith(('.pdf', '.xml')) and f['id'] not in processed_ids:
            path = download_file_from_drive(drive_service, f['id'], f['name'])
            if path: drive_file_map[path] = f['id']

    # XMLs
    for xml_path in TMP_DIR.glob('*.xml'):
        dados = process_with_xml(xml_path)
        if dados:
            nf = dados.get("Dados da NF", {}).get("Número da NF")
            sheet.append_row(map_to_row(dados, "XML (Determinístico)"))
            logger.info(f"SUCESSO XML: NF {nf}")
            processed_nf_numbers.add(nf)
            fid = drive_file_map.get(xml_path)
            if fid:
                save_processed_id(fid)
                archive_file(drive_service, fid, xml_path.name)

    # PDFs
    for pdf_path in TMP_DIR.glob('*.pdf'):
        try:
            doc = fitz.open(pdf_path)
            for page in doc:
                dados = process_with_ai(ai_client, page.get_text())
                if dados:
                    nf = dados.get("Dados da NF", {}).get("Número da NF")
                    if nf not in processed_nf_numbers:
                        sheet.append_row(map_to_row(dados, "PDF (IA Gemini)"))
                        logger.info(f"SUCESSO IA: NF {nf}")
                        processed_nf_numbers.add(nf)
            doc.close()
            fid = drive_file_map.get(pdf_path)
            if fid:
                save_processed_id(fid)
                archive_file(drive_service, fid, pdf_path.name)
        except Exception as e: logger.error(f"Erro PDF {pdf_path}: {e}")

def main():
    logger.info("Iniciando Robô com Auto-Limpeza de 10 dias.")
    try:
        sheet, drive_service, ai_client = init_services()
        while True:
            try: run_pipeline(drive_service, ai_client, sheet)
            except Exception as e: logger.error(f"Erro ciclo: {e}")
            time.sleep(15)
    except KeyboardInterrupt: logger.info("Encerrado.")
    except Exception as e: logger.critical(f"Erro fatal: {e}")

if __name__ == "__main__": main()