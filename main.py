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
# CONFIGURAÇÕES INICIAIS E MEMÓRIA
# ==========================================
load_dotenv()
TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)
ID_DB_PATH = Path("processados_ids.ndjson")     # Memória de ID do Google Drive
NF_DB_PATH = Path("nfs_processadas.txt")        # Memória de Número da NF
LAST_CLEANUP_FILE = Path(".last_cleanup")

PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
ARCHIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ARCHIVE_FOLDER_ID")
GEMINI_MODEL_ID = "gemini-2.5-flash" 

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# --- GESTÃO DE MEMÓRIA ---
def load_memories():
    ids = set()
    if ID_DB_PATH.exists():
        ids = set(line.strip() for line in ID_DB_PATH.read_text().splitlines() if line.strip())
    
    nfs = set()
    if NF_DB_PATH.exists():
        nfs = set(line.strip() for line in NF_DB_PATH.read_text().splitlines() if line.strip())
    
    return ids, nfs

def save_id_memory(file_id):
    with open(ID_DB_PATH, "a") as f: f.write(f"{file_id}\n")

def save_nf_memory(nf_number):
    with open(NF_DB_PATH, "a") as f: f.write(f"{nf_number}\n")

# ==========================================
# INICIALIZAÇÃO DE SERVIÇOS
# ==========================================
def init_services():
    try:
        creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SHEET_ID).sheet1
        drive_service = build('drive', 'v3', credentials=creds)
        ai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        logger.info("Conexões com Google Workspace e Gemini estabelecidas.")
        return sheet, drive_service, ai_client
    except Exception as e:
        logger.error(f"Erro na inicialização dos serviços: {e}")
        raise

def download_file_from_drive(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    file_path = TMP_DIR / file_name
    try:
        with open(file_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return file_path
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo {file_name}: {e}")
        return None

# ==========================================
# FAXINA E ARQUIVAMENTO
# ==========================================
def cleanup_old_files(drive_service):
    now = time.time()
    if LAST_CLEANUP_FILE.exists() and (now - LAST_CLEANUP_FILE.stat().st_mtime) < 86400:
        return
    logger.info("🕒 Iniciando faxina diária na pasta Processados (Arquivos > 10 dias)...")
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=10)
    format_date = cutoff_date.isoformat().replace('+00:00', 'Z')
    query = f"'{ARCHIVE_FOLDER_ID}' in parents and createdTime < '{format_date}' and trashed = false"
    try:
        results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True).execute()
        for f in results.get('files', []):
            drive_service.files().delete(fileId=f['id']).execute()
            logger.info(f"🗑️ Faxina: Arquivo antigo '{f['name']}' removido.")
        LAST_CLEANUP_FILE.touch()
    except Exception as e: logger.error(f"Erro na execução da faxina: {e}")

def archive_file(drive_service, file_id, file_name):
    try:
        file = drive_service.files().get(fileId=file_id, fields='parents').execute()
        prev_parents = ",".join(file.get('parents'))
        drive_service.files().update(fileId=file_id, addParents=ARCHIVE_FOLDER_ID, removeParents=prev_parents).execute()
        logger.info(f"📁 Arquivo '{file_name}' movido para a pasta Processados.")
    except Exception:
        logger.warning(f"⚠️ Não foi possível mover '{file_name}' no Drive (Erro 403). A memória local impedirá reprocessamento.")

# ==========================================
# EXTRAÇÃO E MAPEAMENTO
# ==========================================
def process_with_ai(ai_client, text):
    if not PROMPT_PATH.exists(): return None
    with open(PROMPT_PATH, "r", encoding="utf-8") as f: prompt = f.read()
    try:
        resp = ai_client.models.generate_content(model=GEMINI_MODEL_ID, contents=f"{prompt}\n\nTEXTO NF PARA EXTRAÇÃO:\n{text}")
        match = re.search(r'\{.*\}', resp.text, re.DOTALL)
        return json.loads(match.group()) if match else None
    except Exception as e:
        logger.error(f"Erro na API Gemini: {e}")
        return None

def process_with_xml(xml_path):
    try:
        content = xml_path.read_text(encoding='utf-8', errors='ignore')
        content = re.sub(r'\sxmlns="[^"]+"', '', content, count=1)
        root = ET.fromstring(content)
        inf = root.find('.//infNFe')
        if inf is None: return None
        
        dados = {
            "Dados da NF": {"Data": inf.findtext('.//ide/dhEmi', '')[:10], "Número da NF": inf.findtext('.//ide/nNF', ''), "Chave de Acesso da NF-E": inf.get('Id', '').replace('NFe', ''), "Natureza da operação": inf.findtext('.//ide/natOp', '')},
            "Campos do destinatário": {"Nome/Razao Social": inf.findtext('.//dest/xNome', ''), "CNPJ/CPF": inf.findtext('.//dest/CNPJ', '') or inf.findtext('.//dest/CPF', ''), "Endereço": inf.findtext('.//dest/enderDest/xLgr', ''), "Bairro/Distrito": inf.findtext('.//dest/enderDest/xBairro', ''), "CEP": inf.findtext('.//dest/enderDest/CEP', ''), "Municipio": inf.findtext('.//dest/enderDest/xMun', ''), "UF": inf.findtext('.//dest/enderDest/UF', ''), "Inscrição Estadual": inf.findtext('.//dest/IE', '')},
            "Valor total da Nota Fiscal": inf.findtext('.//total/ICMSTot/vNF', ''),
            "Transportador": {"Razao Social": inf.findtext('.//transporta/xNome', ''), "Quantidade": inf.findtext('.//vol/qVol', ''), "Especie": inf.findtext('.//vol/esp', '')},
            "Faturas": [{"Data de vencimento": d.findtext('dVenc', ''), "Valor": d.findtext('vDup', '')} for d in inf.findall('.//cobr/dup')],
            "Produtos": []
        }
        for det in inf.findall('.//det'):
            prod = det.find('prod')
            dados["Produtos"].append({
                "Cod. Produto": prod.findtext('cProd', ''), "Descrição do prod/serv.": prod.findtext('xProd', ''), "NCM": prod.findtext('NCM', ''),
                "CST": det.findtext('.//imposto//CST', ''), "CFOP": prod.findtext('CFOP', ''), "UN": prod.findtext('uCom', ''), "QUANT": prod.findtext('qCom', ''),
                "V. UNITARIO": prod.findtext('vUnCom', ''), "V. TOTAL": prod.findtext('vProd', ''), "BC ICMS": det.findtext('.//imposto/ICMS//vBC', '0'),
                "V ICMS": det.findtext('.//imposto/ICMS//vICMS', '0'), "V IPI": det.findtext('.//imposto/IPI//vIPI', '0'), "A ICMS": det.findtext('.//imposto/ICMS//pICMS', '0'), "A IPI": det.findtext('.//imposto/IPI//pIPI', '0')
            })
        dados["Dados adicionais"] = {"Informações complementares": inf.findtext('.//infAdic/infCpl', '')}
        return dados
    except Exception as e:
        logger.error(f"Erro ao processar XML: {e}")
        return None

def map_to_row(dados_json, source_type):
    d = dados_json.get("Dados da NF", {}); de = dados_json.get("Campos do destinatário", {}); f = dados_json.get("Faturas", []); t = dados_json.get("Transportador", {}); p = dados_json.get("Produtos", [{}]); a = dados_json.get("Dados adicionais", {})
    f_row = []
    for i in range(8):
        if i < len(f): f_row.extend([f[i].get("Data de vencimento", ""), f[i].get("Valor", "")])
        else: f_row.extend(["", ""])
    
    p1 = p[0] if len(p) > 0 else {}
    row = [d.get("Data", ""), d.get("Número da NF", ""), d.get("Chave de Acesso da NF-E", ""), d.get("Natureza da operação", ""), de.get("Nome/Razao Social", ""), de.get("CNPJ/CPF", ""), de.get("Endereço", ""), de.get("Bairro/Distrito", ""), de.get("CEP", ""), de.get("Municipio", ""), de.get("UF", ""), de.get("Inscrição Estadual", "")]
    row.extend(f_row)
    row.extend([dados_json.get("Valor total da Nota Fiscal", ""), t.get("Razao Social", ""), t.get("Quantidade", ""), t.get("Especie", ""), p1.get("Cod. Produto", ""), p1.get("Descrição do prod/serv.", ""), p1.get("NCM", ""), p1.get("CST", ""), p1.get("CFOP", ""), p1.get("UN", ""), p1.get("QUANT", ""), p1.get("V. UNITARIO", ""), p1.get("V. TOTAL", ""), p1.get("BC ICMS", "0"), p1.get("V ICMS", "0"), p1.get("V IPI", "0"), p1.get("A ICMS", "0"), p1.get("A IPI", "0"), a.get("Informações complementares", ""), source_type])
    return row

# ==========================================
# PIPELINE PRINCIPAL (LOGICA ANTI-DUPLICIDADE)
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    for item in TMP_DIR.glob('*'): item.unlink()
    processed_ids, processed_nfs = load_memories()
    
    cleanup_old_files(drive_service)

    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get('files', [])
    if not results: return

    logger.info(f"Ciclo iniciado: {len(results)} arquivos encontrados.")
    drive_file_map = {} 

    # --- ETAPA 1: DOWNLOAD E TRIAGEM ---
    for f in results:
        fid, name = f['id'], f['name']
        if fid in processed_ids: continue
        
        if not name.lower().endswith(('.pdf', '.xml', '.zip')):
            save_id_memory(fid); archive_file(drive_service, fid, name); continue

        if name.lower().endswith('.zip'):
            logger.info(f"Processando ZIP: {name}")
            lz = download_file_from_drive(drive_service, fid, name)
            if lz:
                try:
                    with zipfile.ZipFile(lz, 'r') as z: z.extractall(TMP_DIR)
                    save_id_memory(fid); archive_file(drive_service, fid, name)
                except Exception as e: logger.error(f"Falha ao extrair ZIP {name}: {e}")
        else:
            path = download_file_from_drive(drive_service, fid, name)
            if path: drive_file_map[path] = fid

    # --- ETAPA 2: XML (PRIORIDADE TOTAL) ---
    for xml_path in TMP_DIR.glob('*.xml'):
        dados = process_with_xml(xml_path)
        if dados:
            nf = dados.get("Dados da NF", {}).get("Número da NF")
            # Verifica se essa NF já foi processada (Independente do arquivo)
            if nf in processed_nfs:
                logger.info(f"NF {nf} (via XML) já processada anteriormente. Pulando.")
            else:
                sheet.append_row(map_to_row(dados, "XML (Determinístico)"))
                logger.info(f"✅ SUCESSO XML: NF {nf} salva.")
                save_nf_memory(nf); processed_nfs.add(nf)
            
            # Marca o ID do arquivo original como resolvido
            fid = drive_file_map.get(xml_path)
            if fid: save_id_memory(fid); archive_file(drive_service, fid, xml_path.name)

    # --- ETAPA 3: PDF (COMPLEMENTO) ---
    for pdf_path in TMP_DIR.glob('*.pdf'):
        try:
            doc = fitz.open(pdf_path)
            for page in doc:
                dados = process_with_ai(ai_client, page.get_text())
                if dados:
                    nf = dados.get("Dados da NF", {}).get("Número da NF")
                    # Só processa se o XML da Etapa 2 não tiver pego essa mesma NF
                    if nf in processed_nfs:
                        logger.info(f"NF {nf} (via PDF) ignorada: já processada por XML ou ciclo anterior.")
                    else:
                        sheet.append_row(map_to_row(dados, "PDF (IA Gemini)"))
                        logger.info(f"✅ SUCESSO IA: NF {nf} salva.")
                        save_nf_memory(nf); processed_nfs.add(nf)
            doc.close()
            fid = drive_file_map.get(pdf_path)
            if fid: save_id_memory(fid); archive_file(drive_service, fid, pdf_path.name)
        except Exception as e: logger.error(f"Erro ao processar PDF {pdf_path.name}: {e}")

def main():
    logger.info("Robô de Notas Fiscais iniciado (v3.0 - Blindagem Total).")
    try:
        sheet, ds, ai = init_services()
        while True:
            try:
                run_pipeline(ds, ai, sheet)
            except Exception as e:
                logger.error(f"Erro no ciclo principal: {e}")
            time.sleep(15)
    except KeyboardInterrupt:
        logger.info("Processo interrompido manualmente.")
    except Exception as e:
        logger.critical(f"FALHA FATAL NO SISTEMA: {e}")

if __name__ == "__main__":
    main()