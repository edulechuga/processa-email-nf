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
# CONFIGURAÇÃO DE LOGS (Auditoria e Console)
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
ID_DB_PATH = Path("processados_ids.ndjson")
NF_DB_PATH = Path("nfs_processadas.txt")
LAST_CLEANUP_FILE = Path(".last_cleanup")

PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
ARCHIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_ARCHIVE_FOLDER_ID")
GEMINI_MODEL_ID = "gemini-2.5-flash" 

SCOPES =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def normalize_nf(nf):
    """Remove zeros à esquerda para comparação exata (ex: 00034057 -> 34057)."""
    if not nf: return ""
    return str(nf).strip().lstrip('0')

def load_memories():
    ids = set()
    if ID_DB_PATH.exists():
        ids = set(line.strip() for line in ID_DB_PATH.read_text().splitlines() if line.strip())
    nfs = set()
    if NF_DB_PATH.exists():
        nfs = set(normalize_nf(line) for line in NF_DB_PATH.read_text().splitlines() if line.strip())
    return ids, nfs

def save_id_memory(file_id):
    with open(ID_DB_PATH, "a") as f: f.write(f"{file_id}\n")

def save_nf_memory(nf_number):
    with open(NF_DB_PATH, "a") as f: f.write(f"{normalize_nf(nf_number)}\n")

# ==========================================
# INICIALIZAÇÃO E UTILITÁRIOS DE NUVEM
# ==========================================
def init_services():
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    drive_service = build('drive', 'v3', credentials=creds)
    ai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    return sheet, drive_service, ai_client

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
        prev = ",".join(file.get('parents'))
        drive_service.files().update(fileId=file_id, addParents=ARCHIVE_FOLDER_ID, removeParents=prev).execute()
        logger.info(f"📁 Arquivado: {file_name}")
    except: logger.warning(f"⚠️ Erro ao mover {file_name} (Memória garantirá que não repita)")

# ==========================================
# LIMPEZA AUTOMÁTICA (A FAXINA DE 10 DIAS)
# ==========================================
def cleanup_old_files(drive_service):
    """Apaga arquivos da pasta Processados que tenham mais de 10 dias. Executa 1x ao dia."""
    now = time.time()
    
    # Verifica se já limpamos nas últimas 24 horas (86400 segundos)
    if LAST_CLEANUP_FILE.exists():
        if (now - LAST_CLEANUP_FILE.stat().st_mtime) < 86400:
            return

    logger.info("🕒 Iniciando faxina diária na pasta Processados (Arquivos > 10 dias)...")
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=10)
    format_date = cutoff_date.isoformat().replace('+00:00', 'Z')
    
    query = f"'{ARCHIVE_FOLDER_ID}' in parents and createdTime < '{format_date}' and trashed = false"
    
    try:
        results = drive_service.files().list(
            q=query, 
            fields="files(id, name)",
            supportsAllDrives=True
        ).execute()
        
        files_to_delete = results.get('files',[])

        if not files_to_delete:
            logger.info("Nenhum arquivo antigo encontrado para remoção hoje.")
        else:
            for f in files_to_delete:
                try:
                    drive_service.files().delete(fileId=f['id']).execute()
                    logger.info(f"🗑️ Faxina: Arquivo '{f['name']}' removido permanentemente.")
                except Exception as e:
                    logger.error(f"Erro ao apagar arquivo na faxina: {e}")
        
        # Atualiza a data de modificação para marcar que a limpeza foi feita agora
        LAST_CLEANUP_FILE.touch()
        
    except Exception as e:
        logger.error(f"Erro ao executar faxina no Drive: {e}")

# ==========================================
# EXTRAÇÃO E MAPEAMENTO
# ==========================================
def pre_check_pdf_nf(text):
    """Lê o texto sujo do PDF e acha a Chave de Acesso para evitar gastar IA do Gemini."""
    match_chave = re.search(r'\b\d{44}\b', re.sub(r'\s+', '', text))
    if match_chave:
        chave = match_chave.group(0)
        nf_str = chave[25:34]
        return normalize_nf(nf_str)
    
    match_n = re.search(r'N[º\.]?\s*0*(\d{2,9})\b', text, re.IGNORECASE)
    if match_n:
        return normalize_nf(match_n.group(1))
    return None

def process_with_ai(ai_client, text):
    if not PROMPT_PATH.exists(): return None
    with open(PROMPT_PATH, "r", encoding="utf-8") as f: prompt = f.read()
    try:
        resp = ai_client.models.generate_content(model=GEMINI_MODEL_ID, contents=f"{prompt}\n\nTEXTO NF:\n{text}")
        match = re.search(r'\{.*\}', resp.text, re.DOTALL)
        return json.loads(match.group()) if match else None
    except Exception as e:
        logger.error(f"Erro API Gemini: {e}")
        return None

def process_with_xml(xml_path):
    try:
        content = xml_path.read_text(encoding='utf-8', errors='ignore')
        content = re.sub(r'\sxmlns(:\w+)?="[^"]+"', '', content)
        root = ET.fromstring(content)
        inf = root.find('.//infNFe')
        if inf is None: 
            logger.warning(f"O XML '{xml_path.name}' não parece ser uma NFe válida.")
            return None
        
        dados = {
            "Dados da NF": {"Data": inf.findtext('.//ide/dhEmi', '')[:10], "Número da NF": inf.findtext('.//ide/nNF', ''), "Chave de Acesso da NF-E": inf.get('Id', '').replace('NFe', ''), "Natureza da operação": inf.findtext('.//ide/natOp', '')},
            "Campos do destinatário": {"Nome/Razao Social": inf.findtext('.//dest/xNome', ''), "CNPJ/CPF": inf.findtext('.//dest/CNPJ', '') or inf.findtext('.//dest/CPF', ''), "Endereço": inf.findtext('.//dest/enderDest/xLgr', ''), "Bairro/Distrito": inf.findtext('.//dest/enderDest/xBairro', ''), "CEP": inf.findtext('.//dest/enderDest/CEP', ''), "Municipio": inf.findtext('.//dest/enderDest/xMun', ''), "UF": inf.findtext('.//dest/enderDest/UF', ''), "Inscrição Estadual": inf.findtext('.//dest/IE', '')},
            "Valor total da Nota Fiscal": inf.findtext('.//total/ICMSTot/vNF', ''),
            "Transportador": {"Razao Social": inf.findtext('.//transporta/xNome', '') or infNFe.findtext('.//transporta/transporta/xNome', ''), "Quantidade": inf.findtext('.//vol/qVol', ''), "Especie": inf.findtext('.//vol/esp', '')},
            "Faturas":[{"Data de vencimento": d.findtext('dVenc', ''), "Valor": d.findtext('vDup', '')} for d in inf.findall('.//cobr/dup')],
            "Produtos":[]
        }
        for det in inf.findall('.//det'):
            prod = det.find('prod')
            dados["Produtos"].append({"Cod. Produto": prod.findtext('cProd', ''), "Descrição do prod/serv.": prod.findtext('xProd', ''), "NCM": prod.findtext('NCM', ''), "CST": det.findtext('.//imposto//CST', ''), "CFOP": prod.findtext('CFOP', ''), "UN": prod.findtext('uCom', ''), "QUANT": prod.findtext('qCom', ''), "V. UNITARIO": prod.findtext('vUnCom', ''), "V. TOTAL": prod.findtext('vProd', ''), "BC ICMS": det.findtext('.//imposto/ICMS//vBC', '0'), "V ICMS": det.findtext('.//imposto/ICMS//vICMS', '0'), "V IPI": det.findtext('.//imposto/IPI//vIPI', '0'), "A ICMS": det.findtext('.//imposto/ICMS//pICMS', '0'), "A IPI": det.findtext('.//imposto/IPI//pIPI', '0')})
        dados["Dados adicionais"] = {"Informações complementares": inf.findtext('.//infAdic/infCpl', '')}
        return dados
    except Exception as e:
        logger.error(f"Erro ao processar XML '{xml_path.name}': {e}")
        return None

def map_to_row(dados_json, source_type):
    d = dados_json.get("Dados da NF", {}); de = dados_json.get("Campos do destinatário", {}); f = dados_json.get("Faturas",[]); t = dados_json.get("Transportador", {}); p = dados_json.get("Produtos", [{}]); a = dados_json.get("Dados adicionais", {})
    f_row =[]
    for i in range(8):
        if i < len(f): f_row.extend([f[i].get("Data de vencimento", ""), f[i].get("Valor", "")])
        else: f_row.extend(["", ""])
    p1 = p[0] if p else {}
    row =[d.get("Data", ""), d.get("Número da NF", ""), d.get("Chave de Acesso da NF-E", ""), d.get("Natureza da operação", ""), de.get("Nome/Razao Social", ""), de.get("CNPJ/CPF", ""), de.get("Endereço", ""), de.get("Bairro/Distrito", ""), de.get("CEP", ""), de.get("Municipio", ""), de.get("UF", ""), de.get("Inscrição Estadual", "")]
    row.extend(f_row)
    row.extend([dados_json.get("Valor total da Nota Fiscal", ""), t.get("Razao Social", ""), t.get("Quantidade", ""), t.get("Especie", ""), p1.get("Cod. Produto", ""), p1.get("Descrição do prod/serv.", ""), p1.get("NCM", ""), p1.get("CST", ""), p1.get("CFOP", ""), p1.get("UN", ""), p1.get("QUANT", ""), p1.get("V. UNITARIO", ""), p1.get("V. TOTAL", ""), p1.get("BC ICMS", "0"), p1.get("V ICMS", "0"), p1.get("V IPI", "0"), p1.get("A ICMS", "0"), p1.get("A IPI", "0"), a.get("Informações complementares", ""), source_type])
    return row

# ==========================================
# PIPELINE ESTRITAMENTE SEQUENCIAL
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    for item in TMP_DIR.rglob('*'): 
        if item.is_file(): item.unlink()
        
    processed_ids, processed_nfs = load_memories()
    
    # Executa a limpeza diária de arquivos velhos
    cleanup_old_files(drive_service)

    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get('files',[])
    if not results: return

    logger.info(f"Ciclo: {len(results)} arquivos encontrados na entrada.")
    drive_file_map = {} 

    # --- ETAPA 1: DOWNLOAD E EXTRAÇÃO ---
    for f in results:
        fid, name = f['id'], f['name']
        if fid in processed_ids: continue
        
        if not name.lower().endswith(('.pdf', '.xml', '.zip')):
            save_id_memory(fid); archive_file(drive_service, fid, name); continue

        if name.lower().endswith('.zip'):
            logger.info(f"Descompactando ZIP: {name}")
            lz = download_file_from_drive(drive_service, fid, name)
            if lz:
                try:
                    with zipfile.ZipFile(lz, 'r') as z: z.extractall(TMP_DIR)
                    save_id_memory(fid); archive_file(drive_service, fid, name)
                except Exception as e: logger.error(f"Falha ao extrair ZIP {name}: {e}")
        else:
            path = download_file_from_drive(drive_service, fid, name)
            if path: drive_file_map[path] = fid

    # --- ETAPA 2: XML (PRIORIDADE TOTAL COM RGLOB PARA SUBPASTAS) ---
    xml_count = 0
    for xml_path in TMP_DIR.rglob('*.xml'):
        dados = process_with_xml(xml_path)
        if dados:
            nf_bruto = dados.get("Dados da NF", {}).get("Número da NF")
            nf_norm = normalize_nf(nf_bruto)
            
            if nf_norm in processed_nfs:
                logger.info(f"NF {nf_norm} (XML) já consta no histórico. Pulando.")
            else:
                sheet.append_row(map_to_row(dados, "XML (Determinístico)"))
                logger.info(f"✅ SUCESSO XML: NF {nf_norm} salva na planilha.")
                save_nf_memory(nf_norm)
                processed_nfs.add(nf_norm)
            xml_count += 1
            
            fid = drive_file_map.get(xml_path)
            if fid: save_id_memory(fid); archive_file(drive_service, fid, xml_path.name)
    
    if xml_count > 0: logger.info(f"Processamento de XMLs concluído. Total: {xml_count}")

    # --- ETAPA 3: PDF (SOMENTE O QUE FUGIU AO XML) ---
    for pdf_path in TMP_DIR.rglob('*.pdf'):
        try:
            doc = fitz.open(pdf_path)
            logger.info(f"Analisando PDF '{pdf_path.name}' ({doc.page_count} páginas)...")
            
            for i, page in enumerate(doc):
                text = page.get_text()
                
                # PRÉ-CHECAGEM RÁPIDA: Acha a NF sem gastar Inteligência Artificial
                nf_rapida = pre_check_pdf_nf(text)
                if nf_rapida and nf_rapida in processed_nfs:
                    logger.info(f"  -> Pág {i+1}: NF {nf_rapida} ignorada (Detectada localmente, já processada pelo XML).")
                    continue
                
                # Se não tem a NF ou ela é nova, chama a IA
                logger.info(f"  -> Pág {i+1}: NF não reconhecida localmente, enviando para Gemini...")
                dados = process_with_ai(ai_client, text)
                if dados:
                    nf_bruto = dados.get("Dados da NF", {}).get("Número da NF")
                    nf_norm = normalize_nf(nf_bruto)
                    
                    if nf_norm in processed_nfs:
                        logger.info(f"  -> Pág {i+1}: NF {nf_norm} (IA) já processada. Ignorada.")
                    else:
                        sheet.append_row(map_to_row(dados, "PDF (IA Gemini)"))
                        logger.info(f"✅ SUCESSO IA: Pág {i+1} registrou NF {nf_norm}.")
                        save_nf_memory(nf_norm)
                        processed_nfs.add(nf_norm)
            doc.close()
            fid = drive_file_map.get(pdf_path)
            if fid: save_id_memory(fid); archive_file(drive_service, fid, pdf_path.name)
        except Exception as e: logger.error(f"Erro ao ler PDF {pdf_path.name}: {e}")

def main():
    logger.info("Robô Definitivo Iniciado (Limpeza Automática de 10 Dias Ativada).")
    try:
        sheet, ds, ai = init_services()
        while True:
            try: run_pipeline(ds, ai, sheet)
            except Exception as e: logger.error(f"Erro ciclo: {e}")
            time.sleep(15)
    except Exception as e: logger.critical(f"Erro fatal: {e}")

if __name__ == "__main__": main()