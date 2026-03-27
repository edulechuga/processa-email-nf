import os
import time
import json
import re
import io
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
# CONFIGURAÇÕES E INICIALIZAÇÃO
# (Nenhuma alteração aqui)
# ==========================================
load_dotenv()
TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)
PROMPT_PATH = Path("directives/system_prompt_nf.md")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
LLM_MODEL = "google/gemini-2.5-flash"
SCOPES =['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

def init_google_services():
    creds = Credentials.from_service_account_file('credentials.json', scopes=SCOPES)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SHEET_ID).sheet1
    drive_service = build('drive', 'v3', credentials=creds)
    return sheet, drive_service

def init_openrouter():
    return OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENROUTER_API_KEY"))

# ==========================================
# FUNÇÕES DE EXTRAÇÃO E MANIPULAÇÃO
# (Nenhuma alteração aqui)
# ==========================================
def download_file_from_drive(drive_service, file_id, file_name):
    # ... código inalterado ...
    pass
def split_multipage_pdf(pdf_path: Path):
    # ... código inalterado ...
    pass
def process_with_xml(xml_path):
    # ... código inalterado ...
    pass
def process_with_ai(client, text):
    # ... código inalterado ...
    pass
def extract_text_from_pdf(pdf_path):
    # ... código inalterado ...
    pass

# ==========================================
# FLUXO PRINCIPAL COM ARQUITETURA DE DUAS PASSADAS
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    # Limpa a pasta temporária local antes de cada execução
    for item in TMP_DIR.glob('*'):
        item.unlink()

    # --- FASE 1: STAGING (Baixar, Descompactar, Dividir) ---
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    drive_files = drive_service.files().list(q=query, fields="files(id, name)").execute().get('files', [])
    
    if not drive_files:
        return

    drive_file_map = {}  # Mapeia caminho local para ID do Drive para deleção futura

    print(f"[{time.strftime('%H:%M:%S')}] Encontrados {len(drive_files)} arquivos. Iniciando preparação...")

    # Descompacta ZIPs
    for f in drive_files:
        if f['name'].lower().endswith('.zip'):
            zip_path = download_file_from_drive(drive_service, f['id'], f['name'])
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(TMP_DIR)
            zip_path.unlink()
            drive_service.files().delete(fileId=f['id']).execute()

    # Baixa arquivos restantes e divide PDFs
    drive_files_after_zip = drive_service.files().list(q=query, fields="files(id, name)").execute().get('files', [])
    for f in drive_files_after_zip:
        if not f['name'].lower().endswith(('.pdf', '.xml')):
            continue
        local_path = download_file_from_drive(drive_service, f['id'], f['name'])
        if local_path.suffix.lower() == '.pdf':
            split_pdfs = split_multipage_pdf(local_path)
            for spdf in split_pdfs:
                drive_file_map[spdf] = f['id'] # Todas as páginas divididas apontam para o mesmo ID de arquivo original
        else:
            drive_file_map[local_path] = f['id']

    processed_nf_numbers = set()
    drive_ids_to_delete = set()

    # --- FASE 2: PROCESSAMENTO DE XML (PASSADA DA VERDADE) ---
    print("  -> Fase 2: Processando arquivos XML (prioridade máxima)...")
    for xml_path in TMP_DIR.glob('*.xml'):
        try:
            dados_json = process_with_xml(xml_path)
            if dados_json:
                nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                if nf_number:
                    # ... (lógica de append_row aqui) ...
                    sheet.append_row(...) # Adicionar a linha na planilha
                    
                    print(f"    [XML SUCESSO] NF '{nf_number}' salva no Sheets.")
                    processed_nf_numbers.add(nf_number)
                    drive_ids_to_delete.add(drive_file_map.get(xml_path))
        except Exception as e:
            print(f"    [ERRO XML] Falha ao processar '{xml_path.name}': {e}")
    
    # --- FASE 3: PROCESSAMENTO DE PDF (PASSADA DE PREENCHIMENTO) ---
    print("  -> Fase 3: Processando arquivos PDF restantes...")
    for pdf_path in TMP_DIR.glob('*.pdf'):
        try:
            pdf_text = extract_text_from_pdf(pdf_path)
            dados_json = process_with_ai(ai_client, pdf_text)

            if dados_json:
                nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                if nf_number and nf_number in processed_nf_numbers:
                    # É uma duplicata do que já foi processado via XML
                    print(f"    [PDF DUPLICADO] NF '{nf_number}' já processada via XML. Ignorando.")
                    drive_ids_to_delete.add(drive_file_map.get(pdf_path))
                elif nf_number:
                    # É uma NF única em PDF
                    # ... (lógica de append_row aqui) ...
                    sheet.append_row(...) # Adicionar a linha na planilha
                    
                    print(f"    [PDF SUCESSO] NF '{nf_number}' (sem XML correspondente) salva no Sheets.")
                    processed_nf_numbers.add(nf_number)
                    drive_ids_to_delete.add(drive_file_map.get(pdf_path))
                else:
                    print(f"    [AVISO PDF] IA não conseguiu extrair o número da NF de '{pdf_path.name}'.")
        except Exception as e:
            print(f"    [ERRO PDF] Falha ao processar '{pdf_path.name}': {e}")

    # --- FASE 4: LIMPEZA FINAL ---
    if drive_ids_to_delete:
        print(f"  -> Fase 4: Limpando {len(drive_ids_to_delete)} arquivos processados do Google Drive...")
        for drive_id in drive_ids_to_delete:
            if drive_id:
                try:
                    drive_service.files().delete(fileId=drive_id).execute()
                except Exception as e:
                    print(f"    [ERRO LIMPEZA] Falha ao deletar arquivo {drive_id}: {e}")

# ==========================================
# LOOP INFINITO (DAEMON)
# ==========================================
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