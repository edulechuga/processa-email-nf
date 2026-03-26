import os
import time
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from dotenv import load_dotenv

# Libs de execução
from O365 import Account, FileSystemTokenBackend
import fitz  # PyMuPDF
from openai import OpenAI
import gspread
import requests

# ==========================================
# CONFIGURAÇÕES INICIAIS
# ==========================================
load_dotenv()

TMP_DIR = Path(".tmp")
TMP_DIR.mkdir(exist_ok=True)

PROMPT_PATH = Path("directives/system_prompt_nf.md")
ALLOWED_SENDERS =['d.oliveira@custom.biz', 's.oliveira@custom.biz', 'edulechuga@gmail.com']
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
LLM_MODEL = "google/gemini-2.5-flash"

# ==========================================
# FUNÇÕES DE INICIALIZAÇÃO
# ==========================================
def init_outlook():
    """Conecta ao Microsoft Graph API usando credenciais de usuário (ROPC)."""
    client_id = os.getenv("MS_CLIENT_ID")
    client_secret = os.getenv("MS_CLIENT_SECRET")
    tenant_id = os.getenv("MS_TENANT_ID")
    username = os.getenv("MS_USERNAME")
    password = os.getenv("MS_PASSWORD")
    
    if not all([client_id, client_secret, tenant_id, username, password]):
        print("\n" + "="*50)
        print("ERRO: Configuração incompleta no .env")
        print("Adicione MS_USERNAME e MS_PASSWORD")
        print("="*50 + "\n")
        raise Exception("Credenciais incompletas")
    
    # ROPC - Resource Owner Password Credentials
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
    data = {
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://outlook.office.com/Mail.Read offline_access",
        "username": username,
        "password": password
    }
    
    response = requests.post(token_url, data=data)
    
    if response.status_code != 200:
        print(f"Erro na autenticação: {response.json()}")
        raise Exception("Falha no login ROPC")
    
    token_data = response.json()
    
    # Salva o token
    token_backend = FileSystemTokenBackend(token_path='.', token_filename='token.json')
    token_info = {
        "access_token": token_data["access_token"],
        "token_type": token_data.get("token_type", "Bearer"),
        "expires_in": token_data.get("expires_in", 3600),
        "scope": token_data.get("scope", ""),
        "expires_at": int(time.time()) + token_data.get("expires_in", 3600),
        "refresh_token": token_data.get("refresh_token", "")
    }
    token_backend.save_token(token_info)
    
    # Cria account
    credentials = (client_id, client_secret)
    account = Account(credentials, tenant_id=tenant_id, token_backend=token_backend)
    
    print("Autenticação via ROPC concluída!")
    return account.mailbox()

def init_openrouter():
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.getenv("OPENROUTER_API_KEY"),
    )

def init_sheets():
    gc = gspread.service_account(filename='credentials.json')
    return gc.open_by_key(SHEET_ID).sheet1

# ==========================================
# FUNÇÕES DE EXTRAÇÃO (PDF / XML / IA)
# ==========================================
def extract_text_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

def process_with_ai(client, text):
    """Lê a diretriz de extração e envia o texto pro LLM."""
    
    # 1. Carrega o prompt diretamente do arquivo markdown
    if not PROMPT_PATH.exists():
        print(f"Erro: Arquivo de prompt não encontrado em {PROMPT_PATH}")
        return None
        
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        system_prompt = f.read()
    
    # 2. Faz a chamada à API
    response = client.chat.completions.create(
        model=LLM_MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text}
        ]
    )
    
    # 3. Limpeza de markdown de bloco de código
    raw_output = response.choices[0].message.content
    json_string = re.sub(r'```json\s*|\s*```', '', raw_output).strip()
    
    try:
        return json.loads(json_string)
    except Exception as e:
        print(f"Erro ao parsear JSON da IA: {e}\nRetorno cru: {raw_output}")
        return None

def process_with_xml(xml_path):
    """Caminho determinístico: Extrai os dados nativamente do XML (Custo Zero)."""
    try:
        with open(xml_path, 'r', encoding='utf-8', errors='ignore') as f:
            xml_content = f.read()

        xml_content = re.sub(r'\sxmlns="[^"]+"', '', xml_content, count=1)
        root = ET.fromstring(xml_content)
        infNFe = root.find('.//infNFe')
        
        if infNFe is None: 
            return None

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
            dados_json["Faturas"].append({
                "Data de vencimento": dup.findtext('dVenc', ''),
                "Valor": dup.findtext('vDup', '')
            })

        for det in infNFe.findall('.//det'):
            prod = det.find('prod')
            if prod is not None:
                dados_json["Produtos"].append({
                    "Cod. Produto": prod.findtext('cProd', ''),
                    "Descrição do prod/serv.": prod.findtext('xProd', ''),
                    "NCM": prod.findtext('NCM', ''),
                    "CFOP": prod.findtext('CFOP', ''),
                    "UN": prod.findtext('uCom', ''),
                    "QUANT": prod.findtext('qCom', ''),
                    "V. UNITARIO": prod.findtext('vUnCom', ''),
                    "V. TOTAL": prod.findtext('vProd', '')
                })

        return dados_json
    except Exception as e:
        print(f"Erro ao parsear XML nativamente: {e}")
        return None

# ==========================================
# FLUXO PRINCIPAL DE ORQUESTRAÇÃO
# ==========================================
def run_pipeline(mailbox, ai_client, sheet):
    q = mailbox.new_query().on_attribute('isRead').equals(False).chain('and').on_attribute('hasAttachments').equals(True)
    messages = mailbox.get_messages(query=q, download_attachments=True)
    
    for message in messages:
        sender_email = message.sender.address.lower()
        if sender_email not in ALLOWED_SENDERS:
            continue
            
        print(f"[{time.strftime('%H:%M:%S')}] Processando email de: {sender_email}")
        
        xml_attachment = None
        pdf_attachment = None

        for attachment in message.attachments:
            name_lower = attachment.name.lower()
            if name_lower.endswith('.xml'):
                xml_attachment = attachment
            elif name_lower.endswith('.pdf') or attachment.content_type == 'application/pdf':
                pdf_attachment = attachment

        dados_json = None

        # 1. Tenta o XML primeiro
        if xml_attachment:
            temp_xml = TMP_DIR / xml_attachment.name
            xml_attachment.save(TMP_DIR)
            print("  -> Lendo via XML determinístico...")
            dados_json = process_with_xml(temp_xml)
            if temp_xml.exists(): temp_xml.unlink()

        # 2. Se não tem XML, tenta o PDF via IA lendo o diretório local de prompts
        if not dados_json and pdf_attachment:
            temp_pdf = TMP_DIR / pdf_attachment.name
            pdf_attachment.save(TMP_DIR)
            print(f"  -> Lendo PDF via IA ({LLM_MODEL})...")
            try:
                pdf_text = extract_text_from_pdf(temp_pdf)
                dados_json = process_with_ai(ai_client, pdf_text)
            finally:
                if temp_pdf.exists(): temp_pdf.unlink()

        # 3. Salva no Google Sheets
        if dados_json:
            dados_nf = dados_json.get("Dados da NF", {})
            dest = dados_json.get("Campos do destinatário", {})
            faturas = dados_json.get("Faturas",[])
            transp = dados_json.get("Transportador", {})
            produtos = dados_json.get("Produtos",[])
            adic = dados_json.get("Dados adicionais", {})

            fat1 = faturas[0] if len(faturas) > 0 else {}
            fat2 = faturas[1] if len(faturas) > 1 else {}
            prod1 = produtos[0] if len(produtos) > 0 else {}

            row =[
                dados_nf.get("Data", ""),
                dados_nf.get("Número da NF", ""),
                dados_nf.get("Chave de Acesso da NF-E", ""),
                dados_nf.get("Natureza da operação", ""),
                dest.get("Nome/Razao Social", ""),
                dest.get("CNPJ/CPF", ""),
                dest.get("Endereço", ""),
                dest.get("Bairro/Distrito", ""),
                dest.get("CEP", ""),
                dest.get("Municipio", ""),
                dest.get("UF", ""),
                dest.get("Inscrição Estadual", ""),
                fat1.get("Data de vencimento", ""),
                fat1.get("Valor", ""),
                fat2.get("Data de vencimento", ""),
                fat2.get("Valor", ""),
                dados_json.get("Valor total da Nota Fiscal", ""),
                transp.get("Razao Social", ""),
                transp.get("Quantidade", ""),
                transp.get("Especie", ""),
                prod1.get("Cod. Produto", ""),
                prod1.get("Descrição do prod/serv.", ""),
                prod1.get("NCM", ""),
                prod1.get("CST", ""),
                prod1.get("CFOP", ""),
                prod1.get("UN", ""),
                prod1.get("QUANT", ""),
                prod1.get("V. UNITARIO", ""),
                prod1.get("V. TOTAL", ""),
                prod1.get("BC ICMS", ""),
                prod1.get("V ICMS", ""),
                prod1.get("V IPI", ""),
                prod1.get("A ICMS", ""),
                prod1.get("A IPI", ""),
                adic.get("Informações complementares", "")
            ]
            
            try:
                sheet.append_row(row)
                print(f"  [SUCESSO] NF {dados_nf.get('Número da NF')} enviada para o Sheets.")
            except Exception as e:
                print(f"  [ERRO] Falha ao enviar para o Sheets: {e}")
                
        # Marca como lido (para pdf, xml ou mesmo se falhar - evita loop infinito nos erros)
        message.mark_as_read()

# ==========================================
# LOOP INFINITO (DAEMON)
# ==========================================
def main():
    print("Inicializando conexões (Outlook, OpenRouter, Sheets)...")
    mailbox = init_outlook()
    ai_client = init_openrouter()
    sheet = init_sheets()
    
    print("\nServiço de Notas Fiscais iniciado e rodando em background.")
    print("Pressione Ctrl+C para interromper.\n")

    while True:
        try:
            run_pipeline(mailbox, ai_client, sheet)
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Erro na verificação (tentando novamente no próximo ciclo): {e}")
        
        time.sleep(15)

if __name__ == "__main__":
    main()