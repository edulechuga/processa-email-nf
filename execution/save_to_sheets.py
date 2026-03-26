import os
import json
from google.oauth2 import credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def get_credentials():
    creds = None
    
    if os.path.exists('token.json'):
        creds = credentials.Credentials.from_authorized_user_file('token.json', SCOPES)
    
    if not creds or not creds.valid:
        if os.path.exists('credentials.json'):
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
    
    return creds

def append_to_sheet(nf_data):
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "1pzx7_ad-wWKwhkLTb43OadSxFonVcyJXjDoV-uxCgDA")
    
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)
    
    dados_nf = nf_data.get("Dados da NF", {})
    destinatario = nf_data.get("Campos do destinatário", {})
    faturas = nf_data.get("Faturas", [])
    transportador = nf_data.get("Transportador", {})
    produtos = nf_data.get("Produtos", [{}])
    dados_adicionais = nf_data.get("Dados adicionais", {})
    
    row = [
        dados_nf.get("Data", ""),
        dados_nf.get("Número da NF", ""),
        dados_nf.get("Chave de Acesso da NF-E", ""),
        dados_nf.get("Natureza da operação", ""),
        destinatario.get("Nome/Razao Social", ""),
        destinatario.get("CNPJ/CPF", ""),
        destinatario.get("Endereço", ""),
        destinatario.get("Bairro/Distrito", ""),
        destinatario.get("CEP", ""),
        destinatario.get("Municipio", ""),
        destinatario.get("UF", ""),
        destinatario.get("Inscrição Estadual", ""),
        faturas[0].get("Data de vencimento", "") if len(faturas) > 0 else "",
        faturas[0].get("Valor", "") if len(faturas) > 0 else "",
        faturas[1].get("Data de vencimento", "") if len(faturas) > 1 else "",
        faturas[1].get("Valor", "") if len(faturas) > 1 else "",
        nf_data.get("Valor total da Nota Fiscal", ""),
        transportador.get("Razao Social", ""),
        transportador.get("Quantidade", ""),
        transportador.get("Especie", ""),
        produtos[0].get("Cod. Produto", ""),
        produtos[0].get("Descrição do prod/serv.", ""),
        produtos[0].get("NCM", ""),
        produtos[0].get("CST", ""),
        produtos[0].get("CFOP", ""),
        produtos[0].get("UN", ""),
        produtos[0].get("QUANT", ""),
        produtos[0].get("V. UNITARIO", ""),
        produtos[0].get("V. TOTAL", ""),
        produtos[0].get("BC ICMS", ""),
        produtos[0].get("V ICMS", ""),
        produtos[0].get("V IPI", ""),
        produtos[0].get("A ICMS", ""),
        produtos[0].get("A IPI", ""),
        dados_adicionais.get("Informações complementares", "")
    ]
    
    try:
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="Sheet1!A:AI",
            valueInputOption="USER_ENTERED",
            body={"values": [row]}
        ).execute()
        
        print(f"Dados inseridos: {result.get('updates', {}).get('updatedRows', 0)} linhas")
        
    except HttpError as e:
        print(f"Erro ao inserir no Google Sheets: {e}")