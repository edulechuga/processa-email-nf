# Diretiva: Processar NF por E-mail

## Objetivo
Monitorar e-mail da Microsoft Outlook, extrair dados de Notas Fiscais em PDF e salvar no Google Sheets.

## Entradas
- E-mails de remetentes específicos com anexos PDF
- Credenciais Azure AD para Microsoft Graph API
- Credenciais Google para acesso ao Sheets

## Fluxo
1. **fetch_emails.py** - Polling a cada 60s no Microsoft Graph API
   - Filtro: `from in (d.oliveira@custom.biz, s.oliveira@custom.biz, edulechuga@gmail.com) AND hasAttachments eq true`
   - Baixa anexos e marca e-mail como lido

2. **extract_pdf_text.py** - Extrai texto do PDF usando PyPDF2

3. **extract_nf_data.py** - Envia texto para LLM (Gemini via OpenRouter)
   - Extrai campos estruturados da NF em JSON

4. **save_to_sheets.py** - Insere dados no Google Sheets
   - ID da planilha: `1pzx7_ad-wWKwhkLTb43OadSxFonVcyJXjDoV-uxCgDA`
   - Matching por "Chave de Acesso da NF-E"

## Variáveis de Ambiente
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`
- `OPENROUTER_API_KEY`
- `SPREADSHEET_ID`

## Arquivos Necessários
- `credentials.json` - OAuth Google (baixe do Google Cloud Console)
- `token.json` - Token de acesso Google (gerado automaticamente)

## Execução
```bash
pip install -r requirements.py
python main.py
```

## Logging
Armazena IDs processados em `.tmp/processed_emails.json` para evitar duplicatas.