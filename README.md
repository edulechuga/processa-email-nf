# Processar NF Email

Serviço Python para processar Notas Fiscais recebidas por e-mail e salvar no Google Sheets.

## Estrutura

```
.
├── main.py                    # Script principal (daemon)
├── requirements.txt           # Dependências Python
├── directives/                # Prompts e diretrizes
│   └── system_prompt_nf.md    # Prompt para extração de dados da NF
├── execution/                 # Scripts de execução
│   ├── fetch_emails.py        # (não usado - código está em main.py)
│   ├── extract_pdf_text.py   # (não usado - código está em main.py)
│   ├── extract_nf_data.py   # (não usado - código está em main.py)
│   └── save_to_sheets.py     # (não usado - código está em main.py)
├── .env                       # Variáveis de ambiente (não commit)
└── credentials.json           # Google OAuth (não commit)
```

## Configuração

1. **Criar arquivo `.env`** com as variáveis necessárias:
   - `MS_CLIENT_ID`, `MS_CLIENT_SECRET`, `MS_TENANT_ID` - Azure AD
   - `OPENROUTER_API_KEY` - Chave da OpenRouter
   - `GOOGLE_SHEET_ID` - ID da planilha Google

2. **Baixar `credentials.json`** do Google Cloud Console

3. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Executar:**
   ```bash
   python main.py
   ```

## Gerenciamento do Serviço (LXC Proxmox)

No container, o serviço roda via **systemd**:
- **Nome do serviço:** `nf-processor.service`
- **Status:** `systemctl status nf-processor`
- **Logs:** `journalctl -u nf-processor -f`
- **Reiniciar:** `systemctl restart nf-processor`

## Permissões necessárias

- Microsoft Graph API (e-mail)
- Google Sheets API (escrita)