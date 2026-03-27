# Histórico do Projeto - Processar NF por E-mail

## Objetivo
Migrar um fluxo do N8N (rodando em container LXC no Proxmox) para um serviço Python standalone que processa Notas Fiscais recebidas por e-mail e salva no Google Sheets.

## Arquitetura Original (N8N)
- **Trigger**: Microsoft Outlook (polling a cada 1 minuto)
- **Filtro**: E-mails de `d.oliveira@custom.biz`, `s.oliveira@custom.biz`, `edulechuga@gmail.com` com anexos
- **Processamento**: 
  1. Extrai anexos PDF
  2. Extrai texto do PDF (PyPDF2/n8n nodes)
  3. Envia para LLM (Gemini 2.0 Flash via OpenRouter)
  4. Extrai dados estruturados em JSON
  5. Salva no Google Sheets (spreadsheet ID: `1pzx7_ad-wWKwhkLTb43OadSxFonVcyJXjDoV-uxCgDA`)

## Estrutura do Projeto
```
processa-email-nf/
├── main.py                    # Script principal (daemon)
├── requirements.txt           # Dependências Python
├── .gitignore
├── README.md
├── .env                       # Variáveis de ambiente
├── directives/
│   ├── processar_nf_email.md # Diretiva do fluxo
│   └── system_prompt_nf.md   # Prompt para extração de dados da NF
└── AGENTE.md                  # Instruções do agente
```

## Credenciais e Configurações

### Microsoft Azure AD (App Registration)
- **Client ID**: `e3d20347-604d-43e4-ad23-656d8f13611b` (app antigo do N8N)
- **Client Secret**: `a6d982fc-5013-404a-9366-725025b96061`
- **Tenant ID**: `common`

### Novo App Registration criado
- **Client ID**: `cdb7ca0a-6ee6-4198-96b3-353f14ff7c99` (Processa NF Local)
- **Client Secret**: `7d36d739-8f8f-48fb-a9e8-96bdc1479ffa`
- **Tenant ID**: `common`
- **Configurações**:
  - Supported account types: "Any Entra ID Tenant + Personal Microsoft accounts"
  - Allow public client flows: Yes

### OpenRouter
- **API Key**: `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`
- **Modelo**: `google/gemini-2.5-flash`

### Google Sheets
- **Spreadsheet ID**: `1pzx7_ad-wWKwhkLTb43OadSxFonVcyJXjDoV-uxCgDA`
- **credentials.json**: Necessário (OAuth Google)

## Problema: Autenticação Microsoft

### Tentativa 1: Fluxo Padrão O365 (authenticate)
- Erro: "Need admin approval" - organização não permite apps não-verificados sem consentimento admin

### Tentativa 2: Device Code Flow (MSAL)
- Código Python com `msal.PublicClientApplication`
- Erro: "invalid_client" - app não é "mobile"

### Tentativa 3: ROPC (Resource Owner Password Credentials)
- Attempt implementar auth com username/password
- Problema: Conta tem MFA (2FA) - não funciona

### Tentativa 4: Novo App Registration (Public Client)
- Criado novo app com "Allow public client flows: Yes"
- Mesmo erro: "Need admin approval"

### Tentativa 5: Extrair token do N8N
- Banco SQLite do N8N em `~/.n8n/database.sqlite`
- Credencial Microsoft Outlook ID: `y3yUK9TYBoEZwUE2`
- Encryption key do N8N: `GTSkm5mWjw4ZHZy/wqxsbxARqtkqdULB`

#### Tentativas de descriptografar:
1. Node.js crypto - formato não reconhecido
2. Python cryptography (AES-256-GCM) - formato inválido
3. OpenSSL CLI (AES-256-CBC) - "bad magic number"

O dado encriptado está no formato N8N (OpenSSL-style com prefixo `U2FsdGVkX1`).

## Servidor de Destino ( onde o projeto está rodando)
- **Hostname**: `processa-email-nf`
- **SO**: Debian/Ubuntu (Provavelmente)
- **Python**: 3.11 em venv
- **Diretório**: `/opt/processa-email-nf`

## Servidor N8N (origem)
- **Hostname**: `n8n`
- **N8N**: Instalado via NVM (Node v20.19.2)
- **Dados**: `~/.n8n/database.sqlite` (~491MB)
- **Encryption key**: `GTSkm5mWjw4ZHZy/wqxsbxARqtkqdULB`

## Bibliotecas Python Usadas
```
O365
PyMuPDF (fitz)
openai
gspread
python-dotenv
requests
cryptography
```

## Questão Central
Como obter um token de acesso válido para o Microsoft Graph API sem aprovação de admin, considerando que:
1. A organização tem políticas restritivas
2. A conta tem MFA habilitado
3. O token existente está encriptado no banco do N8N

## Possíveis Soluções a Explorar
1. Usar o token descriptografado do N8N (requer decryption)
2. Criar app registration como "Mobile" no Azure (pode precisar admin)
3. Usar certificado em vez de client secret
4. Another approach para auth que não precise approval
5. another approach para extrair/refresh o token do N8N

## Código Principal (main.py)

### Função de inicialização atual (não funciona por falta de admin):
```python
def init_outlook():
    """Conecta ao Microsoft Graph API usando fluxo de autenticação."""
    credentials = (os.getenv("MS_CLIENT_ID"), os.getenv("MS_CLIENT_SECRET"))
    tenant_id = os.getenv("MS_TENANT_ID")
    token_backend = FileSystemTokenBackend(token_path='.', token_filename='token.json')
    account = Account(credentials, tenant_id=tenant_id, token_backend=token_backend)
    
    if not account.is_authenticated:
        account.authenticate(scopes=['basic', 'message_all'])
        
    return account.mailbox()
```

## Linhas de Comando Úteis

### No servidor N8N (origem):
```bash
# Listar credenciais
sqlite3 ~/.n8n/database.sqlite "SELECT id, name, type FROM credentials_entity;"

# Ver encryption key
cat ~/.n8n/config

# Extrair credencial específica
sqlite3 ~/.n8n/database.sqlite "SELECT data FROM credentials_entity WHERE id='y3yUK9TYBoEZwUE2';"
```

### No servidor de destino:
```bash
# Rodar o serviço
cd /opt/processa-email-nf
source venv/bin/activate
python main.py
```