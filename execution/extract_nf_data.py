import os
import json
from openrouter import OpenRouter
from dotenv import load_dotenv

load_dotenv()

client = OpenRouter(api_key=os.getenv("OPENROUTER_API_KEY"))

SYSTEM_PROMPT = """Você deve receber um arquivo PDF que se trata de uma nota fiscal.

Por favor identifique os seguintes campos da NF:

Data
Número da NF
Chave de Acesso da NF-E
Natureza da operação

E os seguintes campos do destinatário:

Nome/Razao Social
CNPJ/CPF
Endereço
Bairro/Distrito
CEP
Municipio
UF
Inscrição Estadual

Datas de vencimentos das faturas e respectivos valores.

Valor total da Nota Fiscal.

Os seguintes campos da transportador:

Razao Social
Quantidade
Especie

Dados dos produtos:
Cod. Produto
Descrição do prod/serv.
NCM
CST
CFOP
UN
QUANT
V. UNITARIO
V. TOTAL
BC ICMS
V ICMS
V IPI
A ICMS
A IPI

Dados adicionais:
Informações complementares

Por favor, forneça as informações extraídas apenas em formato JSON válido, sem nenhuma marcação adicional ou quebras de linha.
"""

JSON_TEMPLATE = json.dumps({
    "Dados da NF": {
        "Data": "",
        "Número da NF": "",
        "Chave de Acesso da NF-E": "",
        "Natureza da operação": ""
    },
    "Campos do destinatário": {
        "Nome/Razao Social": "",
        "CNPJ/CPF": "",
        "Endereço": "",
        "Bairro/Distrito": "",
        "CEP": "",
        "Municipio": "",
        "UF": "",
        "Inscrição Estadual": ""
    },
    "Faturas": [
        {
            "Data de vencimento": "",
            "Valor": ""
        }
    ],
    "Valor total da Nota Fiscal": "",
    "Transportador": {
        "Razao Social": "",
        "Quantidade": "",
        "Especie": ""
    },
    "Produtos": [
        {
            "Cod. Produto": "",
            "Descrição do prod/serv.": "",
            "NCM": "",
            "CST": "",
            "CFOP": "",
            "UN": "",
            "QUANT": "",
            "V. UNITARIO": "",
            "V. TOTAL": "",
            "BC ICMS": "",
            "V ICMS": "",
            "V IPI": "",
            "A ICMS": "",
            "A IPI": ""
        }
    ],
    "Dados adicionais": {
        "Informações complementares": ""
    }
}, ensure_ascii=False)

def extract_nf_data(pdf_text):
    try:
        response = client.chat.completions.create(
            model="google/gemini-2.0-flash-001",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": pdf_text[:8000]}
            ]
        )
        
        result = response.choices[0].message.content
        
        result = result.replace("```json", "").replace("```", "").strip()
        
        return json.loads(result)
        
    except Exception as e:
        print(f"Erro ao extrair dados da NF: {e}")
        return None