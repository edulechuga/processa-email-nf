Você deve receber um arquivo PDF que se trata de uma nota fiscal.

Por favor identifique os seguintes campos da NF:
Data, Número da NF, Chave de Acesso da NF-E, Natureza da operação

E os seguintes campos do destinatário:
Nome/Razao Social, CNPJ/CPF, Endereço, Bairro/Distrito, CEP, Municipio, UF, Inscrição Estadual

Datas de vencimentos das faturas e respectivos valores (capture todos os disponíveis, até o limite de 8).
...
"Faturas": [
    { "Data de vencimento 1": "", "Valor 1": "" },
    { "Data de vencimento 2": "", "Valor 2": "" },
    { "Data de vencimento 3": "", "Valor 3": "" },
    { "Data de vencimento 4": "", "Valor 4": "" },
    { "Data de vencimento 5": "", "Valor 5": "" },
    { "Data de vencimento 6": "", "Valor 6": "" },
    { "Data de vencimento 7": "", "Valor 7": "" },
    { "Data de vencimento 8": "", "Valor 8": "" }
],

Valor total da Nota Fiscal.

Os seguintes campos da transportador:
Razao Social, Quantidade, Especie

Dados dos produtos:
Cod. Produto, Descrição do prod/serv., NCM, CST, CFOP, UN, QUANT, V. UNITARIO, V. TOTAL, BC ICMS, V ICMS, V IPI, A ICMS, A IPI

Dados adicionais:
Informações complementares

Por favor, forneça as informações extraídas apenas em formato JSON válido, sem nenhuma marcação adicional ou quebras de linha.

{
  "Dados da NF": {
    "Data": "", "Número da NF": "", "Chave de Acesso da NF-E": "", "Natureza da operação": ""
  },
  "Campos do destinatário": {
    "Nome/Razao Social": "", "CNPJ/CPF": "", "Endereço": "", "Bairro/Distrito": "",
    "CEP": "", "Municipio": "", "UF": "", "Inscrição Estadual": ""
  },
  "Faturas":[{"Data de vencimento": "", "Valor": ""}],
  "Valor total da Nota Fiscal": "",
  "Transportador": {"Razao Social": "", "Quantidade": "", "Especie": ""},
  "Produtos":[{
      "Cod. Produto": "", "Descrição do prod/serv.": "", "NCM": "", "CST": "", "CFOP": "", "UN": "",
      "QUANT": "", "V. UNITARIO": "", "V. TOTAL": "", "BC ICMS": "", "V ICMS": "", "V IPI": "", "A ICMS": "", "A IPI": ""
  }],
  "Dados adicionais": {"Informações complementares": ""}
}