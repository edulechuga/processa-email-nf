# ==========================================
# FLUXO PRINCIPAL COM ARQUITETURA DE DUAS PASSADAS
# ==========================================
def run_pipeline(drive_service, ai_client, sheet):
    # Limpa a pasta temporária local antes de cada execução
    for item in TMP_DIR.glob('*'):
        item.unlink()

    # --- FASE 1: STAGING ---
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    
    # Adicionado supportsAllDrives para contas corporativas
    results = drive_service.files().list(
        q=query, 
        fields="files(id, name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    
    drive_files = results.get('files',[])
    
    # O HEARTBEAT: Avisa que olhou a pasta, mesmo que esteja vazia
    print(f"[{time.strftime('%H:%M:%S')}] Vasculhando pasta... Encontrados {len(drive_files)} arquivos visíveis para o robô.")
    
    if not drive_files:
        return

    drive_file_map = {} 

    print(f"  -> Iniciando preparação dos arquivos...")

    # Descompacta ZIPs
    for f in drive_files:
        if f['name'].lower().endswith('.zip'):
            zip_path = download_file_from_drive(drive_service, f['id'], f['name'])
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(TMP_DIR)
            zip_path.unlink()
            drive_service.files().delete(fileId=f['id']).execute()

    # Baixa arquivos restantes e divide PDFs
    drive_files_after_zip = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get('files',[])
    for f in drive_files_after_zip:
        if not f['name'].lower().endswith(('.pdf', '.xml')):
            continue
        local_path = download_file_from_drive(drive_service, f['id'], f['name'])
        if local_path.suffix.lower() == '.pdf':
            split_pdfs = split_multipage_pdf(local_path)
            for spdf in split_pdfs:
                drive_file_map[spdf] = f['id'] 
        else:
            drive_file_map[local_path] = f['id']

    processed_nf_numbers = set()
    drive_ids_to_delete = set()

    # --- FASE 2: PROCESSAMENTO DE XML ---
    xml_files = list(TMP_DIR.glob('*.xml'))
    if xml_files:
        print("  -> Fase 2: Processando arquivos XML (prioridade máxima)...")
        for xml_path in xml_files:
            try:
                dados_json = process_with_xml(xml_path)
                if dados_json:
                    nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                    if nf_number:
                        
                        dados_nf=dados_json.get("Dados da NF", {}); dest=dados_json.get("Campos do destinatário",{}); faturas=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); produtos=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                        fat1=faturas[0] if faturas else {}; fat2=faturas[1] if len(faturas) > 1 else {}; prod1=produtos[0] if produtos else {}
                        row=[dados_nf.get("Data",""),dados_nf.get("Número da NF",""),dados_nf.get("Chave de Acesso da NF-E",""),dados_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),prod1.get("Cod. Produto",""),prod1.get("Descrição do prod/serv.",""),prod1.get("NCM",""),prod1.get("CST",""),prod1.get("CFOP",""),prod1.get("UN",""),prod1.get("QUANT",""),prod1.get("V. UNITARIO",""),prod1.get("V. TOTAL",""),prod1.get("BC ICMS",""),prod1.get("V ICMS",""),prod1.get("V IPI",""),prod1.get("A ICMS",""),prod1.get("A IPI",""),adic.get("Informações complementares","")]
                        
                        sheet.append_row(row)
                        print(f"[XML SUCESSO] NF '{nf_number}' salva no Sheets.")
                        processed_nf_numbers.add(nf_number)
                        drive_ids_to_delete.add(drive_file_map.get(xml_path))
            except Exception as e:
                print(f"    [ERRO XML] Falha ao processar '{xml_path.name}': {e}")
    
    # --- FASE 3: PROCESSAMENTO DE PDF ---
    pdf_files = list(TMP_DIR.glob('*.pdf'))
    if pdf_files:
        print("  -> Fase 3: Processando arquivos PDF...")
        for pdf_path in pdf_files:
            try:
                pdf_text = extract_text_from_pdf(pdf_path)
                dados_json = process_with_ai(ai_client, pdf_text)

                if dados_json:
                    nf_number = dados_json.get("Dados da NF", {}).get("Número da NF")
                    if nf_number and nf_number in processed_nf_numbers:
                        print(f"    [PDF DUPLICADO] NF '{nf_number}' já processada via XML. Ignorando.")
                        drive_ids_to_delete.add(drive_file_map.get(pdf_path))
                    elif nf_number:
                        
                        dados_nf=dados_json.get("Dados da NF", {}); dest=dados_json.get("Campos do destinatário",{}); faturas=dados_json.get("Faturas",[]); transp=dados_json.get("Transportador",{}); produtos=dados_json.get("Produtos",[]); adic=dados_json.get("Dados adicionais",{})
                        fat1=faturas[0] if faturas else {}; fat2=faturas[1] if len(faturas) > 1 else {}; prod1=produtos[0] if produtos else {}
                        row=[dados_nf.get("Data",""),dados_nf.get("Número da NF",""),dados_nf.get("Chave de Acesso da NF-E",""),dados_nf.get("Natureza da operação",""),dest.get("Nome/Razao Social",""),dest.get("CNPJ/CPF",""),dest.get("Endereço",""),dest.get("Bairro/Distrito",""),dest.get("CEP",""),dest.get("Municipio",""),dest.get("UF",""),dest.get("Inscrição Estadual",""),fat1.get("Data de vencimento",""),fat1.get("Valor",""),fat2.get("Data de vencimento",""),fat2.get("Valor",""),dados_json.get("Valor total da Nota Fiscal",""),transp.get("Razao Social",""),transp.get("Quantidade",""),transp.get("Especie",""),prod1.get("Cod. Produto",""),prod1.get("Descrição do prod/serv.",""),prod1.get("NCM",""),prod1.get("CST",""),prod1.get("CFOP",""),prod1.get("UN",""),prod1.get("QUANT",""),prod1.get("V. UNITARIO",""),prod1.get("V. TOTAL",""),prod1.get("BC ICMS",""),prod1.get("V ICMS",""),prod1.get("V IPI",""),prod1.get("A ICMS",""),prod1.get("A IPI",""),adic.get("Informações complementares","")]
                        
                        sheet.append_row(row)
                        print(f"    [PDF SUCESSO] NF '{nf_number}' salva no Sheets.")
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