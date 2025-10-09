# Arquivo: data_controller.py

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

# O Controller importa as funções de persistência do Model
from database_model import create_sql_engine, upload_dataframe_to_sql, fetch_processed_ids


# A string de data/hora do site virá no formato DDMMAAAA_HHMM (e.g., '02102025_1400')
DATA_FORMAT_SITE = "%d-%m-%Y_%H%M"


def transform_data(df: pd.DataFrame, data_atualizacao_str: str) -> pd.DataFrame:
    """Aplica as transformações e cria colunas de metadados conforme o dicionário do BD."""
    
    print("Controller: Iniciando a transformação dos dados...")
    
    # 1. Renomear colunas do CSV para o padrão da tabela LIQUIDADOS
    df.rename(columns={
        'nossoNumero': 'NossoNumero',
        'cpfCnpj': 'CpfCnpj',
        'nomeSacado': 'NomeSacado',
        'dataVencimento': 'DataVencimento',
        'vlrPago': 'VlrPago'
    }, inplace=True)
    
    #region CpfCnpj
    COMPRIMENTO_CNPJ = 14
    df['CpfCnpj'] = df['CpfCnpj'].astype(str).str.zfill(COMPRIMENTO_CNPJ)
    #endregion CpfCnpj
    

    # region VlrPago
    df['VlrPago'] = (
        df['VlrPago']
        .str.replace('R$\xa0', '', regex=False)
        .str.replace('.', '', regex=False)
        .str.replace(',', '.', regex=False)
    )
    df['VlrPago'] = pd.to_numeric(df['VlrPago'], errors='coerce') # Converte para numérico
    # endregion VlrPago
    
    # 3. Tratamento de DataVencimento para tipo Date
    # Remove a informação de fuso horário/hora, deixando apenas a data (YYYY-MM-DD)
    df['DataVencimento'] = pd.to_datetime(df['DataVencimento']).dt.normalize().dt.date

    # 4. Criação de Colunas de Metadados (Conforme Dicionário)
    
    # DataImportacao (data e hora)
    data_importacao = datetime.now()
    df['DataImportacao'] = data_importacao
    
    #region DataUltAtualizacao (data e hora extraída do site)
    try:
        # data_atualizacao_str já vem formatada como DDMMAAAA_HHMM do extrair_data_hora_da_pagina
        data_ultima_atualizacao = datetime.strptime(data_atualizacao_str, DATA_FORMAT_SITE)
    except ValueError:
        print(f"ERRO: Falha ao converter data do site ('{data_atualizacao_str}'). Usando DataImportacao.")
        data_ultima_atualizacao = data_importacao
        
    df['DataUltAtualizacao'] = data_ultima_atualizacao
    #endregion DataUltAtualizacao (data e hora extraída do site)


    #region NossoNumeroFormatado (Texto)    
    # Regra: 121 / 0[NossoNumero_Padded]-[Último Dígito]
    
    nosso_numero_str = df['NossoNumero'].astype(str)
    
    ultimo_digito = nosso_numero_str.str[-1]
    
    nosso_numero_corpo = nosso_numero_str.str[:-1]
    
    df['NossoNumeroFormatado'] = (
        "121 / 0" 
        + nosso_numero_corpo 
        + "-" 
        + ultimo_digito
    )
    #endregion NossoNumeroFormatado (Texto)
    
    logging.info("Controller: Transformação concluída.")
    
    df.to_excel("debug_transformacao.xlsx", index=False)  # Debug: Salva o DataFrame transformado em Excel
    
    # 5. Seleciona e retorna colunas na ordem do banco
    return df[['NossoNumero', 'CpfCnpj', 'NomeSacado', 'DataVencimento', 'VlrPago', 
               'NossoNumeroFormatado', 'DataImportacao', 'DataUltAtualizacao']].copy()

def process_and_upload(caminho_arquivo_csv: Path, data_atualizacao_str: str):
    """Função principal do Controller: Orquestra a ETL (Extração, Transformação e Carga)."""
    
    try:
        # 1. Carrega dados brutos
        print(f"Controller: Lendo arquivo CSV: {caminho_arquivo_csv.name}")
        df_raw = pd.read_excel(caminho_arquivo_csv) # Conteúdo de exemplo:
        
        # 2. Transformação inicial (inclui limpeza de VlrPago e formatação de datas)
        df_transformed = transform_data(df_raw, data_atualizacao_str)
        
        # --- NOVA LÓGICA DE DEDUPLICAÇÃO ---
        
        # 2.1. Inicia a conexão com o banco
        sql_engine = create_sql_engine()
        
        # 2.2. Busca os IDs já processados
        processed_ids = fetch_processed_ids(sql_engine) # Busca IDs (ex: '11218353514')
        print(processed_ids)
        
        if processed_ids:
            # Garante que a coluna de comparação do DataFrame também seja uma string de 11 dígitos
            df_transformed['NossoNumero_Str'] = df_transformed['NossoNumero'].astype(str).str[-11:]
            
            # Filtra o DataFrame: mantém APENAS os títulos cujo NossoNumero AINDA NÃO estão na lista de processados
            df_to_upload = df_transformed[~df_transformed['NossoNumero_Str'].isin(processed_ids)].copy()
            
            # Remove a coluna auxiliar antes do upload
            del df_to_upload['NossoNumero_Str']
            
            num_duplicates = len(df_transformed) - len(df_to_upload)
            print(f"Controller: {num_duplicates} títulos duplicados (já processados com Ocorrência 579) foram removidos.")
            print(f"Controller: {len(df_to_upload)} novos títulos prontos para upload.")
            
        else:
            # Se a busca falhou ou retornou vazia, subimos todos os transformados.
            df_to_upload = df_transformed.copy()

        # 3. Persistência (Chama o Model para salvar APENAS os novos)
        if not df_to_upload.empty:
            upload_dataframe_to_sql(df_to_upload, sql_engine)
        else:
            print("Controller: Nenhum novo título para fazer upload. Encerrando upload.")

        return True
    
    except Exception as e:
        print(f"ERRO CRÍTICO no Controller: {e}")
        return False
    
    
#process_and_upload(Path("downloads/TitulosRecebidos_08-10-2025_1100.xlsx"), "07-10-2025_1100")

