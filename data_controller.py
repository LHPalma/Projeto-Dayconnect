import pandas as pd
from datetime import datetime
from pathlib import Path

# Importa a função de inserção da camada Model
from database_model import create_sql_engine, upload_dataframe_to_sql

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica as transformações necessárias para adequar o DataFrame ao schema do BD."""
    print("Controller: Iniciando a transformação dos dados...")
    
    # 1. Renomear colunas para o padrão da tabela LIQUIDADOS
    df.rename(columns={
        'nossoNumero': 'NossoNumero',
        'cpfCnpj': 'CpfCnpj',
        'nomeSacado': 'NomeSacado',
        'dataVencimento': 'DataVencimento',
        'vlrPago': 'VlrPago'
    }, inplace=True)

    # 2. Tratamento de VlrPago para decimal (Removendo "R$ ", pontos e trocando vírgula por ponto)
    # df['VlrPago'] é lido como string com "R$ 921,73"
    df['VlrPago'] = (
        df['VlrPago']
        .str.replace('R$\xa0', '', regex=False)  # Remove "R$ " e o caracter non-breaking space
        .str.replace('.', '', regex=False)        # Remove separador de milhar
        .str.replace(',', '.', regex=False)        # Troca vírgula por ponto decimal
    )
    df['VlrPago'] = pd.to_numeric(df['VlrPago'], errors='coerce')
    
    # 3. Tratamento de DataVencimento para tipo Date
    # df['dataVencimento'] é lido como string com fuso horário (ex: 2025-09-25T00:00:00-03:00)
    df['DataVencimento'] = pd.to_datetime(df['DataVencimento']).dt.normalize().dt.date

    # 4. Adicionar colunas de metadados
    data_atual = datetime.now()
    df['NossoNumeroFormatado'] = df['NossoNumero'].astype(str)
    df['DataImportacao'] = data_atual
    df['DataUltAtualizacao'] = data_atual
    
    print("Controller: Transformação concluída.")
    df.to_excel("Teste.xlsx", index=False)
    return df[['NossoNumero', 'CpfCnpj', 'NomeSacado', 'DataVencimento', 'VlrPago', 'NossoNumeroFormatado', 'DataImportacao', 'DataUltAtualizacao']]


def process_and_upload(caminho_arquivo_excel: Path):
    """Função principal do Controller: Orquestra o processamento e o upload."""
    
    try:
        print(f"Controller: Lendo arquivo XLSX: {caminho_arquivo_excel.name}")
        df_raw = pd.read_excel(caminho_arquivo_excel)

        # Chama a função de transformação
        df_transformed = transform_data(df_raw)
        
        # Conecta ao banco de dados (cria a Engine)
        sql_engine = create_sql_engine()
        
        # Envia os dados transformados para o Model
        upload_dataframe_to_sql(df_transformed, sql_engine)
        
        return True
    
    except FileNotFoundError:
        print(f"ERRO: Arquivo não encontrado no caminho: {caminho_arquivo_excel}")
        return False
    except ImportError as e:
        print(f"ERRO: Verifique se as bibliotecas (pandas/sqlalchemy) estão instaladas. Detalhes: {e}")
        return False
    except Exception as e:
        print(f"ERRO CRÍTICO no Controller: {e}")
        return False

from pathlib import Path

CWD = Path.cwd()
download_path = CWD / "downloads" / "TitulosRecebidos_02102025_1643.xlsx"
transform_data(pd.read_excel(download_path))