import urllib
from sqlalchemy import create_engine, text
import pyodbc # Importar pyodbc é necessário para que a engine funcione

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
SERVER = "192.168.1.219"
DATABASE = "PLANEJAMENTO"
USERNAME = "Planejamento"
PASSWORD = "xmypKOjvRxucrm9o"
# Nome do driver que funcionou na sua máquina
DRIVER_NAME = "SQL Server" 
TABLE_NAME = "LIQUIDADOS"


def create_sql_engine():
    """Cria e retorna o objeto Engine do SQLAlchemy."""
    
    # 1. Montagem da String de Conexão ODBC (ajustada para URL)
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER_NAME}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD}"
    )

    # 2. Criação da Engine de Conexão
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    return engine

def upload_dataframe_to_sql(df, engine):
    """Insere o DataFrame Pandas na tabela LIQUIDADOS."""
    from pandas.io.sql import types as sqltypes
    
    print(f"\nModel: Conectando e inserindo {len(df)} registros na tabela {TABLE_NAME}...")
    
    # Mapeamento explícito dos tipos de dados para o SQL Server
    dtype_mapping = {
        'NossoNumero': sqltypes.BigInteger(),
        'CpfCnpj': sqltypes.Unicode(length=50), # Adaptado para CPF/CNPJ
        'NomeSacado': sqltypes.Unicode(length=255),
        'DataVencimento': sqltypes.Date(),
        'VlrPago': sqltypes.Numeric(precision=10, scale=2),
        'NossoNumeroFormatado': sqltypes.Unicode(length=50),
        'DataImportacao': sqltypes.DateTime(),
        'DataUltAtualizacao': sqltypes.DateTime()
    }

    df.to_sql(
        TABLE_NAME, 
        con=engine, 
        if_exists='append', # Adiciona novos registros
        index=False,
        chunksize=1000,
        dtype=dtype_mapping
    )
    
    print("Model: Dados inseridos no SQL Server com sucesso.")