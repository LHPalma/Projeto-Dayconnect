import urllib
from sqlalchemy import types as sqltypes
from sqlalchemy import create_engine, text
import pandas as pd
import pyodbc  # Importar pyodbc é necessário para que a engine funcione

# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
SERVER = "192.168.1.219"
DATABASE = "PLANEJAMENTO"
USERNAME = "svc_desenvolvimento"
PASSWORD = "Desenvolvimento#2740"
DRIVER_NAME = "ODBC Driver 17 for SQL Server"
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

    print(f"\nModel: Conectando e inserindo {len(df)} registros na tabela {TABLE_NAME}...")


    dtype_mapping = {
        'NossoNumero': sqltypes.BigInteger(),
        'CpfCnpj': sqltypes.NVARCHAR(length=50),
        'NomeSacado': sqltypes.NVARCHAR(length=255),
        'DataVencimento': sqltypes.Date(),
        'VlrPago': sqltypes.Numeric(precision=10, scale=2),
        'NossoNumeroFormatado': sqltypes.NVARCHAR(length=50),
        'DataImportacao': sqltypes.DateTime(),
        'DataUltAtualizacao': sqltypes.DateTime()
    }

    df.to_sql(
        TABLE_NAME,
        con=engine,
        if_exists='append',  # Adiciona novos registros
        index=False,
        chunksize=1000,
        dtype=dtype_mapping
    )

    print("Model: Dados inseridos no SQL Server com sucesso.")


def fetch_processed_ids(engine) -> list[str]:
    """
    Busca na tabela LIQUIDADOS todos os 'NossoNumero' que já foram
    processados na data de hoje.
    Retorna uma lista de strings contendo os números.
    """
    from sqlalchemy import text

    SQL_QUERY_PROCESSED_IDS = """
    SELECT [NossoNumero]
    FROM [PLANEJAMENTO].[dbo].[LIQUIDADOS]
    WHERE CONVERT(DATE, DataUltAtualizacao) = CONVERT(DATE, GETDATE())
    """

    print("Model: Consultando IDs já processados hoje na tabela LIQUIDADOS...")

    try:
        with engine.connect() as connection:
            df_processed = pd.read_sql(text(SQL_QUERY_PROCESSED_IDS), connection)

            processed_ids = df_processed['NossoNumero'].astype(str).tolist()
            print(f"Model: {len(processed_ids)} IDs já processados hoje foram encontrados.")
            return processed_ids

    except Exception as e:
        print(f"ERRO no Model (fetch_processed_ids): Falha ao executar a query: {e}")
        return []


if __name__ == "__main__":
    # Teste da função fetch_processed_ids
    engine = create_sql_engine()
    processed_ids = fetch_processed_ids(engine)
    print("IDs processados hoje:", processed_ids)

    # Teste de conexão
    try:
        print("\n--- TESTE DE CONEXÃO: SERVIDOR DE DESTINO (192.168.1.219) ---")
        with engine.connect() as connection:
            result = connection.execute(text("SELECT GETDATE()"))
            data_hora_servidor = result.fetchone()[0]
            print("SUCESSO: Conexão com o Servidor de DESTINO estabelecida!")
            print(f"Data/Hora do Servidor (219): {data_hora_servidor}")

    except Exception as e:
        print("\nERRO CRÍTICO NA CONEXÃO DE DESTINO: Falha ao conectar ao 192.168.1.219.")
        print(f"Detalhes: {e}")