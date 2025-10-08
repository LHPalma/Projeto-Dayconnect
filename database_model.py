import urllib
from sqlalchemy import create_engine, text
import pandas as pd
import pyodbc # Importar pyodbc é necessário para que a engine funcione

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
    from sqlalchemy import types as sqltypes
    
    print(f"\nModel: Conectando e inserindo {len(df)} registros na tabela {TABLE_NAME}...")
    
    # Mapeamento explícito dos tipos de dados para o SQL Server
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
        if_exists='append', # Adiciona novos registros
        index=False,
        chunksize=1000,
        dtype=dtype_mapping
    )
    
    print("Model: Dados inseridos no SQL Server com sucesso.")
    
    
def fetch_processed_ids(engine) -> list[str]:
    """
    Executa a query fornecida pelo usuário para buscar todos os IDs de títulos 
    com ocorrência '579' registrada hoje no sistema de origem.
    Retorna uma lista de strings (IDs).
    """
    from sqlalchemy import text
    
    # A query original que busca IDs já processados no sistema de origem (cobsystems3)
    SQL_QUERY_PROCESSED_IDS = """
    SELECT 
RTRIM(CONTRATO_TIT) 
AS CONTRATO, 
D.TITULO_OCOR, 
REPLACE(REPLACE(LTRIM(RTRIM(SUBSTRING(COMPLEMENTO_HIST_CLI,CHARINDEX('/', COMPLEMENTO_HIST_CLI) + 1,
CHARINDEX(',', COMPLEMENTO_HIST_CLI) - CHARINDEX('/', COMPLEMENTO_HIST_CLI) - 1))),' ', ''), '-', '')
AS  ID, 
A.DATA_CAD 
FROM [192.168.0.143].cobsystems3.dbo.HISTORICOS_CLIENTES A	WITH(NOLOCK)
INNER JOIN [192.168.0.143].cobsystems3.dbo.HISTORICOS_CLIENTES_TITULOS B WITH(NOLOCK)
	ON A.COD_HIST_CLI = B.COD_HIST_CLI
INNER JOIN [192.168.0.143].cobsystems3.dbo.TITULOS C WITH(NOLOCK)
	ON B.COD_TIT = C.COD_TIT
INNER JOIN [192.168.0.143].cobsystems3.dbo.OCORRENCIAS_CLIENTES D WITH(NOLOCK)
	ON A.COD_OCOR = D.COD_OCOR
WHERE A.COD_OCOR = '579'
AND CONVERT(DATE, A.DATA_CAD) = CONVERT(DATE, GETDATE())
    """
    
    print("Model: Consultando IDs já processados no sistema de origem...")
    
    print("Model: Consultando IDs já processados no sistema de origem...")
    
    try:
        with engine.connect() as connection:
            
            df_processed = pd.read_sql(text(SQL_QUERY_PROCESSED_IDS), connection)
            
            # ALTERAÇÃO CHAVE: Pega os últimos 11 dígitos do 'ID' extraído 
            # para corresponder ao NossoNumero (11 digitos) do arquivo Dayconnect.
            processed_ids = df_processed['ID'].astype(str).str[-11:].tolist()
            print(f"Model: {len(processed_ids)} IDs já processados encontrados para exclusão.")
            return processed_ids

    except Exception as e:
        print(f"ERRO no Model (fetch_processed_ids): Falha ao executar a query de exclusão: {e}")
        return []
    

if __name__ == "__main__":
    fetch_processed_ids(create_sql_engine())
    destino_engine = create_sql_engine() 
    
    try:
        print("\n--- TESTE DE CONEXÃO: SERVIDOR DE DESTINO (192.168.1.219) ---")
        with destino_engine.connect() as connection:
            # Executa a query simples para confirmar a autenticação e o acesso
            result = connection.execute(text("SELECT GETDATE()"))
            data_hora_servidor = result.fetchone()[0]
            print("SUCESSO: Conexão básica com o Servidor de DESTINO estabelecida!")
            print(f"Data/Hora do Servidor (219): {data_hora_servidor}")
            
    except Exception as e:
        print("\nERRO CRÍTICO NA CONEXÃO DE DESTINO: Falha ao conectar ao 192.168.1.219.")
        print("A autenticação para o login 'Planejamento' falhou. Verifique as credenciais.")
        print(f"Detalhes: {e}")
        


    # Opcional: Adicione a lógica de teste do Servidor de Origem abaixo para isolar o problema 
    # (Com as credenciais substituídas)
    # origem_engine = create_sql_engine_origem()
    