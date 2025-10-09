import urllib
# Importante: precisamos do 'text' para consultas SQL em string
from sqlalchemy import create_engine, text 
import pyodbc 
from dotenv import load_dotenv; load_dotenv()


# --- CONFIGURAÇÕES DO BANCO DE DADOS ---
SERVER = load_dotenv("DB_SERVER")
DATABASE = load_dotenv("DB_DATABASE")
USERNAME = load_dotenv("DB_USERNAME")
PASSWORD = load_dotenv("DB_PASSWORD")

DRIVER_NAME = load_dotenv("DB_DRIVER_NAME")


def create_sql_engine():
    """Cria e retorna o objeto Engine do SQLAlchemy para o SQL Server."""
    
    # Montagem da String de Conexão ODBC
    params = urllib.parse.quote_plus(
        f"DRIVER={{{DRIVER_NAME}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD}"
    )

    # Criação da Engine
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    return engine

def testar_conexao(engine):
    """Testa se a engine consegue se conectar e buscar a data/hora do servidor."""
    try:
        print("Testando conexão...")
        with engine.connect() as connection:
            
            result = connection.execute(text("SELECT GETDATE()")) 
            
            
            data_hora_servidor = result.fetchone()[0]
            print("SUCESSO: Conexão com o SQL Server estabelecida!")
            print(f"Data/Hora do Servidor: {data_hora_servidor}")
            return True
    except Exception as e:
        print(f"\nERRO DE CONEXÃO: Falha ao conectar usando o driver '{DRIVER_NAME}'.")
        print(f"Detalhes do Erro: {e}")
        return False

# --- EXECUÇÃO ---
if __name__ == "__main__":
    sql_engine = create_sql_engine()
    testar_conexao(sql_engine)