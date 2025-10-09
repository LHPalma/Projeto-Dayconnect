import os
from typing import Any, Dict, List
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv; load_dotenv()
import urllib
from pathlib import Path
from datetime import datetime



# --- CAMINHOS ---
PASTA_RELATORIOS = Path.cwd() / "relatorios"
PASTA_RELATORIOS.mkdir(exist_ok=True)


# --- CONFIGURAÇÕES DE CONEXÃO ---
DB_SERVER = os.getenv("DB_SERVER", "192.168.1.219")
DB_DATABASE = os.getenv("DB_DATABASE", "PLANEJAMENTO")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def criar_engine_sql_server():
    """
    Cria e retorna uma engine do SQLAlchemy para o SQL Server.
    """
    try:
        # Codifica a senha para ser usada na URL, caso tenha caracteres especiais
        password_encoded = urllib.parse.quote_plus(DB_PASSWORD)

        # String de conexão no formato SQLAlchemy para SQL Server com pyodbc
        conn_str = (
            f"mssql+pyodbc://{DB_USERNAME}:{password_encoded}@{DB_SERVER}/{DB_DATABASE}?"
            "driver=ODBC+Driver+17+for+SQL+Server"
        )
        print(f"Criando engine de conexão para o banco de dados: {DB_SERVER}...")
        engine = create_engine(conn_str)

        with engine.connect() as connection:
            print("Conexão bem-sucedida!")
        return engine

    except Exception as e:
        print(f"ERRO: Falha ao criar a engine de conexão com o banco: {e}")
        return None

def gerar_dataframe_vcon(engine):
    """
    Executa a query principal usando a engine do SQLAlchemy e retorna os dados como um DataFrame pandas.
    """
    if engine is None:
        print("A execução foi interrompida porque a engine de conexão não foi criada.")
        return None

    query = text("""
    SELECT
        t.cod_cred AS 'Código do Credor',
        '11' AS Operação,
        RTRIM(vd.CPFCGC_PES) AS 'CPF/CNPJ',
        RTRIM(CONTRATO_TIT) AS Contrato,
        'BAIXAS - LIQUIDADOS D0' AS 'Título da Ocorrência',
        CONCAT('Baixa bol nº ', RTRIM(BE.IdNossoNumero), ', valor pago R$', BE.ValorVencimento) AS Complemento,
        ' ' AS 'Data de Promessa',
        FORMAT(GETDATE(), 'dd/MM/yyyy HH:mm') AS 'Data/Hora da Ocorrência',
        ' ' AS 'Código do Usuário',
        ' ' AS 'Nome do Usuário',
        ' ' AS 'Número da parcela?'
    FROM [192.168.0.143].cobsystems3.dbo.Boletos_em BE WITH (NOLOCK)
    INNER JOIN [192.168.0.143].cobsystems3.dbo.TITULOS T WITH (NOLOCK)
        ON BE.Cod_Tit = T.COD_TIT
    INNER JOIN [192.168.0.143].cobsystems3.dbo.V_DEVEDORES VD WITH (NOLOCK)
        ON T.COD_DEV = VD.COD_DEV
    WHERE
        REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(BE.IdNossoNumero)), ' ', ''), '-', ''), '121/' , '') COLLATE SQL_Latin1_General_CP1_CI_AS  IN (
        SELECT
        L.NossoNumero
    FROM
        [PLANEJAMENTO].[dbo].[LIQUIDADOS] L
    WHERE
        L.NossoNumero NOT IN (
            SELECT
                REPLACE(REPLACE(LTRIM(RTRIM(SUBSTRING(
                    A.COMPLEMENTO_HIST_CLI,
                    CHARINDEX('/', A.COMPLEMENTO_HIST_CLI) + 1,
                    CHARINDEX(',', A.COMPLEMENTO_HIST_CLI) - CHARINDEX('/', A.COMPLEMENTO_HIST_CLI) - 1
                ))), ' ', ''), '-', '')
            FROM [192.168.0.143].cobsystems3.dbo.HISTORICOS_CLIENTES A WITH (NOLOCK)
            INNER JOIN [192.168.0.143].cobsystems3.dbo.HISTORICOS_CLIENTES_TITULOS B WITH (NOLOCK)
                ON A.COD_HIST_CLI = B.COD_HIST_CLI
            INNER JOIN [192.168.0.143].cobsystems3.dbo.TITULOS C WITH (NOLOCK)
                ON B.COD_TIT = C.COD_TIT
            WHERE
                A.COD_OCOR = '579'
                AND CONVERT(DATE, A.DATA_CAD) = CONVERT(DATE, GETDATE())
        )
        AND CONVERT(DATE, L.DataUltAtualizacao) = CONVERT(DATE, GETDATE())
        )
    ORDER BY
        T.cod_cred;
    """)
    try:
        print("Executando a query para gerar o relatório...")
        df = pd.read_sql(query, engine)
        print("Query executada com sucesso!")
        return df
    except Exception as e:
        print(f"ERRO ao executar a query: {e}")
        return None

def salvar_relatorios_por_credor(df_completo):
    """
    Filtra o DataFrame por 'Código do Credor', salva um arquivo para cada um,
    e retorna uma lista de dicionários com os caminhos dos arquivos.
    """
    if df_completo is None or df_completo.empty:
        print("Nenhum dado para processar. Nenhum relatório será salvo.")
        return []  # MODIFICAÇÃO: Retorna uma lista vazia

    lista_para_upload: List[Dict[str, Any]] = []

    mapa_credores = {
        4: "Daycoval Veiculos",
        5: "Daycoval Juridico",
        6: "ICR Advogados",
        9: "Daycoval Daycred",
        16: "Daycoval Focos"
    }
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    grupos_de_credores = df_completo.groupby('Código do Credor')

    print("\nIniciando a geração de relatórios por credor...")
    for codigo_credor, df_credor in grupos_de_credores:
        nome_credor = mapa_credores.get(codigo_credor, "Outros")
        nome_arquivo = f"{nome_credor}_{timestamp}.csv"
        caminho_arquivo = PASTA_RELATORIOS / nome_arquivo

        try:
            df_credor.to_csv(caminho_arquivo, index=False, encoding="utf-8-sig")
            print(f"  - Relatório salvo com sucesso em: {caminho_arquivo}")
            
            lista_para_upload.append({
                "arquivo": str(caminho_arquivo.resolve()), # Usar .resolve() para caminho absoluto
                "credor": nome_credor
            })
            
        except Exception as e:
            print(f"  - ERRO ao salvar o relatório para o credor {codigo_credor}: {e}")

    # MODIFICAÇÃO PRINCIPAL: Adicionado o retorno da lista
    return lista_para_upload


def main():
    """
    Função principal que orquestra a conexão, a geração do DataFrame,
    o salvamento dos arquivos e retorna a lista de arquivos gerados.
    """
    sql_engine = criar_engine_sql_server()
    df_relatorio = gerar_dataframe_vcon(sql_engine)
    arquivos_gerados = []

    if df_relatorio is not None and not df_relatorio.empty:
        print(f"\nRelatório geral gerado com {len(df_relatorio)} registros.")

        arquivos_gerados = salvar_relatorios_por_credor(df_relatorio)
    else:
        print("\nNenhum relatório gerado, o DataFrame está vazio.")

    if sql_engine:
        # Encerra a pool de conexões da engine
        sql_engine.dispose()
        print("\nEngine de conexão com o banco de dados finalizada.")
    
    
    # Retorna a lista de arquivos (que estará preenchida ou vazia)
    return arquivos_gerados


if __name__ == "__main__":
    main()