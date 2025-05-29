import pandas as pd
import pyodbc
import numpy as np

# Caminho do CSV
csv_path = 'streamers_twitch.csv'

# Carrega o CSV
df = pd.read_csv(csv_path)

# Converte datas com dia primeiro (formato brasileiro)
df['Criacao_Canal'] = pd.to_datetime(df['Criacao_Canal'], errors='coerce', dayfirst=True)
df['Data_stream'] = pd.to_datetime(df['Data_stream'], errors='coerce', dayfirst=True).dt.date
df['Hora_stream'] = pd.to_datetime(df['Hora_stream'], format='%H:%M:%S', errors='coerce').dt.time

# Converte valores de viewers para float (substitui vírgulas por pontos)
df['Viewers'] = df['Viewers'].astype(str).str.replace(',', '.')
df['Viewers'] = pd.to_numeric(df['Viewers'], errors='coerce')


# Conexão com o SQL Server
server = '25.14.84.133'
database = 'twitch_data'
username = 'sa'
password = 'sa'
trusted_connection = 'no'  # como está usando usuário/senha

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Nome da tabela destino
table_name = 'streamers'

# Cria a tabela se não existir
cursor.execute(f"""
IF OBJECT_ID('{table_name}', 'U') IS NULL
BEGIN
    CREATE TABLE {table_name} (
        ID BIGINT PRIMARY KEY,
        Nome NVARCHAR(255),
        Nickname NVARCHAR(255),
        Descricao NVARCHAR(MAX),
        Criacao_Canal DATETIME,
        Categoria NVARCHAR(255),
        Viewers FLOAT,
        Linguagem NVARCHAR(10),
        Data_stream DATE,
        Hora_stream TIME
    )
END
""")
conn.commit()

# Insere os dados
for index, row in df.iterrows():
    try:
        cursor.execute(f"""
            INSERT INTO {table_name} 
            (ID, Nome, Nickname, Descricao, Criacao_Canal, Categoria, Viewers, Linguagem, Data_stream, Hora_stream)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        int(row.ID),
        str(row.Nome),
        str(row.Nickname),
        str(row.Descricao),
        row.Criacao_Canal,
        str(row.Categoria),
        float(row.Viewers) if pd.notnull(row.Viewers) else None,
        str(row.Linguagem),
        row.Data_stream,
        row.Hora_stream
        )
    except Exception as e:
        print(f"Erro ao inserir linha {index} (ID: {row.ID}): {e}")


conn.commit()
cursor.close()
conn.close()

print("Dados inseridos com sucesso no SQL Server.")
