import os
import pandas as pd
import requests
import pyodbc
from datetime import datetime
import time

# Credenciais e caminhos
client_id = "1y5q7nbsxvueh95rqckyzhtsc965ab"
client_secret = "72qddjppywnawakxf72s8ri5dgbyl4"
csv_raw = 'streamers.csv'
csv_clean = 'streamers_twitch.csv'

# SQL Server config
server = 'localhost'
database = 'twitch_data'
username = 'sa'
password = 'sa'
table_name = 'streamers'

def get_app_access_token():
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json().get('access_token', '')

def fetch_top_streams(headers):
    url = 'https://api.twitch.tv/helix/streams?first=100'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('data', [])

def fetch_users_info(user_ids, headers):
    users = []
    for i in range(0, len(user_ids), 100):
        batch = user_ids[i:i+100]
        url = 'https://api.twitch.tv/helix/users?' + '&'.join(f'id={uid}' for uid in batch)
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        users.extend(resp.json().get('data', []))
    return users

def coletar_dados_twitch():
    print("🟣 Iniciando coleta de dados da Twitch...")

    existing_ids = set()
    if os.path.exists(csv_raw):
        try:
            existing_ids = set(pd.read_csv(csv_raw)['id'].astype(str))
        except Exception as e:
            print(f"Erro ao carregar CSV existente: {e}")

    access_token = get_app_access_token()
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}'
    }

    streamers_data = fetch_top_streams(headers)
    user_ids = [stream['user_id'] for stream in streamers_data if stream['user_id'] not in existing_ids]
    if not user_ids:
        print("Nenhum streamer novo encontrado.")
        return

    users_info = fetch_users_info(user_ids, headers)

    new_entries = []
    for user in users_info:
        stream_info = next((s for s in streamers_data if s['user_id'] == user['id']), {})
        new_entries.append({
            'id': user['id'],
            'login': user['login'],
            'display_name': user['display_name'],
            'description': user.get('description', ''),
            'created_at': user.get('created_at', ''),
            'stream_category': stream_info.get('game_name', 'Offline'),
            'viewers': stream_info.get('viewer_count', 0),
            'language': stream_info.get('language', 'unknown'),
            'stream_started_at': stream_info.get('started_at', '')
        })
        existing_ids.add(user['id'])

    df_new = pd.DataFrame(new_entries)

    if os.path.exists(csv_raw):
        df_existing = pd.read_csv(csv_raw)
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_combined = df_new

    df_combined.to_csv(csv_raw, index=False)
    print(f"✅ {len(new_entries)} novos streamers adicionados ao CSV bruto.")

    limpar_e_formatar_csv()

def limpar_e_formatar_csv():
    print("🧹 Limpando e formatando dados...")

    df = pd.read_csv(csv_raw)

    df = df.drop_duplicates(subset='login', keep='first')
    df = df.dropna()

    df['created_at'] = pd.to_datetime(df['created_at'], format="%Y-%m-%dT%H:%M:%SZ")
    df['created_at'] = df['created_at'].dt.strftime("%d/%m/%Y")

    df['viewers'] = pd.to_numeric(df['viewers'], errors='coerce').fillna(0).astype(int)

    # Mapeamento de idiomas
    codigo_para_idioma = {
        'af': 'Africâner', 'sq': 'Albanês', 'am': 'Amárico', 'ar': 'Árabe',
        'hy': 'Armênio', 'az': 'Azerbaijano', 'eu': 'Basco', 'be': 'Bielorrusso',
        'bn': 'Bengali', 'bs': 'Bósnio', 'bg': 'Búlgaro', 'ca': 'Catalão',
        'ceb': 'Cebuano', 'zh': 'Chinês', 'zh-CN': 'Chinês (Simplificado)',
        'zh-TW': 'Chinês (Tradicional)', 'co': 'Corso', 'hr': 'Croata',
        'cs': 'Checo', 'da': 'Dinamarquês', 'nl': 'Holandês', 'en': 'Inglês',
        'eo': 'Esperanto', 'et': 'Estoniano', 'fi': 'Finlandês', 'fr': 'Francês',
        'fy': 'Frísio', 'gl': 'Galego', 'ka': 'Georgiano', 'de': 'Alemão',
        'el': 'Grego', 'gu': 'Guzerate', 'ht': 'Crioulo haitiano', 'ha': 'Hauçá',
        'haw': 'Havaiano', 'he': 'Hebraico', 'hi': 'Hindi', 'hmn': 'Hmong',
        'hu': 'Húngaro', 'is': 'Islandês', 'ig': 'Ibo', 'id': 'Indonésio',
        'ga': 'Irlandês', 'it': 'Italiano', 'ja': 'Japonês', 'jw': 'Javanês',
        'kn': 'Canarês', 'kk': 'Cazaque', 'km': 'Khmer', 'ko': 'Coreano',
        'ku': 'Curdo', 'ky': 'Quirguiz', 'lo': 'Lao', 'la': 'Latim',
        'lv': 'Letão', 'lt': 'Lituano', 'lb': 'Luxemburguês', 'mk': 'Macedônio',
        'mg': 'Malgaxe', 'ms': 'Malaio', 'ml': 'Malaiala', 'mt': 'Maltês',
        'mi': 'Maori', 'mr': 'Marata', 'mn': 'Mongol', 'my': 'Birmanês',
        'ne': 'Nepalês', 'no': 'Norueguês', 'ny': 'Nianja', 'or': 'Oriá',
        'ps': 'Pashto', 'fa': 'Persa', 'pl': 'Polonês', 'pt': 'Português',
        'pa': 'Punjabi', 'ro': 'Romeno', 'ru': 'Russo', 'sm': 'Samoano',
        'gd': 'Gaélico Escocês', 'sr': 'Sérvio', 'st': 'Sesoto', 'sn': 'Shona',
        'sd': 'Sindi', 'si': 'Cingalês', 'sk': 'Eslovaco', 'sl': 'Esloveno',
        'so': 'Somali', 'es': 'Espanhol', 'su': 'Sundanês', 'sw': 'Suaíli',
        'sv': 'Sueco', 'tl': 'Tagalo', 'tg': 'Tajique', 'ta': 'Tâmil',
        'tt': 'Tártaro', 'te': 'Télugo', 'th': 'Tailandês', 'tr': 'Turco',
        'tk': 'Turcomeno', 'uk': 'Ucraniano', 'ur': 'Urdu', 'ug': 'Uigur',
        'uz': 'Uzbeque', 'vi': 'Vietnamita', 'cy': 'Galês', 'xh': 'Xhosa',
        'yi': 'Iídiche', 'yo': 'Iorubá', 'zu': 'Zulu'
    }
    df['language'] = df['language'].map(codigo_para_idioma)

    def extrair_data(data_iso):
        dt = datetime.strptime(data_iso, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%d/%m/%Y")

    def extrair_hora(data_iso):
        dt = datetime.strptime(data_iso, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%H:%M:%S")

    df["Data_stream"] = df["stream_started_at"].apply(extrair_data)
    df["Hora_stream"] = df["stream_started_at"].apply(extrair_hora)
    df = df.drop("stream_started_at", axis=1)

    df = df.rename(columns={
        'id': 'ID',
        'login': 'Nome',
        'display_name': 'Nickname',
        'description': 'Descrição',
        'created_at': 'Criação_Canal',
        'stream_category': 'Categoria',
        'viewers': 'Viewers',
        'language': 'Linguagem'
    })

    df.to_csv(csv_clean, index=False)
    print("🧼 Dados salvos no CSV limpo.")

def inserir_dados_sql():
    print("🟢 Iniciando inserção no SQL Server...")
    df = pd.read_csv(csv_clean)

    df['Criação_Canal'] = pd.to_datetime(df['Criação_Canal'], errors='coerce', dayfirst=True)
    df['Data_stream'] = pd.to_datetime(df['Data_stream'], errors='coerce', dayfirst=True).dt.date
    df['Hora_stream'] = pd.to_datetime(df['Hora_stream'], format='%H:%M:%S', errors='coerce').dt.time
    df['Viewers'] = pd.to_numeric(df['Viewers'], errors='coerce')

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    cursor.execute(f"""
    IF OBJECT_ID('{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name} (
            ID BIGINT PRIMARY KEY,
            Nome NVARCHAR(255),
            Nickname NVARCHAR(255),
            Descrição NVARCHAR(MAX),
            Criação_Canal DATETIME,
            Categoria NVARCHAR(255),
            Viewers FLOAT,
            Linguagem NVARCHAR(50),
            Data_stream DATE,
            Hora_stream TIME
        )
    END
    """)
    conn.commit()

    for index, row in df.iterrows():
        try:
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM {table_name} WHERE ID = ?)
                INSERT INTO {table_name} 
                (ID, Nome, Nickname, Descrição, Criação_Canal, Categoria, Viewers, Linguagem, Data_stream, Hora_stream)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            int(row.ID),
            row.ID,
            row.Nome,
            row.Nickname,
            row.Descrição,
            row.Criação_Canal,
            row.Categoria,
            row.Viewers,
            row.Linguagem,
            row.Data_stream,
            row.Hora_stream)
        except Exception as e:
            print(f"Erro ao inserir linha {index} (ID: {row.ID}): {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Dados inseridos com sucesso no SQL Server.")


if __name__ == "__main__":
    while True:
        try:
            coletar_dados_twitch()
            limpar_e_formatar_csv()
            inserir_dados_sql()
            print("🔚 Processo concluído com sucesso!")
        except Exception as e:
            print(f"⚠️ Ocorreu um erro durante o processo: {e}")

        print("⏳ Dormindo por 2 dias...")
        time.sleep(2 * 24 * 60 * 60)  # 2 dias em segundos
