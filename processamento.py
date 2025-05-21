import pandas as pd
from datetime import datetime

# Carregar CSV
df = pd.read_csv('streamers.csv')

# Remover duplicados mantendo o primeiro
df = df.drop_duplicates(subset='login', keep='first')

# Remover linhas com valores nulos
df = df.dropna()

# Converter coluna created_at para datetime e formatar
df['created_at'] = pd.to_datetime(df['created_at'], format="%Y-%m-%dT%H:%M:%SZ")
df['created_at'] = df['created_at'].dt.strftime("%d/%m/%Y")

# Converter viewers para int (garanta que todos os valores são números)
df['viewers'] = pd.to_numeric(df['viewers'], errors='coerce').fillna(0).astype(int)

# Dicionário de códigos para idioma e mapeamento
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

# Funções para extrair data e hora
def extrair_data(data_iso):
    dt = datetime.strptime(data_iso, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%d/%m/%Y")

def extrair_hora(data_iso):
    dt = datetime.strptime(data_iso, "%Y-%m-%dT%H:%M:%SZ")
    return dt.strftime("%H:%M:%S")

df["Data_stream"] = df["stream_started_at"].apply(extrair_data)
df["Hora_stream"] = df["stream_started_at"].apply(extrair_hora)

df = df.drop("stream_started_at", axis=1)

# Renomear colunas
if all(col in df.columns for col in ['id', 'login', 'display_name', 'description', 'created_at', 'stream_category', 'viewers']):
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

# Salvar CSV atualizado
df.to_csv("streamers_twitch.csv", index=False)
