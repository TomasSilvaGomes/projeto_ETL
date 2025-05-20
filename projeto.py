import os
import time
import pandas as pd
import requests

client_id = "1y5q7nbsxvueh95rqckyzhtsc965ab"
client_secret = "72qddjppywnawakxf72s8ri5dgbyl4"
csv_path = 'top_streamers_tratado.csv'

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

def main_loop():
    existing_ids = set()
    if os.path.exists(csv_path):
        try:
            existing_ids = set(pd.read_csv(csv_path)['id'].astype(str))
        except Exception as e:
            print(f"Erro ao carregar CSV existente: {e}")

    access_token = get_app_access_token()
    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {access_token}'
    }

    while True:
        try:
            print("Verificando novos streamers...")
            streamers_data = fetch_top_streams(headers)
            user_ids = [stream['user_id'] for stream in streamers_data if stream['user_id'] not in existing_ids]

            if not user_ids:
                print("Nenhum streamer novo.")
                time.sleep(300)
                continue

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

            if new_entries:
                df_new = pd.DataFrame(new_entries)
                if os.path.exists(csv_path):
                    df_existing = pd.read_csv(csv_path)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_combined = df_new

                df_combined.to_csv(csv_path, index=False)
                print(f"{len(new_entries)} novos streamers adicionados.")
            else:
                print("Nenhum novo usuário com dados disponíveis.")
        except Exception as e:
            print(f"Erro durante execução: {e}")

        time.sleep(300)  # Espera 5 minutos

if __name__ == "__main__":
    main_loop()
