import os
import io
import pandas as pd
import requests
from flask import Flask, session, redirect, url_for, request
from urllib.parse import urlencode
import urllib.parse

app = Flask(__name__)
app.secret_key = os.urandom(24)

client_id = "1y5q7nbsxvueh95rqckyzhtsc965ab"
client_secret = "72qddjppywnawakxf72s8ri5dgbyl4"
redirect_uri = "http://localhost:5000/callback"

@app.route('/')
def home():
    return """
    <h2>Bem-vindo!</h2>
    <ul>
        <li><a href='/login'>Login Twitch</a></li>
        <li><a href='/top_streamers'>Ver Top 100 Streamers ao Vivo</a></li>
    </ul>
    """

@app.route('/login')
def login():
    params = {
        'client_id': client_id,
        'redirect_uri': redirect_uri,
        'response_type': 'code',
        'scope': 'user:read:email user:read:follows',
        'force_verify': 'true'
    }
    url = f"https://id.twitch.tv/oauth2/authorize?{urlencode(params)}"
    return redirect(url)

@app.route('/callback')
def callback():
    code = request.args.get('code')
    if not code:
        return "Erro: código não fornecido pela Twitch", 400

    token_url = 'https://id.twitch.tv/oauth2/token'
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': redirect_uri
    }
    resp = requests.post(token_url, data=data)
    token_info = resp.json()
    session['token_info'] = token_info
    return redirect(url_for('top_streamers'))

@app.route('/top_streamers')
def top_streamers():
    token_info = session.get('token_info')
    if not token_info:
        return redirect('/')

    oauth_token = token_info.get('access_token')
    if not oauth_token:
        return redirect('/')

    headers = {
        'Client-ID': client_id,
        'Authorization': f'Bearer {oauth_token}'
    }

    try:
        url_streams = 'https://api.twitch.tv/helix/streams?first=100'
        response_streams = requests.get(url_streams, headers=headers)
        if response_streams.status_code != 200:
            return "<h3>Erro ao obter dados dos streams.</h3>"

        streams_data = response_streams.json().get('data', [])
        user_ids = [stream['user_id'] for stream in streams_data]

        users = []
        for i in range(0, len(user_ids), 100):
            batch_ids = user_ids[i:i+100]
            url_users = 'https://api.twitch.tv/helix/users?' + '&'.join(f'id={uid}' for uid in batch_ids)
            response_users = requests.get(url_users, headers=headers)
            users.extend(response_users.json().get('data', []))

        streamers = []
        for user in users:
            stream_info = next((s for s in streams_data if s['user_id'] == user['id']), {})
            streamers.append({
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

        df_streamers = pd.DataFrame(streamers)
        table_html = df_streamers.to_html(classes='table table-striped', index=False)

        buf = io.StringIO()
        df_streamers.to_csv(buf, index=False)
        buf.seek(0)
        csv_data = buf.getvalue()

        # Fazendo encoding para colocar no href
        csv_data_encoded = urllib.parse.quote(csv_data)

        return f"""
        <h2>Top 100 Streamers ao Vivo</h2>
        {table_html}
        <br><br>
        <a id="download_link" href="data:text/csv;charset=utf-8,{csv_data_encoded}" download="top_streamers.csv" style="display:none">Baixar CSV</a>
        <script>
            window.onload = function() {{
                document.getElementById('download_link').click();
            }};
        </script>
        """

    except Exception as e:
        print(f"Erro inesperado: {e}")
        return "<h3>Erro inesperado ao processar os dados dos streamers.</h3>"

    except Exception as e:
        print(f"Erro inesperado: {e}")
        return "<h3>Erro inesperado ao processar os dados dos streamers.</h3>"

def get_followers_count(user_id, headers):
    url = f'https://api.twitch.tv/helix/users/follows?to_id={user_id}'
    response = requests.get(url, headers=headers)
    json_data = response.json()
    if 'total' in json_data:
        return json_data['total']
    else:
        print(f"Erro a obter followers para user_id {user_id}: {json_data}")
        return 0

if __name__ == "__main__":
    app.run(debug=True)

