import pandas as pd


df = pd.read_csv('top_streamers.csv')

if df['login'].isnull().any():
    # elimina a linha
    df = df.dropna(subset=['login'])

df.to_csv('top_streamers_tratado.csv', index=False)


df2 = pd.read_csv('top_streamers_tratado.csv')

df2 = df2.drop(columns=['user_login', 'viewer_count','game_name'])

df2.to_csv('top_streamers_tratado.csv', index=False)


# if stream category is null replace with the correspondent value in the column game_name

df2['stream_category'] = df2['stream_category'].fillna(df2['game_name'])

df2.to_csv('top_streamers_tratado.csv', index=False)