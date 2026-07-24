# %% TEST ZONE
from sqlalchemy import create_engine
import pandas as pd
import requests, os
from prefect import task, flow

engine = create_engine('sqlite:///games_on_sales.db', echo=False)

def get_engine():
    engine = create_engine('sqlite:///games_on_sales.db', echo=False)
    return engine

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    #'pageNumber': f'{page}'
    }

params = {
    "sortBy": "savings"
}

response = requests.get(
    "https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=15",
    headers=headers,
    params=params
)

status_code = response.status_code
engine = get_engine()
content = response.json()

df = pd.DataFrame(content)

df = df.sort_values(['savings', 'normalPrice', 'salePrice', 'steamRatingPercent'], ignore_index=True)[['title', 'savings', 'normalPrice', 'salePrice', 'steamRatingPercent', 'steamRatingText', 'thumb']]

top_n_games = 7
games_offers = f"# TOP {top_n_games} GAMES EM PROMOÇÃO AGORA:\n\n"

from time import sleep

for i in range(top_n_games):
    j = df.iloc[i].to_list()
    if i == 0:
        games_offers += f"> O {i+1}° game: {j[0]} está com {float(j[1]):.2f}% de desconto.\n- De ${float(j[2]):.2f} por apenas {float(j[3]):.2f}. Com {float(j[4]):.2f}% de avaliação da Steam e opiniões: {j[5]}.\n{j[6]}\n\n"
    else:
        games_offers = f"> O {i+1}° game: {j[0]} está com {float(j[1]):.2f}% de desconto.\n- De ${float(j[2]):.2f} por apenas {float(j[3]):.2f}. Com {float(j[4]):.2f}% de avaliação da Steam e opiniões: {j[5]}.\n{j[6]}\n\n"

    print(f"Tamanho do texto para o content{len(games_offers)}")
    url=f"{os.getenv("DISCORD_WEBHOOK")}"

    payload = {'content': games_offers} # WEBHOOK Não permite mais de 2000 caracteres no payload

    response = requests.post(url, json=payload)
    print(f"Status Code das requisições: {response.status_code}\nConteúdo das requisições: {response.content}")

    sleep(1)
