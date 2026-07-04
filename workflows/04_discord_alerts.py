# %%
from sqlalchemy import create_engine
import pandas as pd
import requests, os
from time import sleep
from prefect import task, flow
from prefect.variables import Variable
from dotenv import load_dotenv


@task(name="get_engine", retries=2, retry_delay_seconds=2)
def get_engine():
    engine = create_engine('sqlite:///games_on_sales.db', echo=False)
    return engine

@task(name="create_dataframe", retries=2, retry_delay_seconds=2)
def instantiate_df():
    """Return an empty instance of a DataFrame"""
    df = pd.DataFrame()
    return df

@task(name="database_builder", retries=2, retry_delay_seconds=30)
def db_build(df_size: int):
    """Builds or updates a Sqlite3 database with the information fetched from the API
    The Data is ordered by: 'savings', 'normalPrice', 'salePrice', 'steamRatingPercent'

    max: df_size is 60 cause it's the API limit for each page.
    """
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

    df = instantiate_df()
    if status_code == 200:
        try:
            df = pd.DataFrame(content)
        except Exception as e:
            print(f"[ERRO]: {e}")

    elif status_code != 200:
        try:
            df.to_sql('games_saves', con=engine, if_exists='replace', index=False)
        except Exception as e:
            print(f"[ERRO] ao tentar salvar os dados: {e}")

    else:
        stage = pd.DataFrame(content)
        df = pd.concat([df, stage], ignore_index=True)

    df = df.sort_values(['savings', 'normalPrice', 'salePrice', 'steamRatingPercent'], ignore_index=True)[['title', 'savings', 'normalPrice', 'salePrice', 'steamRatingPercent', 'steamRatingText', 'thumb']]
    df.to_sql('games_saves', con=engine, if_exists='replace', index=False)
    return df.head(n=df_size)

@task(name="messages_creator")
def send_discord_message(top_n_games: int):
    """Max top_n_games is 60"""
    df = db_build(60)

    games_offers = f"# TOP {top_n_games} GAMES EM PROMOÇÃO AGORA:\n\n"

    for i in range(top_n_games):
        j = df.iloc[i].to_list()
        if i == 0:
            games_offers += f"> O {i+1}° game: {j[0]} está com {float(j[1]):.2f}% de desconto.\n- De ${float(j[2]):.2f} por apenas {float(j[3]):.2f}. Com {float(j[4]):.2f}% de avaliação da Steam e opiniões: {j[5]}.\n{j[6]}\n\n"
        else:
            games_offers = f"> O {i+1}° game: {j[0]} está com {float(j[1]):.2f}% de desconto.\n- De ${float(j[2]):.2f} por apenas {float(j[3]):.2f}. Com {float(j[4]):.2f}% de avaliação da Steam e opiniões: {j[5]}.\n{j[6]}\n\n"

        #print(f"Tamanho do texto para o content{len(games_offers)}")
        # url=f"{os.getenv("DISCORD_WEBHOOK")}"
        url = f"{Variable.get("discord_webhook")}"

        payload = {'content': games_offers} # WEBHOOK Não permite mais de 2000 caracteres no payload

        requests.post(url, json=payload)
        #print(f"Status Code das requisições: {response.status_code}\nConteúdo das requisições: {response.content}")
        sleep(1)

@flow(name="send_game_offers", retries=2, retry_delay_seconds=10)
def main():
    send_discord_message(top_n_games=60)

if __name__=="__main__":
    main.deploy(
        name="pipeline",
        work_pool_name="standard_local_dev_work_pool",
        image="alpine",
        tags=["pipeline", "hssf", "discord"],
        cron="30 0/6 * * *",
        concurrency_limit=1
    )


# %% TEST ZONE

'''
from sqlalchemy import create_engine
import pandas as pd
import requests, os
from prefect import task, flow

engine = create_engine('sqlite:///games_on_sales.db', echo=False)

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
'''

