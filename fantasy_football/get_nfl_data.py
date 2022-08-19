from urllib import response
import requests
import pandas as pd
import duckdb
import os
from pathlib import Path

def get_pbp(seasons: list) -> None:
    for season in seasons:
        url = f"https://github.com/nflverse/nflverse-data/releases/download/pbp/play_by_play_{season}.parquet"
        response = requests.get(url)

        with open(f"data/play_by_play/play_by_play_{season}.parquet", 'wb') as f:
            f.write(response.content)
            
def get_weekly_rosters(seasons: list) -> None:
    for season in seasons:
        url = f"https://github.com/nflverse/nflverse-data/releases/download/weekly_rosters/roster_weekly_{season}.parquet"
        response = requests.get(url)

        with open(f"data/weekly_rosters/roster_weekly_{season}.parquet", 'wb') as f:
            f.write(response.content)
            
def get_drafts(seaons: list) -> None:
    url = f"https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.parquet"
    response = requests.get(url)
    
    with open(f"data/draft_picks/draft_picks.parquet", 'wb') as f:
        f.write(response.content) 
            
def get_players() -> None:
    url = f"https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet"
    response = requests.get(url)
    
    with open(f"data/players/players.parquet", 'wb') as f:
        f.write(response.content)
        
def load_duckdb(con, table):
    path = Path('data')
    for file in os.listdir(path/table):
        print(f'loading: {path/table/file}')
        df = pd.read_parquet(path/table/file)
        res = con.execute(f'CREATE TABLE IF NOT EXISTS nflverse.{table} AS SELECT * FROM df')
        if not res.fetchall():
            con.execute(f'INSERT INTO nflverse.{table} SELECT * FROM df')
            
def load_bigquery(table):
    path = Path('data')
    for file in os.listdir(path/table):
        print(f'Loading into BigQuery: {path/table/file}')
        df = pd.read_parquet(path/table/file)
        file_name = file.replace('.parquet','')
        df.to_gbq(f'nflverse.{file_name}', project_id='fantasyvbd', if_exists='replace')
    
        
if __name__ == '__main__':
    seasons = range(1999,2022)
    
    #get_pbp(seasons)
    #get_weekly_rosters(range(2002,2023))
    #get_players()
    #get_drafts(seasons)
    
    #con = duckdb.connect('data/duckdb/fantasy_football.duckdb')
    #con.execute('CREATE SCHEMA IF NOT EXISTS nflverse')
    #load_duckdb(con, 'play_by_play')
    #load_duckdb(con, 'draft_picks')
    #load_duckdb(con, 'players')
    #load_duckdb(con, 'weekly_rosters')
    #con.close()
    
    load_bigquery('play_by_play')
    load_bigquery('draft_picks')
    load_bigquery('players')
    load_bigquery('weekly_rosters')
    