import os 
import pandas as pd
from datetime import datetime
import streamlit as st
from sqlalchemy import create_engine

database_path = os.environ["SP_DATA_WAREHOUSE_PATH"] + "databases/"

db_paths = {
    "teams_db" : f"sqlite:///{database_path}teams.db",
    "games_db" : f"sqlite:///{database_path}game_ids.db",
    "nba_live_db" : f"sqlite:///{database_path}nba_live.db",
    "logs_db" : f"sqlite:///{database_path}logs.db"
}

play_by_play_url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_"


queries = {
    "teams_data" : """SELECT * FROM TEAMS
                     """,
                     
    "game_ids" : """SELECT * FROM GAME_IDS
                     """,

    "nba_live" : """SELECT * FROM NBA_LIVE
                     """,
    "logs" : """SELECT * FROM LOGS
                     """,
}

def get_id(game_ids, home_team, away_team, date):
    target_game = game_ids[game_ids["GAME_DATE"] == date]
    target_game = target_game[target_game["HOME_TEAM_NAME"] == home_team]
    target_game = target_game[target_game["AWAY_TEAM_NAME"] == away_team]
    try:
        id = target_game["GAME_ID"].iloc[0]
    except:
        st.info(f"{home_team} vs {away_team} at {date} is not a valid game")
        id = None
    
    return id

def load_logs():
    db_name = db_paths["logs_db"]
    engine = create_engine(db_name)
    query = queries["logs"]
    logs = pd.read_sql(query, engine)
    return logs

def load_game_ids():
    db_name = db_paths["games_db"]
    engine = create_engine(db_name)
    query = queries["game_ids"]
    game_ids = pd.read_sql(query, engine, parse_dates=["GAME_DATE"])
    game_ids["GAME_DATE"] = game_ids["GAME_DATE"].apply(lambda x: x.to_pydatetime().date())
    return game_ids

def load_teams_data():
    db_name = db_paths["teams_db"]
    engine = create_engine(db_name)
    query = queries["teams_data"]
    teams_data = pd.read_sql(query, engine)
    return teams_data

def load_nba_live_data(game_ids, home_team, away_team, date):
    id = get_id(game_ids, home_team, away_team, date)
    nba_live_data = pd.DataFrame()
    if id:
        db_name = db_paths["nba_live_db"]
        engine = create_engine(db_name)
        nba_live_data = pd.read_sql(id, engine, parse_dates=["TIME_ACTUAL"])
        nba_live_data["TIME_ACTUAL"] = nba_live_data["TIME_ACTUAL"].apply(lambda x: x.to_pydatetime())
        nba_live_data = nba_live_data.sort_values(by="TIME_ACTUAL")
        nba_live_data["TOTAL_SCORE"] = (nba_live_data["SCORE_HOME"].astype(float) + nba_live_data["SCORE_AWAY"].astype(float))
        nba_live_data["SPREAD"] = (nba_live_data["SCORE_HOME"].astype(float) - nba_live_data["SCORE_AWAY"].astype(float))
        columns = ["TIME_ACTUAL", "PERIOD", "SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE", "SPREAD"]
        curr_columns = list(nba_live_data.columns)
        curr_columns = list(filter(lambda d: d not in columns, curr_columns))
        columns += curr_columns
        nba_live_data = nba_live_data[columns]
    return nba_live_data


