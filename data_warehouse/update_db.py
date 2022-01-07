from contextlib import ExitStack
import requests 
import pandas as pd
from sqlalchemy import create_engine, inspect
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from utils import db_paths, play_by_play_url, headers, queries
from tqdm import tqdm

def get_games(season, league_id, season_type):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable=league_id, season_type_nullable=season_type)
    games = gamefinder.get_data_frames()[0]
    return games

def get_teams_data(games, db_name, db_engine):
    incoming_team_ids = list(games["TEAM_ID"].unique())
    incoming_team_abbs = list(games["TEAM_ABBREVIATION"].unique())
    incoming_team_names = list(games["TEAM_NAME"].unique())

    try:
        stored_data = pd.read_sql(queries["teams_data"], db_engine)
        stored_team_ids = list(stored_data["TEAM_ID"].values)
        stored_team_abbs = list(stored_data["TEAM_ABBREVIATION"].values)
        stored_team_names = list(stored_data["TEAM_NAME"].values)
    
    except:
        stored_team_ids = []
        stored_team_abbs = []
        stored_team_names = []

    new_team_ids = list(set(incoming_team_ids)-set(stored_team_ids))
    new_team_abbs = list(set(incoming_team_abbs)-set(stored_team_abbs))
    new_team_names = list(set(incoming_team_names)-set(stored_team_names))

    teams_data = pd.DataFrame(columns=["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"])
    teams_data["TEAM_ID"] = new_team_ids
    teams_data["TEAM_ABBREVIATION"] = new_team_abbs
    teams_data["TEAM_NAME"] = new_team_names

    if len(teams_data) > 0:
        print(f"Loading new teams: {new_team_names}")

    return teams_data

def get_game_ids(games, db_name, db_engine):
    incoming_game_ids = list(games["GAME_ID"].values)
    try:
        stored_game_ids_data = pd.read_sql(queries["game_ids"], db_engine)
        stored_game_ids = list(stored_game_ids_data["GAME_ID"].values)
    except:
        stored_game_ids = []
    
    new_game_ids = list(set(incoming_game_ids)-set(stored_game_ids))
    game_ids = pd.DataFrame()
    game_ids["GAME_ID"] = new_game_ids
    game_ids["GAME_DATE"] = 0
    game_ids["HOME_TEAM_NAME"] = ""
    game_ids["AWAY_TEAM_NAME"] = ""
    
    if len(game_ids) > 0:
        for idx in tqdm(games.index):
            id = games["GAME_ID"][idx]
            home_team_name = games[games["GAME_ID"] == id]["TEAM_NAME"].values[0]
            away_team_name = games[games["GAME_ID"] == id]["TEAM_NAME"].values[1]
            game_ids["GAME_DATE"][idx] = games[games["GAME_ID"] == id]["GAME_DATE"].values[0]
            game_ids["HOME_TEAM_NAME"][idx] = home_team_name
            game_ids["AWAY_TEAM_NAME"][idx] = away_team_name
            print(f"Loading new game: {home_team_name} vs. {away_team_name}; GAME ID={id}")
    return game_ids

def update_teams_db(season, league_id, season_type):
    games = get_games(season, league_id, season_type)
    db_name = db_paths["teams_db"]
    db_engine = create_engine(db_name)
    teams_data = get_teams_data(games, db_name, db_engine)
    teams_data.to_sql("TEAMS", db_engine, index=False, if_exists="append")


def update_games_db(season, league_id, season_type):
    games = get_games(season, league_id, season_type)
    db_name = db_paths["games_db"]
    db_engine = create_engine(db_name)
    game_ids = get_game_ids(games, db_name, db_engine)
    game_ids.to_sql("GAME_IDS", db_engine, index=False, if_exists="append")

def update_nba_live_db():
    games_db_engine = create_engine(db_paths["games_db"])
    stored_game_ids_data = pd.read_sql(queries["game_ids"], games_db_engine)
    game_ids = list(stored_game_ids_data["GAME_ID"].values)
    db_name = db_paths["nba_live_db"]
    db_engine = create_engine(db_name)
    current_game_ids = inspect(db_engine).get_table_names()
    game_ids = list(filter(lambda d: d not in current_game_ids, game_ids))
    print("Updating NBA LIVE Database")

    for id in tqdm(game_ids):
        url = play_by_play_url + str(id) + ".json"
        response = requests.get(url=url).json()
        play_by_play_data = pd.DataFrame(response["game"]["actions"])
        nba_live_data = pd.DataFrame()
        nba_live_data["SCORE_HOME"] = play_by_play_data["scoreHome"]
        nba_live_data["SCORE_AWAY"] = play_by_play_data["scoreAway"]
        nba_live_data["PERIOD"] = play_by_play_data["period"]
        nba_live_data["TIME_ACTUAL"] = play_by_play_data["timeActual"]
        nba_live_data["CLOCK"] = play_by_play_data["clock"]
        nba_live_data["PLAYER_NAME"] = play_by_play_data["playerName"] 
        nba_live_data["PLAYER_NAME_2"] = play_by_play_data["playerNameI"] 
        nba_live_data["FOUL_TECHNICAL"] = play_by_play_data["foulTechnicalTotal"]
        nba_live_data["FOUL_PERSONAL"] = play_by_play_data["foulPersonalTotal"]

        nba_live_data.to_sql(name=id, con=db_engine, index=False, if_exists="append")

def update_db():
    season = "2021-22"
    league_id = "00"
    season_type = "Regular Season"
    update_teams_db(season, league_id, season_type)
    update_games_db(season, league_id, season_type)
    update_nba_live_db()

if __name__ == "__main__":
    update_db()