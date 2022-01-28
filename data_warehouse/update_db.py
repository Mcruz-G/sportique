from datetime import date
from hashlib import new
import requests 
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from contextlib import ExitStack
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
from sqlalchemy import create_engine, inspect
from .data_warehouse_utils import db_paths, play_by_play_url, queries, load_nba_live_data, load_teams_data, load_game_ids

def get_games(season, league_id, season_type):
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season, league_id_nullable=league_id, season_type_nullable=season_type)
    games = gamefinder.get_data_frames()[0]

    return games

def get_teams_data(games):
    teams_data = games[["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"]].drop_duplicates()

    return teams_data

def get_game_ids(games, db_engine):
    today = datetime.now().date()
    today = datetime.strftime(today, "%Y-%m-%d")
    
    incoming_game_ids = sorted(list(set(list(games["GAME_ID"].values))))
    most_recent_games = sorted(list(set(list(games[games["GAME_DATE"] >= today ]["GAME_ID"].values))))
    try:
        stored_game_ids_data = pd.read_sql(queries["game_ids"], db_engine)
        stored_game_ids = list(stored_game_ids_data["GAME_ID"].values)
    except:
        stored_game_ids_data = pd.DataFrame()
        stored_game_ids = []
    new_game_ids = list(filter(lambda d: d in most_recent_games or d not in stored_game_ids, incoming_game_ids))
    new_game_ids = list(set(new_game_ids))
    game_ids = pd.DataFrame()
    game_ids["GAME_ID"] = new_game_ids
    game_ids["GAME_DATE"] = ""
    game_ids["HOME_TEAM_NAME"] = ""
    game_ids["AWAY_TEAM_NAME"] = ""
    if len(new_game_ids) > 0:
        for idx, id in enumerate(new_game_ids):
            home_team_name = games[games["GAME_ID"] == id]["TEAM_NAME"].values[0]
            away_team_name = games[games["GAME_ID"] == id]["TEAM_NAME"].values[1]
            date = games[games["GAME_ID"] == id]["GAME_DATE"].values[0]
            game_ids["GAME_ID"][idx] = id
            game_ids["GAME_DATE"][idx] = date
            game_ids["HOME_TEAM_NAME"][idx] = home_team_name
            game_ids["AWAY_TEAM_NAME"][idx] = away_team_name
            print(f"Loading new game: {home_team_name} vs. {away_team_name}; GAME ID={id}")
    
    return game_ids

def update_teams_db(season, league_id, season_type):
    games = get_games(season, league_id, season_type)
    db_name = db_paths["teams_db"]
    db_engine = create_engine(db_name)
    teams_data = get_teams_data(games)
    teams_data.to_sql("TEAMS", db_engine, index=False, if_exists="append")


def update_games_db(season, league_id, season_type):
    games = get_games(season, league_id, season_type)
    db_name = db_paths["games_db"]
    db_engine = create_engine(db_name)
    game_ids = get_game_ids(games, db_engine)
    try:
        stored_game_ids = load_game_ids()
        stored_game_ids = list(stored_game_ids["GAME_ID"].values)
        stored_game_ids = sorted(list(set(stored_game_ids)))

        current_game_ids = list(game_ids["GAME_ID"].values)
        current_game_ids = sorted(list(set(current_game_ids)))
        if stored_game_ids == current_game_ids:
            return
    except:
        pass
    game_ids.to_sql("GAME_IDS", db_engine, index=False, if_exists="append")

def update_nba_live_db():
    call_date = datetime.now().date()    
    stored_game_ids = load_game_ids()
    game_ids = list(set(list(stored_game_ids["GAME_ID"].values)))
    most_recent_games = list(stored_game_ids[stored_game_ids["GAME_DATE"] == call_date]["GAME_ID"].values)
    db_name = db_paths["nba_live_db"]
    db_engine = create_engine(db_name)
    current_game_ids = inspect(db_engine).get_table_names()
    game_ids = list(filter(lambda d: d in most_recent_games or d not in current_game_ids, game_ids))
    game_ids = list(set(game_ids))
    print("Updating NBA LIVE Database")
    log_db_name = db_paths["logs_db"]
    log_db_engine = create_engine(log_db_name)
    logs = pd.DataFrame(columns = ["CALL_DATE", "REQUESTED_ID", "HOME_TEAM", "AWAY_TEAM", "GAME_DATE"])
    logs["REQUESTED_ID"] = game_ids
    idx = 0
    for id in tqdm(game_ids):
        url = play_by_play_url + str(id) + ".json"
        response = requests.get(url=url).json()
        play_by_play_data = pd.DataFrame(response["game"]["actions"])
        nba_live_data = pd.DataFrame()
        nba_live_data["POSSESSION"] = play_by_play_data["possession"]
        nba_live_data["REBOUND_DEFENSIVE"] = play_by_play_data["reboundDefensiveTotal"]
        nba_live_data["REBOUND_OFFENSIVE"] = play_by_play_data["reboundDefensiveTotal"]
        nba_live_data["SHOT_RESULT"] = play_by_play_data["shotResult"]
        nba_live_data["SCORE_HOME"] = play_by_play_data["scoreHome"]
        nba_live_data["SCORE_AWAY"] = play_by_play_data["scoreAway"]
        nba_live_data["PERIOD"] = play_by_play_data["period"]
        nba_live_data["TIME_ACTUAL"] = play_by_play_data["timeActual"]
        nba_live_data["CLOCK"] = play_by_play_data["clock"]
        nba_live_data["PLAYER_NAME"] = play_by_play_data["playerName"] 
        nba_live_data["PLAYER_NAME_2"] = play_by_play_data["playerNameI"] 
        nba_live_data["IS_FIELD_GOAL"] = play_by_play_data["isFieldGoal"]
        nba_live_data["SHOT_DISTANCE"] = play_by_play_data["shotDistance"].fillna(method="ffill")
        nba_live_data["ASSIST_TOTAL"] = play_by_play_data["assistTotal"].fillna(method="ffill")
        nba_live_data["TURNOVER_TOTAL"] = play_by_play_data["turnoverTotal"].fillna(method="ffill")
        nba_live_data["POINTS_TOTAL"] = play_by_play_data["pointsTotal"].fillna(method="ffill")
        nba_live_data["REBOUND_TOTAL"] = play_by_play_data["reboundTotal"].fillna(method="ffill")
        nba_live_data["X_LEGACY"] = play_by_play_data["xLegacy"].fillna(method="ffill")
        nba_live_data["Y_LEGACY"] = play_by_play_data["yLegacy"].fillna(method="ffill")
        
        if id in current_game_ids:
            sql = f"DROP TABLE IF EXISTS '{id}'"
            db_engine.execute(sql)

        nba_live_data.to_sql(name=id, con=db_engine, index=False, if_exists="append")

        logs["CALL_DATE"][idx] = call_date
        logs["REQUESTED_ID"][idx] = id
        logs["HOME_TEAM"][idx] = stored_game_ids[stored_game_ids["GAME_ID"] == id]["HOME_TEAM_NAME"].iloc[0]
        logs["AWAY_TEAM"][idx] = stored_game_ids[stored_game_ids["GAME_ID"] == id]["AWAY_TEAM_NAME"].iloc[0]
        logs["GAME_DATE"][idx] = stored_game_ids[stored_game_ids["GAME_ID"] == id]["GAME_DATE"].iloc[0]
        logs.to_sql(name="LOGS", con=log_db_engine, index=False, if_exists="append")
        idx += 1

def update_db():
    season = "2021-22"
    league_id = "00"
    season_type = "Regular Season"
    # update_teams_db(season, league_id, season_type)
    update_games_db(season, league_id, season_type)
    update_nba_live_db()

if __name__ == "__main__":
    update_db()