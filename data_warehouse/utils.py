import os 
database_path = os.environ["SP_DATA_WAREHOUSE_PATH"] + "databases/"

db_paths = {
    "teams_db" : f"sqlite:///{database_path}teams.db",
    "games_db" : f"sqlite:///{database_path}game_ids.db",
    "nba_live_db" : f"sqlite:///{database_path}nba_live.db",
}

play_by_play_url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_"

headers  = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

queries = {
    "teams_data" : """SELECT * FROM TEAMS
                     """,
                     
    "game_ids" : """SELECT * FROM GAME_IDS
                     """,

    "nba_live" : """SELECT * FROM NBA_LIVE
                     """,
}

def load_game_ids():
    pass

def load_team_ids():
    pass

def nba_live_data():
    pass

