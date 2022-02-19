import os, sys, h2o
import pandas as pd
import numpy as np 
import joblib

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids, load_teams_data, load_nba_live_data
from tqdm import tqdm

def get_teams_sma_score():
    teams = list(load_teams_data()["TEAM_NAME"].values)
    game_ids = load_game_ids()
    mean_final_score = 0
    teams_sma_score = pd.DataFrame()

    for team in teams:
        team_game_ids = pd.concat([game_ids[game_ids["HOME_TEAM_NAME"] == team], game_ids[game_ids["AWAY_TEAM_NAME"] == team]])
        for row in list(team_game_ids.index)[:1]:
            home_team, away_team = game_ids.at[row, "HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
            date = game_ids.at[row, "GAME_DATE"]
            live_data = load_nba_live_data(game_ids, home_team, away_team, date)
            score_column = "SCORE_HOME"  if home_team == team else "SCORE_AWAY"
            mean_final_score = float(live_data.iloc[-1][score_column])
        mean_final_score /= len(game_ids)
        teams_sma_score[team] = mean_final_score
    return teams_sma_score


def data_process_pipeline(data):

    # data["Y"] = data["TOTAL_SCORE"].shift(-50)
    # data["Y"] = data["TOTAL_SCORE"].iloc[-1]
    data["Y"] = data["TOTAL_SCORE"].shift(-50)
    data = data[data["PERIOD"] == 4]
    data = data[data["CLOCK"] < "PT10M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["POSSESSION", "REBOUND_DEFENSIVE", "REBOUND_OFFENSIVE",
                       "SHOT_DISTANCE", "ASSIST_TOTAL",
                        "TURNOVER_TOTAL", "REBOUND_TOTAL", 
                        "X_LEGACY", "Y_LEGACY","TOTAL_SCORE", "Y"]]
    data = data.dropna()
    return data

def data_process_4qp_pipeline(data):

    data["Y"] = data.iloc[-1]["TOTAL_SCORE"]
    data = data[data["PERIOD"] == 4]
    data = data[data["CLOCK"] < "PT7M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE", "Y"]]

    data = data.dropna()
    return data

def data_process_team_pipeline(data, team):
    if team == "home":
        team = "SCORE_HOME"
    else:
        team = "SCORE_AWAY"

    data["Y"] = data[team].shift(-50)
    data = data[data["PERIOD"] == 3]
    data = data[data["CLOCK"] < "PT10M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["POSSESSION", "REBOUND_DEFENSIVE", "REBOUND_OFFENSIVE",
                       "SHOT_DISTANCE", "ASSIST_TOTAL",
                        "TURNOVER_TOTAL", "REBOUND_TOTAL", 
                        "X_LEGACY", "Y_LEGACY","SCORE_HOME", "SCORE_AWAY", "Y"]]
    data = data.dropna()
    return data

def get_test_samples(game_ids, team=None):
    data = pd.DataFrame()
    if team == "home":
        team = "SCORE_HOME"
    elif team == "away":
        team = "SCORE_AWAY"

    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        if team:
            live_data = data_process_team_pipeline(live_data, team)
        else:
            live_data = data_process_pipeline(live_data)

        live_data["HOME_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["HOME_TEAM_NAME"].iloc[0]
        live_data["AWAY_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["AWAY_TEAM_NAME"].iloc[0]
        
        data = pd.concat([data, live_data], ignore_index=True)
    data = h2o.H2OFrame(data)
    data["HOME_TEAM_NAME"] = data["HOME_TEAM_NAME"].asfactor()
    data["AWAY_TEAM_NAME"] = data["AWAY_TEAM_NAME"].asfactor()
    cols = list(data.columns)
    cols.remove("Y")
    cols.append("Y")
    data = data[cols]
    return data

def get_4qp_test_samples(game_ids):
    data = pd.DataFrame()
    teams_sma_score = get_teams_sma_score()
    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        live_data = data_process_4qp_pipeline(live_data)
        live_data["HOME_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["HOME_TEAM_NAME"].iloc[0]
        live_data["AWAY_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["AWAY_TEAM_NAME"].iloc[0]
        print(teams_sma_score[home_team])
        print(teams_sma_score.columns)
        live_data["GAME_RESULT_ESTIMATE"] = teams_sma_score[home_team] + teams_sma_score[away_team] 
        data = pd.concat([data, live_data], ignore_index=True)
    data = h2o.H2OFrame(data)
    data["HOME_TEAM_NAME"] = data["HOME_TEAM_NAME"].asfactor()
    data["AWAY_TEAM_NAME"] = data["AWAY_TEAM_NAME"].asfactor()
    cols = list(data.columns)
    cols.remove("Y")
    cols.append("Y")
    data = data[cols]
    return data

def fit_data(model, data):
    x = data.columns[:-1]
    y = data.columns[-1]
    model.train(x=x,y=y, training_frame=data)
    return model

def save_model(model, save_path, mode=None):
    if mode == "h20":
        h2o.save_model(model=model, path=save_path, force=True)
    else:
        joblib.dump(model, save_path)

def load_model(model_path, mode=None):
    if mode == "h20":
        model = h2o.load_model(model_path)
    else:
        model = joblib.load(model_path)
    return model

model_paths = {
    "3QPModel" : os.environ["SP_MODELS_PATH"] + "3QPModel/3QPModel",
    "4QPModel" : os.environ["SP_MODELS_PATH"] + "4QPModel/4QPModel",
    "HomeTeamModel" : os.environ["SP_MODELS_PATH"] + "HomeTeamModel/HomeTeamModel",
    "AwayTeamModel" : os.environ["SP_MODELS_PATH"] + "AwayTeamModel/AwayTeamModel",
    "BLPModel" : os.environ["SP_MODELS_PATH"] + "BMPModel/BMPModel.sav",
}