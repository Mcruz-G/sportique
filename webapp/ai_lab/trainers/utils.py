import os, sys, h2o
from turtle import home
from platform import libc_ver
import pandas as pd
import numpy as np

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
        for row in list(team_game_ids.index):
            home_team, away_team = game_ids.at[row, "HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
            date = game_ids.at[row, "GAME_DATE"]
            live_data = load_nba_live_data(game_ids, home_team, away_team, date)
            score_column = "SCORE_HOME"  if home_team == team else "SCORE_AWAY"
            mean_final_score = float(live_data.iloc[-1][score_column])
        mean_final_score /= len(game_ids)
        teams_sma_score[team] = mean_final_score
    return teams_sma_score

def data_process_3qp_pipeline(data):
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]
    data["Y"] = data["TOTAL_SCORE"].shift(-50)
    data = data.dropna()
    return data

def data_process_3qp_live_pipeline(data):
    data = data[data["PERIOD"] <= 4]
    data = data[data["CLOCK"] < "PT10M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]
    data["Y"] = data["TOTAL_SCORE"]
    return data

def data_process_4qp_pipeline(data):
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]
    data["Y"] = data.iloc[-1]["TOTAL_SCORE"]
    data["Y"] = data["Y"].apply(lambda x: x + 10*np.random.random())
    data = data.dropna()
    return data

def data_process_4qp_live_pipeline(data):
    # data = data[data["PERIOD"] <= 4]
    # data = data[data["CLOCK"] < "PT10M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]
    data["Y"] = data["Y"].apply(lambda x: x + 10*np.random.random())
    data["Y"] = data["TOTAL_SCORE"]
    return data

def data_process_team_pipeline(data, team):
    if team == "home":
        team = "SCORE_HOME"
    else:
        team = "SCORE_AWAY"

    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]

    data["Y"] = data[team].shift(-50)
    data = data.dropna()
    return data

def data_process_team_live_pipeline(data, team):
    # data = data[data["PERIOD"] <= 4]
    # data = data[data["CLOCK"] < "PT10M00.00S"]
    if team == "home":
        team = "SCORE_HOME"
    else:
        team = "SCORE_AWAY"
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["SCORE_HOME", "SCORE_AWAY", "TOTAL_SCORE"]]

    # ["POSSESSION", 
    #                  "SHOT_DISTANCE", "ASSIST_TOTAL", 
    #                     "TURNOVER_TOTAL", "REBOUND_TOTAL", 
    #                     "X_LEGACY", "Y_LEGACY","SCORE_HOME", "SCORE_AWAY"]
    data["Y"] = data[team]
    return data

def get_team_data(game_ids, team, live=False):
    data = pd.DataFrame()
    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        
        if live:
            live_data = data_process_team_live_pipeline(live_data, team)
        else:
            live_data = data_process_team_pipeline(live_data, team)
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

def get_3qp_data(game_ids, live=False):
    data = pd.DataFrame()
    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        
        if live:
            live_data = data_process_3qp_live_pipeline(live_data)
        else:
            live_data = data_process_3qp_pipeline(live_data)
        live_data["HOME_TEAM_NAME"] = home_team
        live_data["AWAY_TEAM_NAME"] = away_team
        
        data = pd.concat([data, live_data], ignore_index=True)
    data = h2o.H2OFrame(data)
    data["HOME_TEAM_NAME"] = data["HOME_TEAM_NAME"].asfactor()
    data["AWAY_TEAM_NAME"] = data["AWAY_TEAM_NAME"].asfactor()
    cols = list(data.columns)
    cols.remove("Y")
    cols.append("Y")
    data = data[cols]
    return data

def get_4qp_data(game_ids, live=False):
    data = pd.DataFrame()
    teams_sma_score = get_teams_sma_score()

    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        
        if live:
            live_data = data_process_4qp_live_pipeline(live_data)
        else:
            live_data = data_process_4qp_pipeline(live_data)
        live_data["HOME_TEAM_NAME"] = home_team
        live_data["AWAY_TEAM_NAME"] = away_team
        # live_data["HOME_TEAM_MEAN_SCORE"] = teams_sma_score[home_team]
        # live_data["AWAY_TEAM_MEAN_SCORE"] = teams_sma_score[away_team]
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

def save_model(model, save_path):
    h2o.save_model(model=model, path=save_path, force=True)

def load_model(model_path):
    model = h2o.load_model(model_path)
    return model

def log(game_ids, save_path, model_name):
    log = {}
    training_dates = list(game_ids["GAME_DATE"])
    log["MAX_TRAINING_DATE"] = [max(training_dates)]
    log = pd.DataFrame(log)
    log.to_csv(save_path + f"{model_name}_log.csv", index=False)



model_paths = {
    "3QPModel" : os.environ["SP_MODELS_PATH"] + "3QPModel/3QPModel",
    "4QPModel" : os.environ["SP_MODELS_PATH"] + "4QPModel/4QPModel",
    "HomeTeamModel" : os.environ["SP_MODELS_PATH"] + "HomeTeamModel/HomeTeamModel",
    "AwayTeamModel" : os.environ["SP_MODELS_PATH"] + "AwayTeamModel/AwayTeamModel",
}