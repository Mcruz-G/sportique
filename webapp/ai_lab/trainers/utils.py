import os, sys, h2o
import pandas as pd

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids, load_teams_data, load_nba_live_data
from tqdm import tqdm

def data_process_pipeline(data):
    data = data[["SPREAD", "POSSESSION", "REBOUND_DEFENSIVE", "REBOUND_OFFENSIVE",
                     "SHOT_DISTANCE", "ASSIST_TOTAL",
                        "TURNOVER_TOTAL", "REBOUND_TOTAL", 
                        "X_LEGACY", "Y_LEGACY","TOTAL_SCORE"]]

    # data["Y"] = data["TOTAL_SCORE"].shift(-50)
    # data["Y"] = data["TOTAL_SCORE"].iloc[-1]
    data["Y"] = data["TOTAL_SCORE"].shift(-75)
    data = data.dropna()
    return data

def data_process_live_pipeline(data):
    # data = data[data["PERIOD"] == 4]
    # data = data[data["CLOCK"] < "PT10M00.00S"]
    data = data[data["CLOCK"] > "PT05M00.00S"]
    data = data[["SPREAD", "POSSESSION", "REBOUND_DEFENSIVE", "REBOUND_OFFENSIVE",
                     "SHOT_DISTANCE", "ASSIST_TOTAL",
                        "TURNOVER_TOTAL", "REBOUND_TOTAL", 
                        "X_LEGACY", "Y_LEGACY","TOTAL_SCORE"]]
    data["Y"] = data["TOTAL_SCORE"]
    return data

def get_data(game_ids, live=False):
    data = pd.DataFrame()
    for row in tqdm(list(game_ids.index)):
        home_team, away_team = game_ids.at[row,"HOME_TEAM_NAME"], game_ids.at[row, "AWAY_TEAM_NAME"]
        date = game_ids.at[row, "GAME_DATE"]
        id = game_ids.at[row, "GAME_ID"]
        live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        
        if live:
            live_data = data_process_live_pipeline(live_data)
        else:
            live_data = data_process_pipeline(live_data)
        live_data["HOME_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["HOME_TEAM_NAME"].iloc[0]
        live_data["AWAY_TEAM_NAME"] = game_ids[game_ids["GAME_ID"]==id]["AWAY_TEAM_NAME"].iloc[0]
        
        data = pd.concat([data, live_data], ignore_index=True)
    data = h2o.H2OFrame(data)
    data["REBOUND_DEFENSIVE"] = data["REBOUND_DEFENSIVE"].asfactor()
    data["REBOUND_OFFENSIVE"] = data["REBOUND_OFFENSIVE"].asfactor()
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
    "3QPModel" : os.environ["SP_MODELS_PATH"] + "3QPModel/3QPModel"
}