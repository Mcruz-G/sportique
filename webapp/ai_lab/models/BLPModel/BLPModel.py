import os, sys
import numpy as np 
import pandas as pd
from tqdm import tqdm
from sklearn import tree
from datetime import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.tree import DecisionTreeRegressor
import matplotlib.pyplot as plt

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_teams_data, load_nba_live_data, load_game_ids

def add_avgs(blp_dataset):

    home_agg_df = blp_dataset.groupby(["HOME_TEAM_NAME", "GAME_DATE"]).agg({"SCORE_HOME" : np.mean})
    away_agg_df = blp_dataset.groupby(["AWAY_TEAM_NAME", "GAME_DATE"]).agg({"SCORE_AWAY" : np.mean})
    
    home_means = home_agg_df.groupby(["HOME_TEAM_NAME"]).agg({"SCORE_HOME" : np.mean})
    away_means = away_agg_df.groupby(["AWAY_TEAM_NAME"]).agg({"SCORE_AWAY" : np.mean})

    home_means.columns = [''.join(col).strip() for col in home_means.columns.values]
    home_means = home_means.reset_index()
    home_means = home_means.rename(columns = {"HOME_TEAM_NAME" : "TEAM", "SCORE_HOME" : "AVG_SCORE"})

    away_means.columns = [''.join(col).strip() for col in away_means.columns.values]
    away_means = away_means.reset_index()
    away_means = away_means.rename(columns = {"AWAY_TEAM_NAME" : "TEAM","SCORE_AWAY" : "AVG_SCORE"})
    
    blp_dataset["HOME_AVG_SCORE"] = blp_dataset.HOME_TEAM_NAME.apply(lambda x: (home_means[home_means.TEAM == x].AVG_SCORE.iloc[0] + away_means[away_means.TEAM == x].AVG_SCORE.iloc[0]) / 2)
    blp_dataset["AWAY_AVG_SCORE"] = blp_dataset.AWAY_TEAM_NAME.apply(lambda x: (home_means[home_means.TEAM == x].AVG_SCORE.iloc[0] + away_means[away_means.TEAM == x].AVG_SCORE.iloc[0]) / 2)
    blp_dataset["TOTAL_SCORE"] = blp_dataset.SCORE_HOME + blp_dataset.SCORE_AWAY

    return blp_dataset

def add_lines(blp_dataset, lines):
    df = pd.DataFrame()
    for line in lines:
        blp_dataset["LINE"] = line
        df = pd.concat([df, blp_dataset])
    return df

def add_comparing_columns(blp_dataset):
    blp_dataset["TOTAL_SCORE > LINE"] = blp_dataset.TOTAL_SCORE > blp_dataset.LINE
    blp_dataset["TOTAL_SCORE < LINE"] = blp_dataset.TOTAL_SCORE < blp_dataset.LINE
    return blp_dataset

def add_naive_prediction(blp_dataset):
    agg_df = blp_dataset.groupby(["HOME_TEAM_NAME", "AWAY_TEAM_NAME"]).agg({"TOTAL_SCORE > LINE" : sum, "TOTAL_SCORE < LINE" : sum})
    agg_df["NAIVE_PRED"] = agg_df["TOTAL_SCORE > LINE"] > agg_df["TOTAL_SCORE < LINE"]
    agg_df["NAIVE_PRED_prob"] = agg_df["TOTAL_SCORE > LINE"] / (agg_df["TOTAL_SCORE > LINE"] + agg_df["TOTAL_SCORE < LINE"])
    blp_dataset = pd.merge(blp_dataset, agg_df, on=["HOME_TEAM_NAME", "AWAY_TEAM_NAME"], how="left")
    return blp_dataset


def get_full_nba_live_data(game_ids):
    full_nba_live_data = pd.DataFrame()

    for game in tqdm(game_ids.itertuples()):
        date = game.GAME_DATE
        game_id = game.GAME_ID
        home_team, away_team = game.HOME_TEAM_NAME, game.AWAY_TEAM_NAME
        nba_live_data = load_nba_live_data(game_ids, home_team, away_team, date)
        nba_live_data["GAME_ID"] = game_id
        nba_live_data["GAME_DATE"] = date
        full_nba_live_data = pd.concat([full_nba_live_data, nba_live_data])       

    full_nba_live_data = full_nba_live_data.groupby(["GAME_ID"]).agg({"SCORE_HOME" : max, "SCORE_AWAY" : max})
    full_nba_live_data.columns = [''.join(col).strip() for col in full_nba_live_data.columns.values]
    full_nba_live_data = full_nba_live_data.reset_index()
    return full_nba_live_data

def build_blp_dataset(game_ids):
    lines = [170,180,190,200,205,210,215,220,225,230,235,240,245,250,260]
    
    full_nba_live_data = get_full_nba_live_data(game_ids)
    blp_dataset = pd.merge(game_ids, full_nba_live_data, on=["GAME_ID"], how="left")
    blp_dataset = blp_dataset.sort_values(by="GAME_DATE")

    blp_dataset = add_avgs(blp_dataset)
    blp_dataset = add_lines(blp_dataset, lines)
    blp_dataset = add_comparing_columns(blp_dataset)
    blp_dataset = add_naive_prediction(blp_dataset)    
    return blp_dataset

def train_blp_model(blp_dataset, target, full_training=False):
    
    categorical_encoder = LabelEncoder()
    model = DecisionTreeRegressor()

    if full_training:
      train_set = blp_dataset.copy()

    else:
        train_set = blp_dataset.iloc[:int(len(blp_dataset) * 0.7)]

    X = train_set[['HOME_TEAM_NAME', 'AWAY_TEAM_NAME','HOME_AVG_SCORE', 'AWAY_AVG_SCORE','LINE']]
    y = train_set[target]

    categorical_encoder.fit(X["HOME_TEAM_NAME"])
    X["HOME_TEAM_NAME"] = categorical_encoder.transform(X["HOME_TEAM_NAME"])
    X["AWAY_TEAM_NAME"] = categorical_encoder.transform(X["AWAY_TEAM_NAME"])

    model.fit(X, y)
    return model

def test_blp_model(blp_dataset, target, model):
    categorical_encoder = LabelEncoder()

    test_set = blp_dataset.iloc[int(len(blp_dataset) * 0.7):]
    X = test_set[['HOME_TEAM_NAME', 'AWAY_TEAM_NAME','HOME_AVG_SCORE', 'AWAY_AVG_SCORE','LINE']]
    y = test_set[target]

    categorical_encoder.fit(X["HOME_TEAM_NAME"])
    X["HOME_TEAM_NAME"] = categorical_encoder.transform(X["HOME_TEAM_NAME"])
    X["AWAY_TEAM_NAME"] = categorical_encoder.transform(X["AWAY_TEAM_NAME"])


    print(model.score(X,y))

if __name__ == "__main__":
    game_id= load_game_ids().drop_duplicates()
    blp_dataset = build_blp_dataset(game_id)
    model = train_blp_model(blp_dataset, "NAIVE_PRED_prob")
    test_blp_model(blp_dataset, "NAIVE_PRED_prob", model)
    