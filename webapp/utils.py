from audioop import avg
import os, sys
from re import L
import pandas as pd
from data_warehouse.data_warehouse_utils import load_teams_data
import streamlit as st

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids, load_nba_live_data

def build_3qp_display(prediction, live_data, line):
    live_data = live_data.as_data_frame()
    predicted_value = list(prediction.iloc[[-1]].values)[0]
    error_margin = 8 if predicted_value < 200 else 15
    value = f"{int(predicted_value)}    +/- {error_margin}"
    st.metric(label="Predicted Score:", value=value)

def build_mmmf_display(scores, home_team, away_team, line):
    value = int(sum(list(scores.values())))
    delta = int(value - line)
    st.metric(label="Predicted Score:", value=value, delta=delta)
    st.metric(label= home_team, value=int(scores[home_team]))
    st.metric(label= away_team, value=int(scores[away_team]))

def build_kyle_display(score, line):
    value = int(score)
    delta = int(value - line)
    st.metric(label="Predicted Score:", value=value, delta=delta)

def build_mr9zeros_display(scores, line):
    value = int(sum(list(scores.values())))
    delta = int(value - line)
    home_team, away_team = list(scores.keys())[0], list(scores.keys())[1]

    st.metric(label="Predicted Score:", value=value, delta=delta)
    st.metric(label= home_team, value=int(scores[home_team]))
    st.metric(label= away_team, value=int(scores[away_team]))

def get_last_n_games(team, game_ids, n_avg, date):
    home_games = game_ids[game_ids["HOME_TEAM_NAME"] == team]
    away_games = game_ids[game_ids["AWAY_TEAM_NAME"] == team]
    team_games = pd.concat([home_games, away_games])
    team_games = team_games[team_games["GAME_DATE"] < date]
    team_games = team_games.sort_values(by="GAME_DATE")
    team_games = team_games.iloc[-n_avg:]

    return team_games

def compute_n_avg(target_column, team, game_ids, nba_live_data, date, n_avg):
    game_ids = game_ids.drop_duplicates()
    teams_data = load_teams_data()
    last_n_games = get_last_n_games(team, game_ids, n_avg, date)
    dates = list(last_n_games["GAME_DATE"])
    avg = 0 
    num_games = len(dates)

    for date in dates:
        game = last_n_games[last_n_games["GAME_DATE"] == date]
        home_team, away_team = game["HOME_TEAM_NAME"].iloc[0], game["AWAY_TEAM_NAME"].iloc[0]
        game_data = load_nba_live_data(game_ids, home_team, away_team, date)
        game_data["PACE"] = game_data["POSSESSION"].apply(lambda x: list(teams_data[teams_data["TEAM_ID"] == x]["TEAM_NAME"]))
        game_data["PACE"] = game_data["PACE"].apply(lambda x: x[0] if len(x) > 0 else 0)
        if target_column == "PACE":
            count = game_data.PACE.value_counts().iloc[:-1]
            count /= sum(count)
            count *= 200
            avg += count[team]
        else:
            avg += game_data[target_column].iloc[-1]

    avg /= num_games
    return avg


def compute_league_avg_total_score():
    return 223.6

def compute_league_avg_pace():
    return 99.1

def compute_kyle_prediction(home_avg_score, home_opp_avg_score, away_avg_score, 
                            away_opp_avg_score, home_avg_pace, away_avg_pace,
                            league_avg, pace_avg):

    prediction = (home_avg_score + home_opp_avg_score) * (away_avg_score + away_opp_avg_score) / league_avg
    prediction *= (home_avg_pace + away_avg_pace) / (2 * pace_avg)
    return prediction

def compute_mr9zeros_prediction(home_avg_score, away_avg_score):

    prediction = home_avg_score + away_avg_score
    return prediction