from json import encoder
import os, sys
from turtle import home
data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

import matplotlib
import matplotlib.pyplot as plt
matplotlib.use('TkAgg')

import numpy as np
import streamlit as st
import plotly.express as px
from datetime import datetime
from .ai_lab.models.MMMFModel.MMMFModel import NBAModel 
from .ai_lab.trainers.utils import load_model, model_paths
from .ai_lab.trainers.train_linear_regressor import train_linear_regressor
from .ai_lab.models.BLPModel.BLPModel import build_blp_dataset
from .utils import compute_kyle_prediction, compute_n_avg, compute_league_avg_pace, compute_league_avg_total_score, get_team_analytics
from .utils import build_prediction_display
from data_warehouse_utils import get_abbs, load_nba_live_data, load_game_ids

def build_get_everyones_opinion_button(game_ids, nba_live_data,home_team, away_team, date, line):
    build_get_everyones_opinion_button = st.button("Average all the predictions")
    home_team_abb, away_team_abb = get_abbs(home_team, away_team)

    if build_get_everyones_opinion_button:

        linear_regressor_pred = on_click_get_linear_regressor_button(nba_live_data, line)
        mmmf_pred = on_click_get_mmmf_score_prediction(home_team_abb, away_team_abb, line)
        blp_pred = on_click_get_blp_prediction(game_ids, home_team, away_team, date, line)
        predictions = [linear_regressor_pred, mmmf_pred, blp_pred]
        everyone_pred = {"over": 0, "confidence" : 0}

        for p in predictions:
            if p["over"]:
                everyone_pred["over"] += 1

        everyone_pred["confidence"] = everyone_pred["over"] / len(predictions)
        everyone_pred["over"] = True if everyone_pred["over"] >= len(predictions) // 2 else False
        
        build_prediction_display(everyone_pred)


def build_get_linear_regressor_button(nba_live_data, line):

    build_get_linear_regressor_button = st.button("Predict Over/Under Line    ")
    if build_get_linear_regressor_button:
        prediction = on_click_get_linear_regressor_button(nba_live_data, line)
        build_prediction_display(prediction)

def build_get_mmmf_score_prediction_button(home_team, away_team, line):
    build_get_mmmf_score_prediction_button = st.button("Predict Over/Under Line")
    if build_get_mmmf_score_prediction_button:
        prediction = on_click_get_mmmf_score_prediction(home_team, away_team, line)    
        build_prediction_display(prediction)

def build_get_blp_prediction_button(game_ids, home_team, away_team, date, line):
    build_get_blp_prediction_button = st.button("Predict Over/Under Line        ")
    if build_get_blp_prediction_button:
        prediction = on_click_get_blp_prediction(game_ids, home_team, away_team, date, line)
        build_prediction_display(prediction)

def build_plot_score_button(nba_live_data):
    build_plot_score_button = st.button("Plot game scores")
    if build_plot_score_button:
        on_click_plot_score(nba_live_data)

def build_plot_score_analytics_button(game_ids, home_team, away_team):
    build_plot_score_analytics_button = st.button("Plot analytics")
    if build_plot_score_analytics_button:
        on_click_plot_score_analytics(game_ids, home_team, away_team)

def on_click_get_linear_regressor_button(nba_live_data, line):
    t = 120
    model = train_linear_regressor(nba_live_data)
    model_pred = model.predict(np.array([[t]]))
    model_pred = model_pred[0][0]
    r2 = model_pred / line
    prediction = {"over" : model_pred > line, "confidence" : r2, "num_pred" : model_pred}
    return prediction


def on_click_get_mmmf_score_prediction(home_team, away_team, line):
    
    model = NBAModel()
    scores = model.get_scores(home_team, away_team) 
    model_pred = sum(list(scores.values()))
    r2 = 1 - model_pred / line
    prediction = {"over" : model_pred > line, "confidence" : r2, "num_pred" : model_pred}
    return prediction

def on_click_get_blp_prediction(game_ids, home_team, away_team, date, line):
    model_path = model_paths["BLPModel"]
    encoder_path = model_paths["BLPencoder"]
    model = load_model(model_path)
    encoder = load_model(encoder_path) 
    
    sample = build_blp_dataset(game_ids.drop_duplicates().iloc[-500:])
    sample = sample[sample.HOME_TEAM_NAME == home_team][sample.AWAY_TEAM_NAME == away_team][sample.GAME_DATE == date]    

    X = sample[['HOME_TEAM_NAME', 'AWAY_TEAM_NAME','HOME_AVG_SCORE', 'AWAY_AVG_SCORE','LINE']]
    X["HOME_TEAM_NAME"] = encoder.transform(X["HOME_TEAM_NAME"])
    X["AWAY_TEAM_NAME"] = encoder.transform(X["AWAY_TEAM_NAME"])
    model_pred = model.predict(X)[0]
    
    r2 = model_pred
    prediction = {"over" : model_pred > 0.5, "confidence" : r2, "num_pred" : line * (1 + model_pred)}
    return prediction

def on_click_plot_score(nba_live_data):
    fig = px.line(nba_live_data, x="TIME_ACTUAL", y="SCORE_HOME")
    st.plotly_chart(fig)
    home_score = nba_live_data["SCORE_HOME"].iloc[-1]
    st.subheader(f"Current Home Score: {home_score}")
    
    fig = px.line(nba_live_data, x="TIME_ACTUAL", y="SCORE_AWAY")
    st.plotly_chart(fig)
    away_score = nba_live_data["SCORE_AWAY"].iloc[-1]
    st.subheader(f"Current Visit Score: {away_score}")

    fig = px.line(nba_live_data, x="TIME_ACTUAL", y="TOTAL_SCORE")
    st.plotly_chart(fig)
    total_score = nba_live_data["TOTAL_SCORE"].iloc[-1]
    st.subheader(f"Current Total Score: {total_score}")

def on_click_plot_score_analytics(game_ids, home_team, away_team):
    for team in [home_team, away_team]:
        st.subheader(f"{team}'s Score Analytics")   
        team_analytics = get_team_analytics(game_ids, team)
        st.bar_chart(team_analytics["SCORE"].loc[:,["mean"]])


