import os, sys
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
from .ai_lab.trainers.utils import get_3qp_data, load_model, model_paths
from .ai_lab.trainers.train_linear_regressor import train_linear_regressor
from .utils import compute_kyle_prediction, compute_n_avg, compute_league_avg_pace, compute_league_avg_total_score, get_team_analytics, get_team_games
from .utils import build_linear_regressor_display, build_3qp_display, build_mmmf_display, build_kyle_display, build_mr9zeros_display
from data_warehouse_utils import get_abbs, load_nba_live_data

def build_get_everyones_opinion_button(game_ids, nba_live_data,home_team, away_team, date, line, n_avg):
    build_get_everyones_opinion_button = st.button("Average all the predictions")
    home_team_abb, away_team_abb = get_abbs(home_team, away_team)

    if build_get_everyones_opinion_button:
        t = 120
        linear_model = train_linear_regressor(nba_live_data)
        linear_model_prediction = linear_model.predict(np.array([[t]]))

        mmmf_model = NBAModel()
        mmmf_model_prediction = mmmf_model.get_scores(home_team_abb, away_team_abb)
        mmmf_model_prediction = int(sum(list(mmmf_model_prediction.values())))

        home_avg_score = compute_n_avg("SCORE_HOME",home_team, game_ids, nba_live_data, date,n_avg)
        home_opp_avg_score = compute_n_avg("SCORE_AWAY",home_team, game_ids, nba_live_data, date,n_avg)
        away_avg_score = compute_n_avg("SCORE_HOME",away_team, game_ids, nba_live_data, date,n_avg)
        away_opp_avg_score = compute_n_avg("SCORE_AWAY",away_team, game_ids, nba_live_data, date,n_avg)
        home_avg_pace = compute_n_avg("PACE",home_team, game_ids, nba_live_data, date,n_avg)
        away_avg_pace = compute_n_avg("PACE",away_team, game_ids, nba_live_data, date,n_avg)
        league_avg = compute_league_avg_total_score()
        pace_avg = compute_league_avg_pace()
        kyle_model_prediction = compute_kyle_prediction(home_avg_score, home_opp_avg_score, away_avg_score, away_opp_avg_score,
                        home_avg_pace, away_avg_pace, league_avg, pace_avg)

        mr9zeros_model_prediction = home_avg_score + away_avg_score

        everyones_opinion_prediction = linear_model_prediction + mmmf_model_prediction + kyle_model_prediction + mr9zeros_model_prediction
        everyones_opinion_prediction /= 4
        build_kyle_display(everyones_opinion_prediction, line)
    

def build_get_linear_regressor_button(nba_live_data):
    build_get_linear_regressor_button = st.button("Predict final total score     ")
    if build_get_linear_regressor_button:
        on_click_get_linear_regressor_button(nba_live_data)

def build_get_3qp_score_prediction_button(game_ids, home_team, away_team, date, line):
    build_get_3qp_score_prediction_button = st.button("Predict total score 10 minutes ahead ")
    if build_get_3qp_score_prediction_button:
        on_click_get_3qp_score_prediction(game_ids, home_team, away_team, date, line)    

def build_get_mmmf_score_prediction_button(home_team, away_team, line):
    build_get_mmmf_score_prediction_button = st.button("Predict final total score ")
    if build_get_mmmf_score_prediction_button:
        on_click_get_mmmf_score_prediction(home_team, away_team, line)    

def build_get_kyles_score_prediction_button(game_ids, nba_live_data,home_team, away_team, date, line, n_avg):
    build_get_kyles_score_prediction_button = st.button("Predict final total score  ")
    if build_get_kyles_score_prediction_button:
        on_click_get_kyles_score_prediction(game_ids, nba_live_data,home_team, away_team, date, line, n_avg)

def build_get_mr9zeros_score_prediction_button(game_ids, nba_live_data, home_team, away_team, date, line, n_avg):
    build_get_mr9zeros_score_prediction_button = st.button("Predict final total score   ")
    if build_get_mr9zeros_score_prediction_button:
        on_click_get_mr9zeros_score_prediction(game_ids, nba_live_data, home_team, away_team, date, line, n_avg)

def build_plot_score_button(nba_live_data):
    build_plot_score_button = st.button("Plot game scores")
    if build_plot_score_button:
        on_click_plot_score(nba_live_data)

def build_plot_score_analytics_button(game_ids, home_team, away_team):
    build_plot_score_analytics_button = st.button("Plot analytics")
    if build_plot_score_analytics_button:
        on_click_plot_score_analytics(game_ids, home_team, away_team)

def build_get_today_games_button(game_ids):
    build_get_today_games_button = st.button("Get today games")
    if build_get_today_games_button:
        on_click_get_today_games(game_ids)

def on_click_get_linear_regressor_button(nba_live_data):
    t = 120
    model = train_linear_regressor(nba_live_data)
    prediction = model.predict(np.array([[t]]))
    build_linear_regressor_display(prediction)

def on_click_get_3qp_score_prediction(game_ids, home_team, away_team, date, line):
    game_ids = game_ids[game_ids["GAME_DATE"] == date][game_ids["HOME_TEAM_NAME"] == home_team].drop_duplicates()
    live_data = get_3qp_data(game_ids, live=True)
    model_path = model_paths["3QPModel"]
    model = load_model(model_path)
    pred = model.predict(live_data).as_data_frame()
    build_3qp_display(pred, live_data, line)

def on_click_get_mmmf_score_prediction(home_team, away_team, line):
    
    model = NBAModel()
    scores = model.get_scores(home_team, away_team) 
    build_mmmf_display(scores, home_team, away_team, line)

def on_click_get_kyles_score_prediction(game_ids, nba_live_data, home_team, away_team, date, line, n_avg):
    home_avg_score = compute_n_avg("SCORE_HOME",home_team, game_ids, nba_live_data, date,n_avg)
    home_opp_avg_score = compute_n_avg("SCORE_AWAY",home_team, game_ids, nba_live_data, date,n_avg)
    away_avg_score = compute_n_avg("SCORE_HOME",away_team, game_ids, nba_live_data, date,n_avg)
    away_opp_avg_score = compute_n_avg("SCORE_AWAY",away_team, game_ids, nba_live_data, date,n_avg)
    home_avg_pace = compute_n_avg("PACE",home_team, game_ids, nba_live_data, date,n_avg)
    away_avg_pace = compute_n_avg("PACE",away_team, game_ids, nba_live_data, date,n_avg)
    league_avg = compute_league_avg_total_score()
    pace_avg = compute_league_avg_pace()
    final_score = compute_kyle_prediction(home_avg_score, home_opp_avg_score, away_avg_score, away_opp_avg_score,
                    home_avg_pace, away_avg_pace, league_avg, pace_avg)
    build_kyle_display(final_score, line)

def on_click_get_mr9zeros_score_prediction(game_ids, nba_live_data, home_team, away_team, date, line, n_avg):
    home_avg_score = compute_n_avg("SCORE_HOME",home_team, game_ids, nba_live_data, date,n_avg)
    away_avg_score = compute_n_avg("SCORE_HOME",away_team, game_ids, nba_live_data, date,n_avg)
    scores = dict(zip([home_team, away_team], [home_avg_score, away_avg_score]))
    build_mr9zeros_display(scores, line)


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

def on_click_get_today_games(game_ids):
    date = datetime.now().date()
    target_games = game_ids[game_ids["GAME_DATE"] == date]
    if len(target_games) == 0:
        st.info("Oops! Looks like there are no games today")
    else:
        data_to_show = game_ids[["HOME_TEAM_NAME", "AWAY_TEAM_NAME"]]
        data_to_show.rename({"HOME_TEAM_NAME" : "Home", "AWAY_TEAM_NAME" : "Away"})
        st.table(data = data_to_show)

