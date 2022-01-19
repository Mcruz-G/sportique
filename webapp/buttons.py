import streamlit as st
from datetime import datetime
import plotly.express as px
from .ai_lab.trainers.utils import get_data, load_model, model_paths
from .utils import build_prediction_display

def build_get_final_score_prediction_button(game_ids, home_team, away_team, date):
    build_get_final_score_prediction_button = st.button("Predict Score 10 mins ahead")
    if build_get_final_score_prediction_button:
        on_click_get_final_score_prediction(game_ids, home_team, away_team, date)    

def build_plot_score_button(nba_live_data):
    build_plot_score_button = st.button("Plot game scores")
    if build_plot_score_button:
        on_click_plot_score(nba_live_data)

def build_get_today_games_button(game_ids):
    build_get_today_games_button = st.button("Get today games")
    if build_get_today_games_button:
        on_click_get_today_games(game_ids)


def on_click_get_final_score_prediction(game_ids, home_team, away_team, date):
    game_ids = game_ids[game_ids["GAME_DATE"] == date][game_ids["HOME_TEAM_NAME"] == home_team].drop_duplicates()
    live_data = get_data(game_ids, live=True)
    model_path = model_paths["3QPModel"]
    model = load_model(model_path)
    pred = model.predict(live_data).as_data_frame()
    build_prediction_display(pred, live_data)


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

def on_click_get_today_games(game_ids):
    date = datetime.now().date()
    target_games = game_ids[game_ids["GAME_DATE"] == date]
    if len(target_games) == 0:
        st.info("Oops! Looks like there are no games today")
    else:
        data_to_show = game_ids[["HOME_TEAM_NAME", "AWAY_TEAM_NAME"]]
        data_to_show.rename({"HOME_TEAM_NAME" : "Home", "AWAY_TEAM_NAME" : "Away"})
        st.table(data = data_to_show)