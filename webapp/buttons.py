import streamlit as st
from datetime import datetime
import plotly.express as px


def build_plot_score_button(nba_live_data):
    build_plot_score_button = st.button("Plot game scores")
    if build_plot_score_button:
        on_click_plot_score(nba_live_data)

def build_get_today_games_button(game_ids):
    build_get_today_games_button = st.button("Get today games")
    if build_get_today_games_button:
        on_click_get_today_games(game_ids)

def on_click_plot_score(nba_live_data):
    fig = px.line(nba_live_data[["SCORE_HOME", "SCORE_AWAY"]])
    st.plotly_chart(fig)
    fig = px.line(nba_live_data[["TOTAL_SCORE"]])
    st.plotly_chart(fig)
def on_click_get_today_games(game_ids):
    date = datetime.now().date()
    target_games = game_ids[game_ids["GAME_DATE"] == date]
    if len(target_games) == 0:
        st.info("Oops! Looks like there are no games today")
    else:
        data_to_show = game_ids[["HOME_TEAM_NAME", "AWAY_TEAM_NAME"]]
        data_to_show.rename({"HOME_TEAM_NAME" : "Home", "AWAY_TEAM_NAME" : "Away"})
        st.table(data = data_to_show)