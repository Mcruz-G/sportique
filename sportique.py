import pandas as pd
import streamlit as st
from data_warehouse.update_db import update_db
from data_warehouse.data_warehouse_utils import  load_game_ids, load_teams_data, load_nba_live_data, get_abbs 
from webapp.selectbox import build_selectbox
from webapp.mr9zeros_input import build_mr9zeros_input
from webapp.kyle_input import build_kyle_input
from webapp.line_input import build_line_input
from webapp.date_input import build_date_input
from webapp.buttons import build_plot_score_button
from webapp.buttons import build_get_mmmf_score_prediction_button
from webapp.buttons import build_get_kyles_score_prediction_button
from webapp.buttons import build_get_mr9zeros_score_prediction_button, build_get_linear_regressor_button

if __name__ == "__main__":
    st.title("Welcome to Sportique, Angel. It's good to see you again.")
    
    st.subheader("")
    st.subheader("")

    st.header("Pick a game to build some kick ass analytics and win bets using the power of data.")

    game_ids, teams_data= load_game_ids(), load_teams_data()
    home_team, away_team = build_selectbox(teams_data)
    home_team_abb, away_team_abb = get_abbs(home_team, away_team)
    date = build_date_input()
    line = build_line_input()
    nba_live_data = load_nba_live_data(game_ids, home_team, away_team, date)
    
    st.subheader("")    
    st.subheader("")   
    
    st.subheader("Linear Predictor")   
     
    st.subheader("")    
    build_get_linear_regressor_button(nba_live_data)
    st.subheader("")

    st.subheader("MMMF Model")   

    st.subheader("")    
    build_get_mmmf_score_prediction_button(home_team_abb, away_team_abb, line)
    st.subheader("")

    st.subheader("Kyle's Model")   
    
    st.subheader("")    
    n_avg = build_kyle_input()
    build_get_kyles_score_prediction_button(game_ids, nba_live_data, home_team, away_team, date, line, n_avg)
    st.subheader("")
    
    st.subheader("Mr. 9zeros Model")   

    st.subheader("")    
    n_avg = build_mr9zeros_input()
    build_get_mr9zeros_score_prediction_button(game_ids, nba_live_data, home_team, away_team, date, line, n_avg)
    st.subheader("")
    
    st.header("Visualize some analytics")   

    st.subheader("")    
    build_plot_score_button(nba_live_data)
    st.subheader("")

    st.subheader("... Talk to our admin if you'd like to add any functionality :)")
    update_db()
