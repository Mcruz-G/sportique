import datetime
import pandas as pd
import streamlit as st
from data_warehouse.update_db import update_db
from data_warehouse.data_warehouse_utils import  load_game_ids, load_teams_data, load_nba_live_data  
from webapp.selectbox import build_selectbox
from webapp.date_input import build_date_input
from webapp.buttons import build_get_today_games_button, build_plot_score_button
from webapp.buttons import build_get_final_score_prediction_button
import h2o

if __name__ == "__main__":
    h2o.init()
    st.title("Welcome to Sportique, Angel. It's good to see you again.")
    
    st.subheader("")
    st.subheader("")

    st.header("Pick a game to build some kick ass analytics and win bets using the power of data.")

    game_ids, teams_data= load_game_ids(), load_teams_data()
    home_team, away_team = build_selectbox(teams_data)
    date = build_date_input()
    nba_live_data = load_nba_live_data(game_ids, home_team, away_team, date)
    
    st.subheader("")    
    st.subheader("")   
     
    st.subheader("")    
    build_get_final_score_prediction_button(game_ids, home_team, away_team, date)
    st.subheader("")

    st.subheader("")    
    build_plot_score_button(nba_live_data)
    st.subheader("")
    
    # st.subheader("")    
    # build_get_today_games_button(game_ids)
    # st.subheader("")
    

    st.subheader("... Talk to our admin if you'd like to add any functionality :)")
    update_db()
