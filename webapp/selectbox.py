import streamlit as st

def build_selectbox(teams_data):
    team_names = sorted(list(teams_data["TEAM_NAME"].values))
    home_team = st.selectbox('Select Home Team', team_names)
    team_names = sorted(list(set(team_names) - {home_team}))
    away_team = st.selectbox("Select Away Team", team_names)
    return home_team, away_team

