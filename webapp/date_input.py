import streamlit as st
from datetime import datetime

def build_date_input():
    date_input = st.date_input(
     "Select the game's date",
     datetime.now())
    return date_input