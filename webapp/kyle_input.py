import streamlit as st

def build_kyle_input():
    line_input = st.number_input(
     "Enter the # of games to be averaged  ", value=5, step=1)
    return line_input