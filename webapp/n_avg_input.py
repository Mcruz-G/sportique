import streamlit as st

def build_n_avg_input():
    line_input = st.number_input(
     "Compute the avg. of the last n games", value=5, step=1)
    return line_input