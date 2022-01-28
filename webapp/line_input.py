import streamlit as st

def build_line_input():
    line_input = st.number_input(
     "Enter the betting line", value=200, step=1)
    return line_input