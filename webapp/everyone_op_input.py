import streamlit as st

def build_everyone_op_input():
    line_input = st.number_input(
     "Enter the # of games to be averaged    ", value=5, step=1)
    return line_input