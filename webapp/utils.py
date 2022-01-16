import os, sys
import streamlit as st

def build_prediction_display(prediction, live_data):
    live_data = live_data.as_data_frame()
    curr_value = list(live_data.iloc[[-1]]["TOTAL_SCORE"].values)[0]
    predicted_value = list(prediction.iloc[[-1]].values)[0]
    error_margin = 8 if predicted_value < 200 else 15
    error_margin_str = f"error margin: +/- {error_margin}"
    value = f"{int(predicted_value)} +/- {error_margin} points"
    st.metric(label="Predicted Score:", value=value)