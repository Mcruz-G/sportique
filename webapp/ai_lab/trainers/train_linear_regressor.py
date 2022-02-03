import os, sys
import numpy as np 

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids, load_nba_live_data
from sklearn.linear_model import LinearRegression

def train_linear_regressor(nba_live_data):
    total_score = nba_live_data.SCORE_HOME + nba_live_data.SCORE_AWAY
    total_score = sorted(list(set(list(total_score.values))))
    total_score = np.array(total_score).reshape(-1, 1)
    t = np.array([x for x in range(len(total_score))]).reshape(-1,1)
    model = LinearRegression().fit(t, total_score)
    return model