import h2o
import os, sys
from h2o.estimators import H2ORandomForestEstimator

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
webapp_path = os.environ["SP_WEBAPP_PATH"]
sys.path.insert(1, data_warehouse_path)
sys.path.insert(1, webapp_path)

import matplotlib.pyplot as plt
from data_warehouse_utils import load_game_ids
from utils import get_test_samples, load_model, model_paths


def test_3qp(model_name):
    h2o.init()

    game_ids = load_game_ids()
    game_ids = game_ids.iloc[int(len(game_ids)*0.8):]
    test_data = get_test_samples(game_ids)
    model_path = model_paths["3QPModel"]
    model = load_model(model_path)
    y_real = test_data["Y"].as_data_frame()
    y_pred = model.predict(test_data).as_data_frame()
    y_pred.columns = ["Y"]
    error = y_real - y_pred
    plt.plot(error)
    error.hist(bins=100)
    plt.show()
if __name__ == "__main__":
    test_3qp("3QPModel")