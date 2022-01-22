from turtle import home
import h2o, os, sys, shutil
from h2o.estimators import H2ORandomForestEstimator

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
webapp_path = os.environ["SP_WEBAPP_PATH"]
sys.path.insert(1, data_warehouse_path)
sys.path.insert(1, webapp_path)

from data_warehouse_utils import load_game_ids
from utils import get_test_samples, load_model, model_paths
import matplotlib.pyplot as plt


def test_team_models():
    h2o.init()

    game_ids = load_game_ids()
    game_ids = game_ids.iloc[int(len(game_ids)*0.8):]

    away_test_data = get_test_samples(game_ids, "away")
    away_model_path = model_paths["AwayTeamModel"]
    away_model = load_model(away_model_path)

    home_test_data = get_test_samples(game_ids, "home")
    home_model_path = model_paths["HomeTeamModel"]
    home_model = load_model(home_model_path)

    home_y_real = home_test_data["Y"].as_data_frame()
    home_y_pred = home_model.predict(home_test_data).as_data_frame()

    away_y_real = away_test_data["Y"].as_data_frame()
    away_y_pred = away_model.predict(away_test_data).as_data_frame()

    home_y_pred.columns = ["Y"]
    away_y_pred.columns = ["Y"]
    
    y_real = home_y_real + away_y_real
    y_pred = home_y_pred + away_y_pred

    error = y_real - y_pred
    plt.plot(error)
    error.hist(bins=50)
    plt.show()
    print(y_real[abs(error["Y"]) > 10])
    print(y_pred[abs(error["Y"]) > 10])
    print(len(error[abs(error["Y"]) > 10])/ len(error))
    print(error.std())
if __name__ == "__main__":
    test_team_models()