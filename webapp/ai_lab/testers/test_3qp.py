import h2o, os, sys, shutil
from h2o.estimators import H2ORandomForestEstimator

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
webapp_path = os.environ["SP_WEBAPP_PATH"]
sys.path.insert(1, data_warehouse_path)
sys.path.insert(1, webapp_path)

from data_warehouse_utils import load_game_ids
from ai_lab.trainers.utils import get_data
from utils import get_test_samples, load_model, fit_data, model_paths
import matplotlib.pyplot as plt


def test_3qp(model_name):
    h2o.init()

    game_ids = load_game_ids()
    game_ids = game_ids.iloc[int(len(game_ids)*0.8):]
    test_data = get_test_samples(game_ids)
    print(test_data.columns)
    model_path = model_paths["3QPModel"]
    model = load_model(model_path)
    y_real = test_data["Y"].as_data_frame()
    y_pred = model.predict(test_data).as_data_frame()
    print(y_pred)
    print(y_pred.columns)
    y_pred.columns = ["Y"]
    error = y_real - y_pred
    plt.plot(error)
    error.hist(bins=100)
    plt.show()
    print(y_real[abs(error["Y"]) > 10])
    print(y_pred[abs(error["Y"]) > 10])
    print(len(error[abs(error["Y"]) > 10])/ len(error))
    print(error.std())
    print(model.explain(test_data))
    print(model.performance(test_data))
if __name__ == "__main__":
    test_3qp("3QPModel")