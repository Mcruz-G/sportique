import h2o, os, sys, shutil
from h2o.estimators import H2ORandomForestEstimator

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids
from utils import get_test_samples, load_model, fit_data, model_paths
import matplotlib.pyplot as plt


def test_3qp(model_name):
    h2o.init()

    game_ids = load_game_ids() #.iloc[:4]
    test_data = get_test_samples(game_ids)
    model_path = model_paths["3QPModel"]
    model = load_model(model_path)
    print(model.model_performance(test_data))
    print(test_data["Y"])
    print(model.predict(test_data))
    y_real = test_data["Y"].as_data_frame()
    y_pred = model.predict(test_data).as_data_frame()
    y_pred.columns = ["Y"]
    error = y_real - y_pred
    plt.plot(error)
    error.hist(bins=100)
    plt.show()
    print(y_real[abs(error["Y"]) > 10])
    print(y_pred[abs(error["Y"]) > 10])
    print(len(error[abs(error["Y"]) > 10])/ len(error))
    print(error.std())
if __name__ == "__main__":
    test_3qp("3QPModel")