import h2o, os, sys, shutil
from h2o.estimators import H2ORandomForestEstimator

data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_game_ids
from utils import get_data, fit_data, save_model, log

def train_3qp(model_name):
    model_dir = os.environ["SP_MODELS_PATH"] + f"{model_name}/"
    model_path = model_dir + f"{model_name}"
    if os.path.exists(model_path):
        shutil.rmtree(model_dir)
    
    h2o.init()
    model = H2ORandomForestEstimator(model_name)
    game_ids = load_game_ids()
    data = get_data(game_ids)
    model = fit_data(model, data)
    save_model(model, model_dir)
    log(game_ids, model_dir, model_name)

if __name__ == "__main__":
    train_3qp("3QPModel")