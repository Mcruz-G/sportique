import os, sys
from turtle import update

models_path = os.environ["SP_MODELS_PATH"]
sys.path.insert(1, models_path)

from MMMFModel.MMMFModel import NBAModel

def train_mmmf():
    model = NBAModel(update=True)
    return model

if __name__ == "__main__":
    train_mmmf()