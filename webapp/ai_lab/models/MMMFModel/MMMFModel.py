"""
Predicts NBA scores with regularized matrix factorization.
"""
import os, sys
import urllib.request
import pickle
import subprocess
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm 
import rpy2.robjects as robjects

models_path = os.environ["SP_MODELS_PATH"]
data_warehouse_path = os.environ["SP_DATA_WAREHOUSE_PATH"]
sys.path.insert(1, data_warehouse_path)

from data_warehouse_utils import load_teams_data

class NBAModel:
    """
    NBA model for predicting final scores.
    Seperate predictions are made for Offensive Rating and Pace, which
        are combined tAttributeError: module 'urllib.request' has no attribute 'urlopeno predict the final score.
    """
    def __init__(self, update=False):
        """
        Attributes:
            urls (list): list of basketball reference URLs of games
                to include in model this needs to be manually updated
            teams (list): list of team canonical abbreviations
            box_urls (list): list of URLs to box scores for games
                included in model
            predictions (pd.DataFrame): DataFrame of predicted score.
                Each entry is the predicted score that the team in the
                index will score against each team in the columns.
                To predict a game, two lookups are required, one for
                each team against the other.
        Args:
            update (bool): If True, update predictions DataFrame by
                rescraping and recomputing all values.  Otherwise,
                just use the cached predictions DataFrame.
        """
        self.update = update
        self.game_months = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]
        self.urls = [f"http://www.basketball-reference.com/leagues/NBA_2021_games-{month}.html" for month in self.game_months]
        self.urls += [f"http://www.basketball-reference.com/leagues/NBA_2022_games-{month}.html" for month in self.game_months]
        
        # self.teams = ['ATL', 'BOS', 'BRK', 'CHO', 'CHI', 'CLE',
        #               'DAL', 'DEN', 'HOU', 'DET', 'GSW', 'IND',
        #               'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN',
        #               'NOP', 'NYK', 'OKC', 'ORL', 'PHI', 'PHO',
        #               'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']

        self.teams = list(load_teams_data()["TEAM_ABBREVIATION"].values)

        if update:
            self.box_urls = self.get_urls()
            self.df_pace = pd.DataFrame(0, index=self.teams,
                                        columns=self.teams)
            self.df_OR = pd.DataFrame(0, index=self.teams,
                                      columns=self.teams)
            self.df_pace, self.df_OR = self.make_matrices()
            self.write_matrices()
            self.soft_impute()
        self.predictions_path = os.environ["SP_MODELS_PATH"] + "MMMFModel/predictions.csv"
        self.predictions = self.get_predictions()

    def __repr__(self):
        return "NBAModel(update={update})".format(update=self.update)

    def get_urls(self):
        """
        Gets all URLs for box scores (basketball-reference.com)
            from current season.
        Returns:
            box_urls (list): list of box score URLs from basketball reference
        """
        box_urls = []
        for url in self.urls:
            try:
                print('****', url)
                response = urllib.request.urlopen(url)
                html = response.read()
                soup = BeautifulSoup(html, 'html.parser')
                soup.find_all('a')
                for link in soup.find_all('a'):
                    if link.get('href').startswith('/boxscores/2'):
                        box_urls.append(str(link.get('href')))
                pickle.dump(box_urls, open(models_path + "MMMFModel/box_urls.p", "wb"))
            except:
                pass
        return box_urls

    def get_stats(self, url):
        """
        Extracts statistics from URL
        Args:
            url (str): basketball-reference.com box score
        Returns:
            stats (pd.DataFrame): DataFrame of statistics from game
        """
        response = urllib.request.urlopen(url)
        html = response.read()
        html = html.decode()
        stat_html = html.replace('<!--', "")
        stat_html = stat_html.replace('-->', "")
        stats = pd.read_html(stat_html)
        stats = list(filter(lambda x: ('Unnamed: 0_level_0', 'Unnamed: 0_level_1') in list(x.columns) and ('Unnamed: 1_level_0', 'Pace') and ('Unnamed: 6_level_0', 'ORtg') in list(x.columns), stats))
        stats = stats[0]
        stats = pd.concat([stats["Unnamed: 0_level_0"], stats["Unnamed: 1_level_0"], stats["Unnamed: 6_level_0"]], axis=1)
        stats["TeamName"] = stats["Unnamed: 0_level_1"]
        stats = stats[["TeamName", "Pace", "ORtg"]]
        return stats
        
    def update_df(self, df, team1, team2, value):
        """
        Updates df to add value of team1 and team2.
        For example, you can update the pace dataframe to add a game's pace.csv
        Args:
            df (pd.DataFrame): DataFrame to update
            team1: team on x axis index to update
            team2: team on columns to update
            value: value to add to DataFrame
        Returns:
            df (pd.DataFrame): updated DataFrame
        """
        team1 = team1 if team1 != "CHO" else "CHA"
        team1 = team1 if team1 != "BRK" else "BKN"
        team1 = team1 if team1 != "PHO" else "PHX"

        team2 = team2 if team2 != "CHO" else "CHA"
        team2 = team2 if team2 != "BRK" else "BKN"
        team2 = team2 if team2 != "PHO" else "PHX"
        
        old_value = df[team2].loc[team1]
        print(df)
        print(old_value)
        if old_value == 0:
            new_value = float(value)
        else:
            new_value = (float(old_value) + float(value)) / 2
        df[team2].loc[team1] = new_value
        return df

    def extract_data(self, table):
        """
        Extracts pace and offensive rating data from basketball-
            reference tables
        Args:
            table (pd.DataFrame): table of statistics scraped from
                basketball-reference contains advanced stats for a given games.
        Returns:
            team1 (str): Abbreviation of team1
            team2 (str): Abbreviation of team2
            team1_OR (float): Offensive rating of team1 (points per
                100 posessions)
            team2_OR (float): Offensive rating of team2 (points per
                100 posessions)
            pace (float): pace of game (possessions per game)
        """
        team1 = table["TeamName"].loc[0]
        team2 = table["TeamName"].loc[1]
        pace = table["Pace"][0]
        team1_OR = table["ORtg"].loc[0]
        team2_OR = table["ORtg"].loc[1]
        return team1, team2, team1_OR, team2_OR, pace

    def full_update(self, url, df_pace, df_OR):
        """
        Updates the pace and offensive rating matrices for a given game.
        Args:
            url (str): URL to box score (basketball-reference.com)
            df_pace (pd.DataFrame): pace DataFrame to update
            df_OR (pd.DataFrame): Offensive Rating DataFrame to update
        Returns:
            df_pace, df_OR (pd.DataFrame, pd.DataFrame):
                updated pace and Offensive rating DataFrames
        """

        table = self.get_stats(url)
        team1, team2, team1_OR, team2_OR, pace = self.extract_data(table)
        df_pace = self.update_df(df_pace, team1, team2, pace)
        df_pace = self.update_df(df_pace, team2, team1, pace)
        df_OR = self.update_df(df_OR, team1, team2, team1_OR)
        df_OR = self.update_df(df_OR, team2, team1, team2_OR)
        return df_pace, df_OR

    def make_matrices(self):
        """
        Makes matrices of offesive rating and pace
        Each entry in the matrix is the value (offensive rating or pace)
            of team1 against team2 (rows and columns respectively) for
            all games considered in the model.
        """

        df_pace, df_OR = self.df_pace, self.df_OR
        for url in tqdm(self.box_urls):
            url = 'http://www.basketball-reference.com' + url
            df_pace, df_OR = self.full_update(url, df_pace, df_OR)
        return df_pace, df_OR

    def write_matrices(self):
        """
        Writes pace and offensive ratings csv files.
        """
        self.df_pace.to_csv('pace.csv')
        self.df_OR.to_csv('OR.csv')

    def soft_impute(self):
        """
        Calls soft impute algorithm in R.
        Write predictions.csv
        """
        r_script_path = os.environ["SP_MODELS_PATH"] + "MMMFModel/predict_soft_impute.R"
        r = robjects.r
        r["source"](r_script_path)


    def get_predictions(self):
        """
        Loads predictions from predictions.csv
        Returns:
            predictions (pd.DataFrame): DataFrame of predictions
        """
        predictions = (pd.read_csv(self.predictions_path)
                       .assign(**{'Unnamed: 0': self.teams})
                       .set_index('Unnamed: 0'))
        predictions.columns = self.teams
        return predictions

    def get_scores(self, team1, team2):
        """
        Prints predicted score of two teams playing against each other.
        Teams can be in any order since home team advantage is not considered.
        Args:
            team1 (str): team1 abbreviation
            team2 (str): team2 abbreviation
        Returns:
            None: Prints score
        """
        team1s = self.predictions.loc[team1][team2]
        team2s = self.predictions.loc[team2][team1]
        scores = dict(zip([team1, team2], [team1s, team2s]))
        return scores

if __name__ == "__main__":
    model = NBAModel(update=True)
    scores = model.get_scores("TOR", "CHI")
    print(scores)