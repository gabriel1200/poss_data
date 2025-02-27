#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
import json
import time
import datetime
import os
from datetime import timedelta

def get_team_dict():
    """Returns a dictionary mapping team abbreviations to team IDs."""
    return {
        'ATL': '1610612737', 'BKN': '1610612751', 'BOS': '1610612738', 'CHA': '1610612766',
        'CHI': '1610612741', 'CLE': '1610612739', 'DAL': '1610612742', 'DEN': '1610612743',
        'DET': '1610612765', 'GSW': '1610612744', 'HOU': '1610612745', 'IND': '1610612754',
        'LAC': '1610612746', 'LAL': '1610612747', 'MEM': '1610612763', 'MIA': '1610612748',
        'MIL': '1610612749', 'MIN': '1610612750', 'NOP': '1610612740', 'NYK': '1610612752',
        'OKC': '1610612760', 'ORL': '1610612753', 'PHI': '1610612755', 'PHX': '1610612756',
        'POR': '1610612757', 'SAC': '1610612758', 'SAS': '1610612759', 'TOR': '1610612761',
        'UTA': '1610612762', 'WAS': '1610612764'
    }
def rotation_download(ps=False):
    if ps:
        trail = '_ps'
    else:
        trail = ''
    team_dict=get_team_dict()
    for year in range(2014,2025):
        rotations=[]

        for team in team_dict.keys():
            url = f"https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/rotations/{year}/{team_dict[team]}{trail}.csv"
            try:
                df = pd.read_csv(url)
                rotations.append(df)
                print(f"Successfully read data from: {url}")  # Optional: Confirmation message

            except HTTPError as e:  # Catch HTTPError, which includes 404, 500, etc.
                #print(f"Could not read data from: {url}")
                #print(f"Error: {e}")  # Print the specific error message
                # You might want to log this error or take other actions, like trying a different URL
                continue  # Skip to the next team

        rotations_df = pd.concat(rotations)


        rotations_df.to_csv(f"{year}{trail}_rotations.csv",index=False)
rotation_download(ps=False)

