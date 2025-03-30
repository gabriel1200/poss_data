#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import requests
import pandas as pd
import time
import json
import os
import sys
import pandas as pd
import re
from datetime import datetime
import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher

def map_action_numbers(old_df, new_df, description_weight=0.65, time_weight=0.35):
    """
    Map actionNumber from new_df to the appropriate rows in old_df based on:
    1. Time ranges and periods (original logic)
    2. Text similarity between descriptions
    
    Parameters:
    old_df (pandas.DataFrame): The old dataset with start_seconds, end_seconds and DESCRIPTION columns
    new_df (pandas.DataFrame): The new dataset with actionNumber, period, clock, and description columns
    description_weight (float): Weight given to description similarity (between 0 and 1)
    time_weight (float): Weight given to time proximity (between 0 and 1)
    
    Returns:
    pandas.DataFrame: A copy of old_df with a new column 'actionNumber' added
    """
    # Make a copy of the old_df to avoid modifying the original
    result_df = old_df.copy()
    
    # Initialize the actionNumber column with NaN
    result_df['actionNumber'] = np.nan
    
    # Convert data types to ensure proper comparison
    result_df['PERIOD'] = result_df['PERIOD'].astype(int)
    result_df['start_seconds'] = result_df['start_seconds'].astype(float)
    result_df['end_seconds'] = result_df['end_seconds'].astype(float)
    
    new_df['period'] = new_df['period'].astype(int)
    new_df['actionNumber'] = new_df['actionNumber'].astype(int)
    
    # Helper function to convert clock format (PT12M00.00S) to seconds
    def clock_to_seconds(clock_str):
        if pd.isna(clock_str) or not isinstance(clock_str, str):
            return None
            
        # Extract minutes and seconds using regex
        minutes_match = re.search(r'PT(\d+)M', clock_str)
        seconds_match = re.search(r'M(\d+\.\d+)S', clock_str)
        
        if not seconds_match:
            seconds_match = re.search(r'M(\d+)S', clock_str)
            
        minutes = int(minutes_match.group(1)) if minutes_match else 0
        seconds = float(seconds_match.group(1)) if seconds_match else 0
        
        return minutes * 60 + seconds
    
    # Calculate game seconds for each action in the new dataset
    new_df['clock_seconds'] = new_df['clock'].apply(clock_to_seconds)
    new_df['period_start_seconds'] = (new_df['period'] - 1) * 720
    new_df['seconds_into_period'] = 720 - new_df['clock_seconds']
    new_df['game_seconds'] = new_df['period_start_seconds'] + new_df['seconds_into_period']
    
    # Sort new_df by period and game_seconds
    new_df = new_df.sort_values(['period', 'game_seconds'])
    
    # Group by period for faster access
    new_df_by_period = {period: group for period, group in new_df.groupby('period')}
    
    # Function to calculate description similarity
    def calculate_description_similarity(desc1, desc2):
        if pd.isna(desc1) or pd.isna(desc2):
            return 0
        
        # Normalize text for comparison
        desc1 = str(desc1).lower().strip()
        desc2 = str(desc2).lower().strip()
        
        # Use SequenceMatcher for string similarity
        return SequenceMatcher(None, desc1, desc2).ratio()
    
    # Extract player names and action types
    def extract_features(description):
        if pd.isna(description):
            return {"player": "", "action": "", "points": ""}
        
        description = str(description).lower()
        
        # Extract player name (usually at the beginning)
        player_match = re.search(r'^([a-z][a-z\.\s]+)', description)
        player = player_match.group(1).strip() if player_match else ""
        
        # Extract action type
        actions = ["free throw", "jump shot", "layup", "dunk", "rebound", "assist", 
                  "steal", "block", "turnover", "foul", "timeout", "substitution"]
        action = ""
        for act in actions:
            if act in description:
                action = act
                break
        
        # Extract points if present
        points_match = re.search(r'\((\d+)\s*PTS\)', description)
        points = points_match.group(1) if points_match else ""
        
        return {"player": player, "action": action, "points": points}
    
    # Improved function to find the action number
    def find_action_number(row):
        period = row['PERIOD']
        start_time = row['start_seconds']
        end_time = row['end_seconds']
        old_description = row['DESCRIPTION'] if 'DESCRIPTION' in row else ""
        
        # Check if we have data for this period
        if period not in new_df_by_period:
            return np.nan
        
        period_data = new_df_by_period[period]
        
        # Find actions that fall within or near the time range
        time_window = 10  # seconds
        potential_matches = period_data[
            (period_data['game_seconds'] >= (start_time - time_window)) & 
            (period_data['game_seconds'] <= (end_time + time_window))
        ]
        
        if potential_matches.empty:
            return np.nan
        
        # If only one match, return it
        if len(potential_matches) == 1:
            return potential_matches['actionNumber'].iloc[0]
        
        # Calculate similarities and combine with time proximity
        best_match = None
        best_score = -1
        
        old_features = extract_features(old_description)
        
        for idx, match in potential_matches.iterrows():
            # Time proximity score (1 for exact match, decreases with distance)
            time_diff = abs(match['game_seconds'] - (start_time + end_time)/2)
            time_proximity = max(0, 1 - (time_diff / (time_window * 2)))
            
            # Description similarity
            if 'description' in match:
                desc_similarity = calculate_description_similarity(old_description, match['description'])
                
                # Feature-based similarity
                new_features = extract_features(match['description'])
                feature_similarity = 0
                if old_features["player"] and old_features["player"] == new_features["player"]:
                    feature_similarity += 0.5
                if old_features["action"] and old_features["action"] == new_features["action"]:
                    feature_similarity += 0.3
                if old_features["points"] and old_features["points"] == new_features["points"]:
                    feature_similarity += 0.2
                
                desc_similarity = max(desc_similarity, feature_similarity)
            else:
                desc_similarity = 0
            
            # Combined score
            combined_score = (description_weight * desc_similarity) + (time_weight * time_proximity)
            
            if combined_score > best_score:
                best_score = combined_score
                best_match = match['actionNumber']
        
        return best_match
    
    # Apply the function to each row in the old dataset
    result_df['actionNumber'] = result_df.apply(find_action_number, axis=1)
    
    # Add a confidence score based on description similarity
    if 'DESCRIPTION' in result_df.columns and 'description' in new_df.columns:
        def calculate_confidence(row):
            if pd.isna(row['actionNumber']):
                return 0
                
            action_data = new_df[new_df['actionNumber'] == row['actionNumber']]
            if action_data.empty:
                return 0
                
            return calculate_description_similarity(
                row['DESCRIPTION'], 
                action_data['description'].iloc[0]
            )
            
        result_df['match_confidence'] = result_df.apply(calculate_confidence, axis=1)
    
    return result_df

def main():
    # Example usage
    old_df = pd.read_csv('old.csv')
    new_df = pd.read_csv('new.csv')
    
    result_df = map_action_numbers(old_df, new_df)
    
    # Save the result
    result_df.to_csv('mapped_results.csv', index=False)
    print(f"Mapping complete. Result saved to 'mapped_results.csv'")
    
    # Display some stats
    total_rows = len(result_df)
    mapped_rows = result_df['actionNumber'].notna().sum()
    mapping_percentage = (mapped_rows / total_rows) * 100
    
    print(f"Total rows in old dataset: {total_rows}")
    print(f"Successfully mapped rows: {mapped_rows} ({mapping_percentage:.2f}%)")


def pull_data(url):
    headers = {
        "Host": "stats.nba.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://stats.nba.com/",
        "Origin": "https://stats.nba.com",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    response = requests.get(url, headers=headers)
    json = response.json()
    
    # Handle the video events format
    if 'resultSets' in json and isinstance(json['resultSets'], dict):
        if 'Meta' in json['resultSets'] and 'videoUrls' in json['resultSets']['Meta']:
            video_urls = json['resultSets']['Meta']['videoUrls']
            playlist = json['resultSets'].get('playlist', [])
            
            # Convert video URLs to dataframe
            video_df = pd.DataFrame(video_urls)
            
            # Convert playlist to dataframe
            playlist_df = pd.DataFrame(playlist)
            
            # Merge video and playlist data
            if not playlist_df.empty:
                df = pd.concat([video_df, playlist_df], axis=1)
            else:
                df = video_df

        else:
            # Fallback in case no video data is found
            df = pd.DataFrame()

    # Handle the original stats format
    elif 'resultSets' in json and isinstance(json['resultSets'], list):
        if len(json["resultSets"]) == 1:
            data = json["resultSets"][0]["rowSet"]
            columns = json["resultSets"][0]["headers"]
            df = pd.DataFrame.from_records(data, columns=columns)
        else:
            data = json["resultSets"][1]["rowSet"]
            columns = json["resultSets"][1]["headers"]["columnNames"]
            df = pd.DataFrame.from_records(data, columns=columns)

    else:
        # Empty dataframe if no recognizable format is found
        df = pd.DataFrame()

    time.sleep(1.2)
    return df

import pandas as pd



# List of NBA team acronyms


# Example: Access the DataFrame for the Atlanta Hawks
# atl_df = team_dfs['ATL']
missing_frame=pd.read_csv('all_missing.csv')
result_frames=[]
teams = [
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]

# Dictionary to store DataFrames for each team
team_dfs = {}

# Loop through each team and read the CSV file
for team in teams:
    file_path = f'2025/{team}_2025_clips_with_players.csv'
    
    if os.path.exists(file_path):  # Ensure the file exists before reading
        team_dfs[team] = pd.read_csv(file_path)
        print(f"Loaded {team}")
    else:
        print(f"File not found: {file_path}")


    all_df = pd.read_csv(file_path)
# Identify the rows to exclude
    mask = all_df['GAMEID'].isin(missing_frame['game_id'].unique())

    # Set the 'URL' column to None for matching GAMEIDs
    all_df.loc[mask, 'URL'] = None

    # Identify GAME_IDs with at least one non-NaN URL
    valid_games = all_df[all_df['URL'].notna()]['GAMEID'].unique()

    # Filter out GAME_IDs without any non-NaN URLs
    missing_url_games = all_df[~all_df['GAMEID'].isin(valid_games)]['GAMEID'].unique()

    print("GAME_IDs without at least one non-NaN URL:")
    print(missing_url_games)


    missing=all_df[all_df.GAMEID.isin(missing_url_games)]


    # API endpoint

    all_rows = []

    for game_id in missing_url_games:
  


    # Direct API endpoint for play-by-play data

        url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_00{game_id}.json"

        # Set headers to mimic a browser request
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

        # Fetch the JSON data
        response = requests.get(url, headers=headers)
        


        if response.status_code == 200:
            data = response.json()
    

            actions = data.get('game', {}).get('actions', [])
                
                # Convert each action into a dictionary and add to the list
            for action in actions:
                action['game_id'] = game_id  # Add the game ID as a column
                all_rows.append(action)
            
        else:
            print(f"Failed to fetch data: {response.status_code}")
        time.sleep(.5)
    time.sleep(.5)
    teamdf = pd.DataFrame(all_rows)

    old_df=missing.copy()

    new_df=teamdf.copy()

    # Example usage
    old_df['GAMEID']='00'+old_df['GAMEID'].astype(str)
    old_df.sort_values(by='GAMEDATE',inplace=True)
    new_df.sort_values(by='timeActual',inplace=True)
    new_df.dropna(subset='teamId',inplace=True)
    new_df['teamId']=new_df['teamId'].astype(int)
    teamid=old_df['TEAM_ID'].iloc[0]
    if team == 'ATL':
        new_df.to_csv('ATL_missing1.csv',index=False)
    #new_df=new_df[new_df.teamId==teamid]
    if team == 'ATL':
        new_df.to_csv('ATL_missing2.csv',index=False)
    result_df = map_action_numbers(old_df, new_df)
    
    # Save the result
   
    print(f"Mapping complete. Result saved to 'mapped_results.csv'")
    if team == 'ATL':
        result_df.to_csv('mapped_atl.csv',index=False)
    # Display some stats
    total_rows = len(result_df)
    mapped_rows = result_df['actionNumber'].notna().sum()
    mapping_percentage = (mapped_rows / total_rows) * 100
    
    print(f"Total rows in old dataset: {total_rows}")
    print(f"Successfully mapped rows: {mapped_rows} ({mapping_percentage:.2f}%)")

    result_frames.append(result_df)
data=pd.concat(result_frames)
data


# In[2]:


data.sort_values(by=['GAMEDATE','GAMEID','PERIOD','start_seconds'],inplace=True)

data=data[data.players_on!='GAME_NOT_FOUND']
data.to_csv('missing_actions.csv',index=False)
data['GAMEID'] = data['GAMEID'].str.replace(r'^00', '', regex=True)
data['GAMEID']= data['GAMEID'].astype(int)


# In[3]:


import pandas as pd
import sys
data=pd.read_csv('missing_actions.csv')
action_map = pd.read_csv('nba_ping_results_final.csv')
action_map


print(action_map.columns)

action_map.rename(columns={'game_id':'GAMEID','action_number':'actionNumber'},inplace=True)
action_map=action_map[['GAMEID','actionNumber','url']]



newdata= data.merge(action_map,how='left',on=['GAMEID','actionNumber'])
newdata.drop(columns='URL',inplace=True)
newdata.rename(columns={'url':'URL'},inplace = True)

print(len(newdata[~newdata.URL.isna()]))



teams = [
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]

# Dictionary to store DataFrames for each team
team_dfs = {}

# Remove leading '00' from each game ID
#newdata['GAMEID'] = newdata['GAMEID'].str.lstrip('0').astype(int)

newdata['GAMEID']=newdata['GAMEID'].astype(int)

# Loop through each team and read the CSV file
for team in teams:
    file_path = f'2025/{team}_2025_clips_with_players.csv'
    df= pd.read_csv(file_path)
    print(len(df))
    teamid=df['TEAM_ID'].iloc[0]
   
    df['GAMEID']=df['GAMEID'].astype(int)




 
    print(len(df.GAMEID.unique()))
    teamnew=newdata[newdata.TEAM_ID==teamid]
    
    # Remove duplicate columns by transposing, dropping duplicates, and transposing back
    teamnew = teamnew.loc[:, ~teamnew.columns.duplicated()].copy()
    teamnew.drop_duplicates(subset=['GAMEID','TEAM_ID','actionNumber'],inplace=True)

    # Verify the columns




    df=df[~df.GAMEID.isin(teamnew.GAMEID.unique())]
  
    print(len(df.GAMEID.unique()))
    print(len(teamnew.GAMEID.unique()))
    df = pd.concat([df,teamnew])
    df.sort_values(by=['GAMEDATE','PERIOD','start_seconds'],inplace=True)
    #df.drop(columns=['level_0','index'],inplace=True)
    df['URL'] = df['URL'].str.replace('320x180.mp4', '1280x720.mp4')
    file_path2 = f'2025/test_{team}_2025_clips_with_players.csv'

    df.to_csv(file_path,index=False)
    print(len(df.GAMEID.unique()))
 


# In[4]:


team='ATL'

file_path2 = f'2025/{team}_2025_clips_with_players.csv'

df=pd.read_csv(file_path2)
df=df[df.GAMEID==22401062]
df.head(40)

