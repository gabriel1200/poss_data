#!/usr/bin/env python
# coding: utf-8

# In[23]:


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


def map_action_numbers(old_df, new_df):
    """
    Map actionNumber from new_df to the appropriate rows in old_df based on time ranges and periods.
    
    Parameters:
    old_df (pandas.DataFrame): The old dataset with start_seconds and end_seconds columns
    new_df (pandas.DataFrame): The new dataset with actionNumber, period, and clock columns
    
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
    
    # Sort new_df by period and game_seconds (should already be in order but just to be sure)
    new_df = new_df.sort_values(['period', 'game_seconds'])
    
    # Group by period for faster access
    new_df_by_period = {period: group for period, group in new_df.groupby('period')}
    
    # Function to find the nearest action number for a given time range and period
    def find_action_number(row):
        period = row['PERIOD']
        start_time = row['start_seconds']
        end_time = row['end_seconds']
        
        # Check if we have data for this period
        if period not in new_df_by_period:
            return np.nan
        
        period_data = new_df_by_period[period]
        
        # Find actions that fall within the time range
        matches = period_data[
            (period_data['game_seconds'] >= start_time) & 
            (period_data['game_seconds'] <= end_time)
        ]
        
        if not matches.empty:
            # Return the first action number in the time range
            # (actions are already ordered chronologically)
            return matches['actionNumber'].iloc[0]
        
        # If no direct match, find the closest action before the time range
        before_matches = period_data[period_data['game_seconds'] < start_time]
        if not before_matches.empty:
            return before_matches['actionNumber'].iloc[-1]
        
        # If still no match, find the closest action after the time range
        after_matches = period_data[period_data['game_seconds'] > end_time]
        if not after_matches.empty:
            return after_matches['actionNumber'].iloc[0]
        
        return np.nan
    
    # Apply the function to each row in the old dataset
    result_df['actionNumber'] = result_df.apply(find_action_number, axis=1)
    
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

    # Identify GAME_IDs with at least one non-NaN URL
    valid_games = all_df[all_df['URL'].notna()]['GAMEID'].unique()

    # Filter out GAME_IDs without any non-NaN URLs
    missing_url_games = all_df[~all_df['GAMEID'].isin(valid_games)]['GAMEID'].unique()

    print("GAME_IDs without at least one non-NaN URL:")
    print(missing_url_games)


    missing=all_df[all_df.GAMEID.isin(missing_url_games)]
    missing

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
        time.sleep(1)
    time.sleep(1)
    teamdf = pd.DataFrame(all_rows)


    old_df=missing.copy()

    new_df=teamdf.copy()

    # Example usage
    old_df['GAMEID']='00'+old_df['GAMEID'].astype(str)
    old_df.sort_values(by='GAMEDATE',inplace=True)
    new_df.sort_values(by='timeActual',inplace=True)

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

    result_frames.append(result_df)


# In[42]:


all_missing=pd.concat(result_frames)
all_missing.to_csv('all_missing.csv',index=False)


# In[43]:


# Convert unhashable columns to strings
all_missing = all_missing.applymap(lambda x: str(x) if isinstance(x, list) else x)
print(len(all_missing))
# Drop duplicates
all_missing = all_missing.drop_duplicates()
print(len(all_missing))



# In[41]:


all_missing.columns
print(len(all_missing))
all_missing.drop_duplicates(inplace=True)


# In[44]:


# result = pd.DataFrame({'actionNumber': [1, 2, 3, None, 5, None, 7], 'DESCRIPTION': ['A', 'B', 'C', 'D', 'E', 'F', 'G']})
import pandas as pd
import requests
import time

# Example dataframe
# all_missing = pd.DataFrame({'actionNumber': [4, 7, 10], 'GAMEID': ['0022401005', '0022401006', '0022401007']})

# Function to ping the URL for each row
def ping_nba_urls(df):
    base_url = "https://stats.nba.com/stats/videoeventsasset"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nba.com",
        "Accept": "application/json"
    }
    
    results = []
    count=0
    df=df.drop_duplicates(subset='actionNumber')
    
    for index, row in df.iterrows():
        
        action_number = row['actionNumber']
        if not pd.isna(action_number):

            action_number=int(action_number)
            game_id = row['game_id']
            description=row['description']
            url = f"{base_url}?GameEventID={action_number}&GameID=00{game_id}"


            try:
                response = requests.get(url, headers=headers, timeout=10)
                data=response.json()
                playlist=data['resultSets']['Meta']['videoUrls']
                if len(playlist)>0:
                    video_link=playlist[0]['surl']
                else:
                    video_link=None

                if response.status_code == 200:
                    results.append({
                        "game_id": game_id,
                        "action_number": action_number,
                        "status": "Success",
                        "description":description,
                        "url":video_link
                    })
                else:
                    results.append({
                        "game_id": game_id,
                        "action_number": action_number,
                        "status": f"Failed: {response.status_code}",
                
                    })
            
            except Exception as e:
                results.append({
                    "game_id": game_id,
                    "action_number": action_number,
                    "status": "Error",
                    
                })
            
            # Sleep to avoid being rate-limited
            count+=1
            if count %100 ==0:
                print(f"{count} of {len(df)} completed")

                time.sleep(0.1)  # Adjust sleep time as needed
        
    return pd.DataFrame(results)

# Ping URLs and save the results

result_df = ping_nba_urls(all_missing)

# Save the results to a CSV or inspect
result_df.to_csv("nba_ping_results.csv", index=False)
print(result_df.head())

