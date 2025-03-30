#!/usr/bin/env python
# coding: utf-8

# In[1]:


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

def map_action_numbers(old_df, new_df):
    """
    Map actionNumber from new dataset to old dataset based on period and time information.
    
    Parameters:
    -----------
    old_df : pandas.DataFrame
        The old NBA play-by-play dataset
    new_df : pandas.DataFrame
        The new NBA play-by-play dataset with actionNumber
        
    Returns:
    --------
    pandas.DataFrame
        Old dataset with mapped actionNumber from new dataset
    """
    # Make a copy of the old DataFrame to avoid modifying the original
    old_df_with_action = old_df.copy()
    
    # Initialize the actionNumber column with None/NaN
    old_df_with_action['actionNumber'] = None
    
    # Function to convert "PT12M00.00S" format to seconds remaining in period
    def clock_to_seconds(clock_str):
        if pd.isna(clock_str) or clock_str is None:
            return None
        
        # Parse the clock string using regex
        match = re.match(r'PT(\d+)M(\d+\.\d+)S', clock_str)
        if match:
            minutes = int(match.group(1))
            seconds = float(match.group(2))
            return minutes * 60 + seconds
        return None
    
    # Calculate seconds remaining in period for both datasets
    new_df['seconds_remaining'] = new_df['clock'].apply(clock_to_seconds)
    
    # Convert MM:SS format to seconds from start of the period
    def mmss_to_seconds_from_start(time_str, period):
        if pd.isna(time_str) or time_str is None:
            return None
        
        # Parse MM:SS format
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = int(parts[1])
            
            # Calculate seconds from start of period
            # NBA periods are 12 minutes (720 seconds)
            return 720 - (minutes * 60 + seconds)
        return None
    
    # Calculate seconds from start of period for old dataset
    old_df_with_action['start_seconds_in_period'] = old_df_with_action.apply(
        lambda row: mmss_to_seconds_from_start(row['STARTTIME'], row['PERIOD']), 
        axis=1
    )
    
    old_df_with_action['end_seconds_in_period'] = old_df_with_action.apply(
        lambda row: mmss_to_seconds_from_start(row['ENDTIME'], row['PERIOD']), 
        axis=1
    )
    
    # Iterate through each row in the old dataset
    for idx, old_row in old_df_with_action.iterrows():
        # Find matching actions in the new dataset by period and time range
        period_matches = new_df[new_df['period'] == old_row['PERIOD']]
        
        # If no matches for this period, continue to next row
        if len(period_matches) == 0:
            continue
        
        # Find actions that fall within the time range
        time_matches = period_matches[
            (period_matches['seconds_remaining'] >= old_row['start_seconds_in_period']) &
            (period_matches['seconds_remaining'] <= old_row['end_seconds_in_period'])
        ]
        
        # If we found matches, take the first action number
        # This assumes the first action in that timeframe corresponds to the event
        if len(time_matches) > 0:
            old_df_with_action.at[idx, 'actionNumber'] = time_matches['actionNumber'].iloc[0]
            
            # Alternative: If you want to store all possible action numbers for this timeframe
            # old_df_with_action.at[idx, 'actionNumber'] = time_matches['actionNumber'].tolist()
    
    return old_df_with_action

def advanced_map_action_numbers(old_df, new_df):
    """
    A more sophisticated mapping that tries to match events based on descriptions
    in addition to timing information.
    
    Parameters:
    -----------
    old_df : pandas.DataFrame
        The old NBA play-by-play dataset
    new_df : pandas.DataFrame
        The new NBA play-by-play dataset with actionNumber
        
    Returns:
    --------
    pandas.DataFrame
        Old dataset with mapped actionNumber from new dataset
    """
    # First get the basic time-based mapping
    old_df_with_action = map_action_numbers(old_df, new_df)
    
    # Now try to improve the mapping using event descriptions
    for idx, old_row in old_df_with_action.iterrows():
        # Skip if we already have a match
        if not pd.isna(old_row['actionNumber']):
            continue
            
        # Try to find a match based on event description
        period_matches = new_df[new_df['period'] == old_row['PERIOD']]
        
        # Extract key info from descriptions
        old_desc = old_row['DESCRIPTION'].lower() if not pd.isna(old_row['DESCRIPTION']) else ""
        
        # Look for potential matches in description
        for _, new_row in period_matches.iterrows():
            new_desc = new_row['description'].lower() if not pd.isna(new_row['description']) else ""
            
            # Check for common patterns in descriptions
            # E.g., player names, shot types, points, etc.
            if old_desc and new_desc:
                # Extract player names and actions
                # This is a simplified approach - a more robust solution would use NLP
                common_words = set(old_desc.split()).intersection(set(new_desc.split()))
                
                # If there are enough common words, consider it a match
                if len(common_words) >= 2:  # Arbitrary threshold, adjust as needed
                    old_df_with_action.at[idx, 'actionNumber'] = new_row['actionNumber']
                    break
    
    return old_df_with_action

def match_by_game_events(old_df, new_df):
    """
    Additional approach: try to align events sequence by sequence within games.
    This is useful when the datasets are from the same games but with different time formats.
    
    Parameters:
    -----------
    old_df : pandas.DataFrame
        The old NBA play-by-play dataset
    new_df : pandas.DataFrame
        The new NBA play-by-play dataset with actionNumber
        
    Returns:
    --------
    pandas.DataFrame
        Old dataset with mapped actionNumber from new dataset
    """
    # Group by game ID
    old_games = old_df.groupby('GAMEID')
    
    # Prepare result DataFrame
    result_df = old_df.copy()
    result_df['actionNumber'] = None
    
    # Process each game
    for game_id, old_game_df in old_games:
        # Find corresponding data in new dataset
        new_game_df = new_df[new_df['game_id'] == game_id]
        
        # If no matching game, continue
        if len(new_game_df) == 0:
            continue
            
        # Sort both datasets by period and time
        old_game_df = old_game_df.sort_values(['PERIOD', 'start_seconds'])
        new_game_df = new_game_df.sort_values(['period', 'timeActual'])
        
        # Try to align sequences of events
        # This is a simplified approach - in practice, you would need more sophisticated alignment
        
        # Example: Align by event type and player involvement
        # (This would need customization based on your specific data)
        
        # Update the result DataFrame
        for idx, old_row in old_game_df.iterrows():
            # Find matching action in new_game_df
            # ... (implementation depends on specific matching logic)
            
            # For demonstration, just map sequentially (this is oversimplified)
            period_matches = new_game_df[new_game_df['period'] == old_row['PERIOD']]
            if len(period_matches) > 0:
                # Get the first unmatched action for this period
                action_num = period_matches['actionNumber'].iloc[0]
                result_df.loc[idx, 'actionNumber'] = action_num
                
                # Remove this action from consideration for next matches
                new_game_df = new_game_df[new_game_df['actionNumber'] != action_num]
    
    return result_df

# Main function to use all approaches
def get_action_numbers(old_df, new_df):
    """
    Map actionNumber from the new dataset to the old dataset using multiple approaches.
    
    Parameters:
    -----------
    old_csv_path : str
        Path to the old dataset CSV file
    new_csv_path : str
        Path to the new dataset CSV file
        
    Returns:
    --------
    pandas.DataFrame
        Old dataset with mapped actionNumber from new dataset
    """
    # Load datasets

    
    # First try the time-based mapping approach
    result_df = map_action_numbers(old_df, new_df)
    
    # Check how many rows got mapped
    mapped_count = result_df['actionNumber'].notna().sum()
    total_count = len(result_df)
    
    print(f"Time-based mapping: {mapped_count}/{total_count} rows mapped ({mapped_count/total_count:.1%})")
    
    # If the mapping is not satisfactory, try the description-based approach
    if mapped_count / total_count < 0.5:  # Arbitrary threshold
        result_df = advanced_map_action_numbers(old_df, new_df)
        
        mapped_count = result_df['actionNumber'].notna().sum()
        print(f"Description-based mapping: {mapped_count}/{total_count} rows mapped ({mapped_count/total_count:.1%})")
    
    # If the datasets are from the same games but time formats differ, try sequence-based
    # This approach may be less accurate but provides a fallback
    if mapped_count / total_count < 0.3:  # Arbitrary threshold
        result_df = match_by_game_events(old_df, new_df)
        
        mapped_count = result_df['actionNumber'].notna().sum()
        print(f"Sequence-based mapping: {mapped_count}/{total_count} rows mapped ({mapped_count/total_count:.1%})")
    
    return result_df
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


    result_frames.append(new_df)


# In[2]:


all_missing=pd.concat(result_frames)

print(all_missing.columns)


# In[3]:


# Convert unhashable columns to strings
all_missing = all_missing.applymap(lambda x: str(x) if isinstance(x, list) else x)
print(len(all_missing))
# Drop duplicates
all_missing = all_missing.drop_duplicates()
print(len(all_missing))

all_missing.to_csv('all_missing.csv',index=False)


# In[4]:


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
'''
result_df = ping_nba_urls(all_missing)

# Save the results to a CSV or inspect
result_df.to_csv("nba_ping_results.csv", index=False)
print(result_df.head())
'''

