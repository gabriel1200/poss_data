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
import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher
def map_action_numbers(old_df, new_df, description_weight=0.25, time_weight=0.05, score_weight=0.7):

    """
    Map actionNumber from new_df to the appropriate rows in old_df based on:
    1. Time ranges and periods
    2. Text similarity between descriptions
    3. Score differential matching
    4. Special handling for missed shot + rebound sequences
    
    Parameters:
    old_df (pandas.DataFrame): The old dataset with start_seconds, end_seconds, DESCRIPTION, and STARTSCOREDIFFERENTIAL columns
    new_df (pandas.DataFrame): The new dataset with actionNumber, period, clock, description, scoreHome, and scoreAway columns
    description_weight (float): Weight given to description similarity
    time_weight (float): Weight given to time proximity
    score_weight (float): Weight given to score differential matching
    
    Returns:
    pandas.DataFrame: A copy of old_df with a new column 'actionNumber' added
    """
    import numpy as np
    import pandas as pd
    import re
    from difflib import SequenceMatcher
    
    # Make a copy of the old_df to avoid modifying the original
    result_df = old_df.copy()
    result_df.drop_duplicates(subset=['start_seconds','end_seconds','GAMEID','GAMEDATE'],inplace=True)
    
    # Initialize the actionNumber column with NaN
    result_df['actionNumber'] = np.nan
    
    # Convert data types to ensure proper comparison
    result_df['PERIOD'] = result_df['PERIOD'].astype(int)
    result_df['start_seconds'] = result_df['start_seconds'].astype(float)
    result_df['end_seconds'] = result_df['end_seconds'].astype(float)
    def extract_free_throws(description):
        if not isinstance(description, str):
            return 0
        
        # Pattern to match successful free throws with points scored
        pattern = r'Free Throw.*\((\d+) PTS\)'
        matches = re.findall(pattern, description)

        # Sum up all points from free throws made
        free_throw_points = sum(int(pts) for pts in matches)
        return free_throw_points

    # Apply the free throw extraction logic if SHOOTINGFOULSDRAWN == 1
    result_df['FREE_THROW_POINTS'] = result_df.apply(
        lambda row: extract_free_throws(row['DESCRIPTION']) if row.get('SHOOTINGFOULSDRAWN') == 1 else 0,
        axis=1
    )
    # Ensure STARTSCOREDIFFERENTIAL is available and numeric
    if 'STARTSCOREDIFFERENTIAL' in result_df.columns:
        result_df['STARTSCOREDIFFERENTIAL'] = pd.to_numeric(result_df['STARTSCOREDIFFERENTIAL'], errors='coerce')
        result_df['ENDSCOREDIFFERENTIAL'] = result_df['STARTSCOREDIFFERENTIAL'] + \
            result_df['FG2M'].fillna(False).astype(int) * 2 + \
            result_df['FG3M'].fillna(False).astype(int) * 3+ \
            result_df['FREE_THROW_POINTS']
    else:
        # If STARTSCOREDIFFERENTIAL is missing, we'll work without it
        result_df['ENDSCOREDIFFERENTIAL'] = np.nan
        score_weight = 0  # Zero out the score weight if we don't have score data
    
    new_df['period'] = new_df['period'].astype(int)
    new_df['actionNumber'] = new_df['actionNumber'].astype(int)
    
    # Calculate score differential for new_df if scoreHome and scoreAway are available
    if 'scoreHome' in new_df.columns and 'scoreAway' in new_df.columns:
        new_df['scoreHome'] = pd.to_numeric(new_df['scoreHome'], errors='coerce').fillna(0)
        new_df['scoreAway'] = pd.to_numeric(new_df['scoreAway'], errors='coerce').fillna(0)
        new_df['scoreDifferential'] = new_df['scoreHome'] - new_df['scoreAway']
    else:
        # If score columns are missing, we'll work without them
        new_df['scoreDifferential'] = np.nan
        score_weight = 0  # Zero out the score weight if we don't have score data
    
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
    new_df.drop_duplicates(subset=['game_id','period','actionType','game_seconds'],inplace=True)

    # Sort new_df by period and game_seconds
    new_df.sort_values(by=['period', 'game_seconds'], inplace=True)
    result_df.sort_values(by=['PERIOD', 'start_seconds'], inplace=True)

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
    
    # Enhanced extract features function with detailed pattern recognition
    def extract_features(description):
        if pd.isna(description):
            return {
                "player": "", 
                "action": "", 
                "points": "", 
                "is_missed_shot": False, 
                "is_rebound": False,
                "team": "",
                "shot_type": ""
            }
        
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
        
        # Extract shot type more specifically
        shot_types = ["3pt", "3-pt", "three point", "2pt", "2-pt", "two point", 
                      "free throw", "layup", "dunk", "jump shot", "hook shot", 
                      "floating jump", "driving layup", "step back"]
        shot_type = ""
        for st in shot_types:
            if st in description:
                shot_type = st
                break
        
        # Extract points if present
        points_match = re.search(r'\((\d+)\s*PTS\)', description)
        points = points_match.group(1) if points_match else ""
        
        # Flag missed shots with more patterns
        is_missed_shot = any(term in description for term in ["miss", "misses", "missed"])
        
        # Flag rebounds with more patterns
        is_rebound = any(term in description for term in ["rebound", "rebounds", "rebounded"])
        
        # Try to identify team
        teams = ["team", "offensive", "defensive", "off", "def"]
        team = ""
        for t in teams:
            if t in description:
                team = t
                break
        
        return {
            "player": player, 
            "action": action, 
            "points": points, 
            "is_missed_shot": is_missed_shot, 
            "is_rebound": is_rebound,
            "team": team,
            "shot_type": shot_type
        }
    
    # Improved function to find the action number with score differential and enhanced handling
    def find_action_number(row):
        period = row['PERIOD']
        start_time = row['start_seconds']
        end_time = row['end_seconds']
        old_description = row['DESCRIPTION'] if 'DESCRIPTION' in row else ""
        score_diff = row['ENDSCOREDIFFERENTIAL'] if 'ENDSCOREDIFFERENTIAL' in row else np.nan
        
        # Check if we have data for this period
        if period not in new_df_by_period:
            return np.nan
        
        period_data = new_df_by_period[period]
        
        # Extract features from the old description
        old_features = extract_features(old_description)
        
        # Determine if this is a rebound or missed shot for special handling
        is_rebound_in_old = old_features["is_rebound"]
        is_missed_shot_in_old = old_features["is_missed_shot"]
        
        # Define time window based on action type
        # For rebounds/missed shots, use a wider window as they happen in succession
        # For other actions, keep a tighter window
        base_time_window = 1.0  # base time window in seconds
        
        # Adjust window for special cases
        if is_rebound_in_old or is_missed_shot_in_old:
            time_window = 2.5  # much wider for these special cases
        else:
            time_window = base_time_window
        
        # Find actions that fall within the time range
        potential_matches = period_data[
            (period_data['game_seconds'] >= (start_time - time_window)) & 
            (period_data['game_seconds'] <= (end_time + time_window))
        ]

        if potential_matches.empty:
            return np.nan
        
        # If only one match, return it
        if len(potential_matches) == 1:
            return potential_matches['actionNumber'].iloc[0]
        
        # Calculate similarities and combine with time proximity and score matching
        best_match = None
        best_score = -1
        
        for idx, match in potential_matches.iterrows():
            # Extract features from the new description
            new_description = match['description'] if 'description' in match else ""
            new_features = extract_features(new_description)
            
            # 1. Time proximity score (1 for exact match, decreases with distance)
            time_diff = abs(match['game_seconds'] - (start_time + end_time)/2)
            time_proximity = max(0, 1 - (time_diff / (time_window * 2)))
            
            # 2. Score differential matching (1 for exact match, decreases with difference)
            score_match = 0
            if not pd.isna(score_diff) and not pd.isna(match.get('scoreDifferential', np.nan)):
                # Check if the absolute values of score differentials match
                abs_diff = abs(abs(score_diff) - abs(match['scoreDifferential']))
                # Perfect match gets 1.0, decreases as difference increases
                score_match = max(0, 1 - (abs_diff / 5.0))  # Dividing by 5 means a diff of 5+ points gets 0
            
            # 3. Description and feature-based similarity
            desc_similarity = calculate_description_similarity(old_description, new_description)
            
            # Base feature similarity
            feature_similarity = 0
            # Player match gives significant weight
            if old_features["player"] and old_features["player"] == new_features["player"]:
                feature_similarity += 0.5
            # Action type match gives decent weight
            if old_features["action"] and old_features["action"] == new_features["action"]:
                feature_similarity += 0.3
            # Points match gives some weight
            if old_features["points"] and old_features["points"] == new_features["points"]:
                feature_similarity += 0.2
            # Shot type match gives additional confidence
            if old_features["shot_type"] and old_features["shot_type"] == new_features["shot_type"]:
                feature_similarity += 0.2
            
            # 4. Special handling for missed shot-rebound sequences
            rebound_shot_bonus = 0
            
            # If timestamps are very close (within 2 seconds) and it's a shot-rebound pair
            if (is_rebound_in_old and new_features["is_missed_shot"]) or \
               (is_missed_shot_in_old and new_features["is_rebound"]):
                # For very close timestamps, give a significant bonus
                if time_diff < 1:
                    rebound_shot_bonus = 0.8 * (1 - time_diff/2.0)  # Scales from 0.8 to 0
                    
                    # If score also matches, this is almost certainly the right match
                    if score_match > 0.9:  # Very high score match
                        rebound_shot_bonus += 0.2  # Additional bonus for score confirmation
            
            # Get the higher of description or feature similarity
            similarity = max(desc_similarity, feature_similarity)
            
            # Add the rebound_shot_bonus
            similarity += rebound_shot_bonus
            
            # Normalize similarity to max of 1.0
            similarity = min(similarity, 1.0)
            
            # Combined weighted score
            combined_score = (description_weight * similarity) +  (time_weight * time_proximity) + (score_weight * score_match)
            
            if combined_score > best_score:
                best_score = combined_score
                best_match = match['actionNumber']
        
        return best_match
    
    # Apply the function to each row in the old dataset
    result_df['actionNumber'] = result_df.apply(find_action_number, axis=1)
    
    # Add a confidence score based on description similarity and special cases
    if 'DESCRIPTION' in result_df.columns and 'description' in new_df.columns:
        def calculate_confidence(row):
            if pd.isna(row['actionNumber']):
                return 0
                
            action_data = new_df[new_df['actionNumber'] == row['actionNumber']]
            if action_data.empty:
                return 0
            
            old_features = extract_features(row['DESCRIPTION'])
            new_description = action_data['description'].iloc[0] if 'description' in action_data else ""
            new_features = extract_features(new_description)
            
            # Basic similarity
            similarity = calculate_description_similarity(row['DESCRIPTION'], new_description)
            
            # Enhance confidence for missed shot-rebound pairs that were matched
            
            if (old_features["is_rebound"] and new_features["is_missed_shot"]) or \
               (old_features["is_missed_shot"] and new_features["is_rebound"]):
                #print(row)
                # Check time proximity
                if 'game_seconds' in action_data:
                    time_diff = abs(action_data['game_seconds'].iloc[0] - (row['start_seconds'] + row['end_seconds'])/2)
                    if time_diff < 2.0:  # Close in time
                        similarity = max(similarity, 0.8)  # Set a high minimum confidence
                        
                        # Check score match for even higher confidence
                        if not pd.isna(row['ENDSCOREDIFFERENTIAL']) and \
                           'scoreDifferential' in action_data and \
                           not pd.isna(action_data['scoreDifferential'].iloc[0]):
                            
                            abs_diff = abs(abs(row['ENDSCOREDIFFERENTIAL']) - 
                                          abs(action_data['scoreDifferential'].iloc[0]))
                            
                            if abs_diff < 1:  # Very close score match
                                similarity = max(similarity, 0.9)  # Almost certain match
                
            return similarity
            
        result_df['match_confidence'] = result_df.apply(calculate_confidence, axis=1)
    
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
missing_frame=pd.read_csv('all_missing.csv')
result_frames=[]
teams = [
    'ATL', 'BOS', 'BKN', 'CHA', 'CHI', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 
    'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 
    'OKC', 'ORL', 'PHI', 'PHX', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS'
]
#teams=['ATL','MIA']
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
    new_df=new_df[new_df.teamId==teamid]
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


import pandas as pd

# Load the CSV
action_map = pd.read_csv('nba_ping_results_final.csv')

# Filter by game_id
action_map = action_map[action_map.game_id == 22401062]

# Adjust display settings
pd.set_option('display.max_colwidth', None)  # Ensure full URL display

# Display the desired columns
print(action_map.head(40)[['description', 'url']])

