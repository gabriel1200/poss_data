#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
import numpy as np
import json
import time
import datetime
import os
from datetime import timedelta
import sys
games_url ='https://raw.githubusercontent.com/gabriel1200/shot_data/refs/heads/master/game_dates.csv'
games = pd.read_csv(games_url)
def get_date_ranges(start_date, end_date):
    """Generate 7-day date ranges between start and end dates."""
    start = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    
    date_ranges = []
    current = start
    
    while current < end:
        range_end = min(current + timedelta(days=6), end)
        date_ranges.append((
            current.strftime('%Y-%m-%d'),
            range_end.strftime('%Y-%m-%d')
        ))
        current = range_end + timedelta(days=1)
    
    return date_ranges
def determine_season(date_str):
    """Determine the season based on a date string."""
    year = int(date_str[:4])
    month = int(date_str[5:7])
    
    if month >= 9:  # New season starts around October
        return f"{year}-{str(year+1)[-2:]}"
    else:
        return f"{year-1}-{str(year)[-2:]}"
def fetch_possessions(team, start_date, end_date):
    """Fetch both offensive and defensive possessions for a team in the given date range."""
    team_dict = get_team_dict()
    season = determine_season(start_date)
    url = "https://api.pbpstats.com/get-possessions/nba"
    
    all_possessions = []
    
    # Fetch offensive possessions
    params = {
        "league": 'nba',
        "TeamId": team_dict[team],
        "Season": season,
        "SeasonType": "All",
        "OffDef": "Offense",
        "StartType": "All",
        "FromDate": start_date,
        "ToDate": end_date,
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        response_json = response.json()
        offensive_possessions = response_json.get("possessions", [])
        
        # Add team info to each possession
        for possession in offensive_possessions:
            possession['Team'] = team
            possession['IsOffense'] = True
        
        all_possessions.extend(offensive_possessions)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching offensive possessions for {team}: {e}")
    
    # Fetch defensive possessions
    '''
    params['OffDef'] = "Defense"
    time.sleep(2)
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        response_json = response.json()
        defensive_possessions = response_json.get("possessions", [])
        
        # Add team info to each possession
        for possession in defensive_possessions:
            possession['Team'] = team
            possession['IsOffense'] = False
        
        all_possessions.extend(defensive_possessions)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching defensive possessions for {team}: {e}")
    '''
    
    print(f"Fetched {len(all_possessions)} possessions for {team} from {start_date} to {end_date}")
    return all_possessions
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

def get_latest_date(file_path):
    """Get the latest date from CSV where at least one non-NaN URL exists."""
    try:
        df = pd.read_csv(file_path)
        
        # Check required columns exist
        required_columns = {'GAMEDATE', 'URL'}
        if not required_columns.issubset(df.columns):
            return None
        
        # Filter rows with valid URLs
        valid_urls = df[df['URL'].notna()]
        
        if valid_urls.empty:
            return None
            
        return valid_urls['GAMEDATE'].max()
        
    except (FileNotFoundError, pd.errors.EmptyDataError):
        return None


def convert_new_to_old_format(possession_data, team_id):
    """Convert new possession data format to match the old CSV structure."""
    converted_data = []
    
    for possession in possession_data:
        # Extract game teams from the GameId
        game_id = possession.get('GameId', '')
        game_id = game_id[2:]
   
      
        # Convert events to string
        events = ','.join(possession.get('Events', [])) if isinstance(possession.get('Events'), list) else str(possession.get('Events', ''))
        
        # Get video data
        video_data = possession.get('VideoUrls', [])
        
        if isinstance(video_data, list) and video_data:
            # Create a row for each video description/url pair
            for video_item in video_data:
                row = {
                    'ENDTIME': possession.get('EndTime', ''),
                    'EVENTS': events,
                    'FG2A': possession.get('FG2A', 0),
                    'FG2M': possession.get('FG2M', 0),
                    'FG3A': possession.get('FG3A', 0),
                    'FG3M': possession.get('FG3M', 0),
                    'GAMEDATE': possession.get('GameDate', ''),
                    'GAMEID': game_id,
                    'NONSHOOTINGFOULSTHATRESULTEDINFTS': possession.get('NonShootingFoulsThatResultedInFts', 0),
                    'OFFENSIVEREBOUNDS': possession.get('OffensiveRebounds', 0),
                    'OPPONENT': possession.get('Opponent', ''),
                    'PERIOD': possession.get('Period', ''),
                    'SHOOTINGFOULSDRAWN': possession.get('ShootingFoulsDrawn', 0),
                    'STARTSCOREDIFFERENTIAL': possession.get('StartScoreDifferential', 0),
                    'STARTTIME': possession.get('StartTime', ''),
                    'STARTTYPE': possession.get('StartType', ''),
                    'TURNOVERS': possession.get('Turnovers', 0),
                    'DESCRIPTION': video_item.get('description', ''),
                    'URL': video_item.get('url', np.nan) if video_item.get('url') else np.nan,
                    'team': possession.get('Team', ''),
                    'TEAM_ID': team_id
                }
                converted_data.append(row)
        else:
            # If no video data, create a single row with empty description and URL
            row = {
                'ENDTIME': possession.get('EndTime', ''),
                'EVENTS': events,
                'FG2A': possession.get('FG2A', 0),
                'FG2M': possession.get('FG2M', 0),
                'FG3A': possession.get('FG3A', 0),
                'FG3M': possession.get('FG3M', 0),
                'GAMEDATE': possession.get('GameDate', ''),
                'GAMEID': game_id,
                'NONSHOOTINGFOULSTHATRESULTEDINFTS': possession.get('NonShootingFoulsThatResultedInFts', 0),
                'OFFENSIVEREBOUNDS': possession.get('OffensiveRebounds', 0),
                'OPPONENT': possession.get('Opponent', ''),
                'PERIOD': possession.get('Period', ''),
                'SHOOTINGFOULSDRAWN': possession.get('ShootingFoulsDrawn', 0),
                'STARTSCOREDIFFERENTIAL': possession.get('StartScoreDifferential', 0),
                'STARTTIME': possession.get('StartTime', ''),
                'STARTTYPE': possession.get('StartType', ''),
                'TURNOVERS': possession.get('Turnovers', 0),
                'DESCRIPTION': '',
                'URL': np.nan,
                'team': possession.get('Team', ''),
                'TEAM_ID': team_id
            }
            converted_data.append(row)
    
    return converted_data

def update_team_possessions(team, season, base_dir='nba_possessions_data'):
    print(season)
    season_str = str(season-1)+'-'+str(season)[-2:]
    team_games = games[games['team']==team]
    team_games = team_games[team_games['season']==season_str]
    #team_games['GAME_ID'] = '00'+team_games['GAME_ID'].astype(str)
    #print(team_games.head())
    team_games = team_games[['GAME_ID','VTM','HTM']]
    team_games.columns = ['GAMEID','VTM','HTM']
    team_games['GAMEID'] = team_games['GAMEID'].astype(str)
    team_games.drop_duplicates(subset='GAMEID',inplace=True)
    #print(team_games.head())
    """Update possession data for a specific team from their last recorded date."""
    team_dict = get_team_dict()
    team_id = team_dict[team]
    
    # Construct file path and create directories if needed
    season_dir = os.path.join(base_dir, str(season))
    os.makedirs(season_dir, exist_ok=True)
    file_path = os.path.join(season_dir, f"{season}_{team}_possessions.csv")
    
    # Get the latest date from existing file
    start_date = get_latest_date(file_path)
    if start_date is None:
        start_date = f"{season-1}-10-22"  # Season start date if no file exists
    else:
        # Add one day to the latest date to avoid duplicates
        start_date = (pd.to_datetime(start_date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Set end date to current date
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    
    print(f"Updating {team} data from {start_date} to {end_date}")
    
    # Fetch new possessions
    date_ranges = get_date_ranges(start_date, end_date)
    new_possessions = []
    
    for start, end in date_ranges:
        possessions = fetch_possessions(team, start, end)
        if possessions:
            converted_possessions = convert_new_to_old_format(possessions, team_id)
            new_possessions.extend(converted_possessions)
        time.sleep(2)  # API rate limiting
    
    if new_possessions:
        new_df = pd.DataFrame(new_possessions)
        new_df = new_df.merge(team_games, on='GAMEID', how='left')

        
         
        # If file exists, append new data; otherwise create new file
        if os.path.exists(file_path):
            existing_df = pd.read_csv(file_path)
            existing_df=existing_df[existing_df.GAMEDATE<=start_date]
            #print(new_df.columns)
            #print(existing_df.columns)
            updated_df = pd.concat([existing_df, new_df], ignore_index=True)
           
            # Remove duplicates based on all columns except index
            for col in updated_df.columns:

            
                updated_df[col] = updated_df[col].astype(existing_df[col].dtype)
                
 
        
          
         
                    
            updated_df.drop_duplicates(inplace=True)
            
            # Sort by date and time
            updated_df = updated_df.sort_values(['GAMEDATE', 'PERIOD', 'STARTTIME'])
        else:
            updated_df = new_df

        
        updated_df.to_csv(file_path, index=False)
        print(f"Added {len(new_possessions)} new possessions to {file_path}")
    else:
        print(f"No new possessions found for {team}")

def update_all_teams(season=2025, base_dir='nba_possessions_data'):
    """Update possession data for all teams."""
    teams = list(get_team_dict().keys())
    
    for team in teams:
        print(f"\nProcessing {team}...")
        update_team_possessions(team, season, base_dir)
        time.sleep(2)  # Delay between teams

if __name__ == "__main__":
    season = 2025  # Current season
    print(f"Starting NBA possession data updater for {season} season...")
    update_all_teams(season)

