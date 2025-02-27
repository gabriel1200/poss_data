#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
import sys
from collections import defaultdict

import requests
import pandas as pd
import numpy as np
import json
import time

import os

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
def get_team_id_dict():
    """Returns a dictionary mapping team IDs to team abbreviations."""
    return {
        '1610612737': 'ATL', '1610612751': 'BKN', '1610612738': 'BOS', '1610612766': 'CHA',
        '1610612741': 'CHI', '1610612739': 'CLE', '1610612742': 'DAL', '1610612743': 'DEN',
        '1610612765': 'DET', '1610612744': 'GSW', '1610612745': 'HOU', '1610612754': 'IND',
        '1610612746': 'LAC', '1610612747': 'LAL', '1610612763': 'MEM', '1610612748': 'MIA',
        '1610612749': 'MIL', '1610612750': 'MIN', '1610612740': 'NOP', '1610612752': 'NYK',
        '1610612760': 'OKC', '1610612753': 'ORL', '1610612755': 'PHI', '1610612756': 'PHX',
        '1610612757': 'POR', '1610612758': 'SAC', '1610612759': 'SAS', '1610612761': 'TOR',
        '1610612762': 'UTA', '1610612764': 'WAS'
    }

def get_id_to_team_abbrev():
    """Returns a dictionary mapping team IDs to team abbreviations."""
    team_dict = get_team_dict()
    return {v: k for k, v in team_dict.items()}

def convert_time_to_seconds(period, time_str):
    """Convert period and MM:SS format to total game seconds"""
    minutes, seconds = map(int, time_str.split(':'))
    
    # Calculate total seconds for all previous periods
    if period <= 4:
        period_seconds = (period - 1) * 720  # 12-minute periods
    else:
        period_seconds = 4 * 720 + (period - 5) * 300  # 5-minute OT periods
    
    # Calculate time passed in current period (counting down)
    #print(period_seconds)
    current_period_length = 720 if period <= 4 else 300
    time_passed = current_period_length - (minutes * 60 + seconds)
    
    return period_seconds + time_passed

def main(year=2025,ps=False):
    if ps:
        trail = 'ps'
    else:
        trail = ''
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='player_tracking_log.txt',
        filemode='w'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    
    ogyear = year
    season = str(year-1)+'-'+str(year)[-2:]
    logging.info("Starting player tracking process")
    
    # Get team ID to abbreviation mapping
    id_to_abbrev = get_id_to_team_abbrev()
    
    # Load the full datasets
    logging.info("Loading rotation and clips data")
    try:
        rotations_df = pd.read_csv(f"{year}{trail}_rotations.csv")
        clips_dir = f"nba_possessions_data/{year}"
        clips_files = [f for f in os.listdir(clips_dir) if f.startswith(str(year)) and f.endswith(".csv")]
        if ps:
            clips_files = [f for f in clips_files if 'ps' in f]
        else:
            clips_files = [f for f in clips_files if 'ps' not in f]
        print('There are',len(clips_files),'clip files')

        if not clips_files:
            logging.error(f"No clip files found in {clips_dir} starting with {year}")
            return None

        clips_df_list = []
        for file in clips_files:
            try:
                file_path = os.path.join(clips_dir, file)
                clips_df_list.append(pd.read_csv(file_path))
                logging.info(f"Loaded clip file: {file}")
            except Exception as e:
                logging.error(f"Error loading clip file {file}: {str(e)}")
                continue  # Skip to the next file if there's an error

        if not clips_df_list:
            logging.error("No clip data loaded successfully.")
            return None

        clips_df = pd.concat(clips_df_list, ignore_index=True)        

        # Convert team IDs to strings
        rotations_df['TEAM_ID'] = rotations_df['TEAM_ID'].astype(str)
        clips_df['TEAM_ID'] = clips_df['TEAM_ID'].astype(str)
        
        # Process rotation times (vectorized)
        rotations_df['IN_TIME_SEC'] = rotations_df['IN_TIME_REAL'] / 10
        rotations_df['OUT_TIME_SEC'] = rotations_df['OUT_TIME_REAL'] / 10
        
        logging.info(f"Loaded {len(rotations_df)} rotation records and {len(clips_df)} clip records")
    except Exception as e:
        logging.error(f"Error loading data: {str(e)}")
        return None
    
    # Extract year from GameDate (vectorized)
    clips_df['Year'] = pd.to_datetime(clips_df['GAMEDATE']).dt.year
    
    # Add game_seconds columns (vectorized using apply)
    logging.info("Converting game times to seconds")
    clips_df['start_seconds'] = clips_df.apply(
        lambda row: convert_time_to_seconds(int(row['PERIOD']), row['STARTTIME']), axis=1
    )
    clips_df['end_seconds'] = clips_df.apply(
        lambda row: convert_time_to_seconds(int(row['PERIOD']), row['ENDTIME']), axis=1
    )
    clips_df['mid_seconds'] = (clips_df['start_seconds'] + clips_df['end_seconds']) / 2
    
    # Create output directory if it doesn't exist
    output_dir = str(year) # year of the season end
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")
    
    # Pre-process rotation data to create a more efficient lookup structure
    logging.info("Creating rotation lookup structure")
    rotation_lookup = {}
    
    # Group rotations by game_id and team_id
    for game_id in rotations_df['GAME_ID'].unique():
        game_rotations = rotations_df[rotations_df['GAME_ID'] == game_id]
        rotation_lookup[game_id] = {}
        
        for team_id in game_rotations['TEAM_ID'].unique():
            team_rotations = game_rotations[game_rotations['TEAM_ID'] == team_id]
            rotation_lookup[game_id][team_id] = []
            
            for _, player_row in team_rotations.iterrows():
                rotation_lookup[game_id][team_id].append({
                    'player_id': str(player_row['PERSON_ID']),
                    'in_time': player_row['IN_TIME_SEC'],
                    'out_time': player_row['OUT_TIME_SEC']
                })
    
    # Function to get players on court using the efficient lookup structure
    def get_players_on_court(team_id, time_sec, game_id):
        if game_id not in rotation_lookup or team_id not in rotation_lookup[game_id]:
            return []
        
        on_court = []
        for player_data in rotation_lookup[game_id][team_id]:
            if player_data['in_time'] <= time_sec < player_data['out_time']:
                on_court.append(player_data['player_id'])
        
        if len(on_court) != 5:
   
            logging.warning(f"Found {len(on_court)} players for team {team_id} at time {time_sec} in game {game_id}. Expected 5 players.")
            if len(on_court)>=5:
                    '''
                    for player_data in rotation_lookup[game_id][team_id]:
                        print(player_data)
                    sys.exit()       
                    '''
                    pass      
            
        return on_court
    
    # Process each team in parallel
    teams = clips_df['TEAM_ID'].unique()
    years = clips_df['Year'].unique()
    print(teams)
  
    logging.info(f"Processing data for {len(teams)} teams across {len(years)} years")
    
    for team in teams:
        team_abbrev = id_to_abbrev.get(team, team)  # Get team abbreviation or use ID if not found
        team_clips = clips_df[clips_df['TEAM_ID'] == team]

        team_clips.drop_duplicates(inplace=True)
        team_clips.sort_values(by='GAMEID',inplace=True)
        print(len(team_clips))
        
        if len(team_clips) == 0:
            logging.info(f"No clips found for {team_abbrev}, skipping")
            continue
        
        logging.info(f"Processing {len(team_clips)} clips for {team_abbrev}")
        
        # Process players on court for all clips at once
        players_on_list = []
        opp_players_on_list = []
        
        # Group by game to minimize lookups
        for game_id, game_clips in team_clips.groupby('GAMEID'):
            if game_id not in rotation_lookup:
                logging.warning(f"Game {game_id} not found in rotation data")
                players_on_list.extend(["GAME_NOT_FOUND"] * len(game_clips))
                opp_players_on_list.extend(["GAME_NOT_FOUND"] * len(game_clips))
                continue
            
            game_teams = list(rotation_lookup[game_id].keys())
            if len(game_teams) < 2:
                logging.warning(f"Not enough teams found for game {game_id}")
                players_on_list.extend(["TEAMS_NOT_FOUND"] * len(game_clips))
                opp_players_on_list.extend(["TEAMS_NOT_FOUND"] * len(game_clips))
                continue
            
            # Determine the opponent's team ID
            opponent_team_id = None
            for t_id in game_teams:
                if t_id != team:
                    opponent_team_id = t_id
                    break
            # Add these debug prints:
    
            if opponent_team_id is None:
                logging.warning(f"Opponent team not found for game {game_id}")
                players_on_list.extend(["OPP_TEAM_NOT_FOUND"] * len(game_clips))
                opp_players_on_list.extend(["OPP_TEAM_NOT_FOUND"] * len(game_clips))
                continue
            
            for _, row in game_clips.iterrows():
                try:
                    mid_time = row['mid_seconds']
                    orig_team = get_team_id_dict()[team]
                    opponent_team = get_team_id_dict()[opponent_team_id]
                    print(orig_team)
                    print(opponent_team)
                    print(game_id)
                        #sys.exit()
                    offensive_players = get_players_on_court(team, mid_time, game_id)
                    print(offensive_players)
                    defensive_players = get_players_on_court(opponent_team_id, mid_time, game_id)
                    print(defensive_players)
                    sys.exit()
                    players_on_list.append('|'.join(offensive_players))
                    opp_players_on_list.append('|'.join(defensive_players))
                except Exception as e:
                    logging.error(f"Error processing possession: {str(e)}")
                    players_on_list.append("ERROR")
                    opp_players_on_list.append("ERROR")
                
        print(players_on_list)
        print(opp_players_on_list)  
           
      
        
        # Add players_on column
        team_clips['players_on'] = players_on_list
        team_clips['opp_players_on'] = opp_players_on_list
        team_clips['season'] = season
        
        # Save results using team abbreviation
        output_file = f"{output_dir}/{team_abbrev}_{ogyear}{trail}_clips_with_players.csv"
        print(output_file)
        team_clips.to_csv(output_file, index=False)
        
        # Log statistics
        error_count = team_clips[team_clips['players_on'].isin(['ERROR', 'GAME_NOT_FOUND', 'TEAMS_NOT_FOUND', 'OPP_TEAM_NOT_FOUND'])].shape[0]
        success_count = team_clips.shape[0] - error_count
        logging.info(f"Processed {team_clips.shape[0]} clips for {team_abbrev}. "
                     f"Success: {success_count} ({success_count/team_clips.shape[0]*100:.1f}%), "
                     f"Errors: {error_count} ({error_count/team_clips.shape[0]*100:.1f}%)")
    
    logging.info("Processing complete")
    return True

if __name__ == "__main__":
    start_time = datetime.now()
    logging.info(f"Script started at {start_time}")
    
    print('starting')
    result = main(year=2025,ps=False)
    
    
    end_time = datetime.now()
    execution_time = end_time - start_time
    logging.info(f"Script completed at {end_time}")
    logging.info(f"Total execution time: {execution_time}")

