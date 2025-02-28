#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import glob
import os
import sys
def process_flip_files(ps = False):

    year_dirs = [d for d in os.listdir() if d.isdigit()]
    year_dirs = [d for d in year_dirs if '2025' in d]
    print(year_dirs)

    # Process each year directory
    for year in year_dirs:
        all_dfs = []
        # Find all CSV files in the year directory
        files = glob.glob(os.path.join(year, "*_clips_with_players.csv"))

        if ps:
            files = [f for f in files if f"{year}ps" in str(f)]
            trail = 'ps'
        else:
            files = [f for f in files if f"{year}ps"  not in str(f)]
            trail = ''
        files = [file for file in files if 'vs' not in str(file)]
        print(len(files))
        print(trail)
        
        # Read each file and append to our list
        for file in files:
            try:
                df = pd.read_csv(file)
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
    
    # Combine all DataFrames
        if not all_dfs:
            print(year)
            print("No data files found!")
            return
            
        all_data = pd.concat(all_dfs, ignore_index=True)
        
        # Get unique opponents
        opponents = all_data['OPPONENT'].unique()
        
        # Create 'processed' directory if it doesn't exist
        if not os.path.exists('processed'):
            os.makedirs('processed')
        
        # Create a new CSV for each opponent
        for opponent in opponents:
            # Filter data for this opponent
            opponent_data = all_data[all_data['OPPONENT'] == opponent]

            opponent_data = opponent_data.rename(columns={'players_on': 'opp_players_on', 'opp_players_on': 'players_on'})

            # Create filename - replace any spaces or special characters
            filename = f"{year}/{opponent}_vs_{year}{trail}_clips_with_players.csv"
            
            # Save to CSV
            opponent_data.to_csv(filename, index=False)
            print(f"Created {filename}")

if __name__ == "__main__":
    process_flip_files(ps=False)

