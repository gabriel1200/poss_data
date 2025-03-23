import pandas as pd
import asyncio
import aiohttp
import time
import json
import os
from tqdm.auto import tqdm
import hashlib
import logging
from concurrent.futures import ProcessPoolExecutor
import sys
# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='nba_scraper.log'
)

# Cache management functions
class RequestCache:
    def __init__(self, cache_dir='request_cache'):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
    
    def _get_cache_key(self, url):
        return hashlib.md5(url.encode()).hexdigest()
    
    def _get_cache_path(self, key):
        return os.path.join(self.cache_dir, f"{key}.json")
    
    def get(self, url):
        key = self._get_cache_key(url)
        path = self._get_cache_path(key)
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def set(self, url, data):
        key = self._get_cache_key(url)
        path = self._get_cache_path(key)
        with open(path, 'w') as f:
            json.dump(data, f)

# Function to process batches of rows
async def process_batch(session, batch, cache, base_url, headers, semaphore):
    tasks = []
    for _, row in batch.iterrows():
        tasks.append(fetch_url(session, row, cache, base_url, headers, semaphore))
    
    return await asyncio.gather(*tasks)

# Function to fetch a single URL
async def fetch_url(session, row, cache, base_url, headers, semaphore):
    async with semaphore:  # Control concurrency
        action_number = row['actionNumber']
        if pd.isna(action_number):
            return None
            
        action_number = int(action_number)
        game_id = row['game_id']
        description = row.get('description', '')
        url = f"{base_url}?GameEventID={action_number}&GameID=00{game_id}"
        
        # Check cache first
        cached_result = cache.get(url)
        if cached_result:
            cached_result.update({
                "game_id": game_id,
                "action_number": action_number,
                "description": description,
                "cached": True
            })
            return cached_result
        
        # If not in cache, make the request
        try:
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    playlist = data['resultSets']['Meta']['videoUrls']
                    video_link = playlist[0]['surl'] if len(playlist) > 0 else None
                    
                    result = {
                        "game_id": game_id,
                        "action_number": action_number,
                        "status": "Success",
                        "description": description,
                        "url": video_link,
                        "cached": False
                    }
                    
                    # Save to cache
                    cache.set(url, result)
                    return result
                else:
                    return {
                        "game_id": game_id,
                        "action_number": action_number,
                        "status": f"Failed: {response.status}",
                        "cached": False
                    }
        except Exception as e:
            logging.error(f"Error fetching {url}: {str(e)}")
            return {
                "game_id": game_id,
                "action_number": action_number,
                "status": f"Error: {str(e)}",
                "cached": False
            }

# Main scraper function
async def ping_nba_urls_async(df, batch_size=500, max_concurrent=100):
    base_url = "https://stats.nba.com/stats/videoeventsasset"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "https://www.nba.com",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.nba.com"
    }
    
    # Initialize cache
    cache = RequestCache()
    
    # Drop duplicate action numbers to avoid unnecessary requests
    logging.info(f"Original dataset size: {len(df)}")
    df = df.drop_duplicates(subset=['actionNumber', 'game_id'])
    df = df.dropna(subset=['actionNumber'])
    logging.info(f"After deduplication: {len(df)}")
    
    # Convert actionNumber to int for consistency
    df['actionNumber'] = df['actionNumber'].astype(int)
    
    # Split the dataframe into batches
    batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
    logging.info(f"Split into {len(batches)} batches of size {batch_size}")
    
    # Prepare session with optimized settings
    connector = aiohttp.TCPConnector(limit=max_concurrent, force_close=True)
    timeout = aiohttp.ClientTimeout(total=30)
    
    # Create a semaphore to limit concurrency
    semaphore = asyncio.Semaphore(max_concurrent)
    
    results = []
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        # Process batches with progress bar
        for i, batch in enumerate(tqdm(batches, desc="Processing batches")):
            logging.info(f"Processing batch {i+1}/{len(batches)}")
            
            # Process batch
            batch_results = await process_batch(session, batch, cache, base_url, headers, semaphore)
            
            # Filter None results and add to results list
            filtered_results = [r for r in batch_results if r is not None]
            results.extend(filtered_results)
            
            # Small delay between batches to avoid overwhelming the server
            if i < len(batches) - 1:
                await asyncio.sleep(0.5)
    
    # Convert results to DataFrame
    result_df = pd.DataFrame(results)
    return result_df

# Checkpoint function to save progress
def save_checkpoint(df, filename="nba_scrape_checkpoint.csv"):
    df.to_csv(filename, index=False)
    logging.info(f"Checkpoint saved: {filename}")

# Main execution function with retry logic
async def main(df, retries=3):
    start_time = time.time()
    
    for attempt in range(retries):
        try:
            logging.info(f"Starting scrape attempt {attempt+1}/{retries}")
            result_df = await ping_nba_urls_async(df)
            
            # Check for failures that might need retry
            failures = result_df[result_df['status'].str.startswith('Failed') | 
                                result_df['status'].str.startswith('Error')]
            
            if len(failures) > 0:
                logging.warning(f"Found {len(failures)} failed requests")
                
                # If this is the last attempt, just return what we have
                if attempt == retries - 1:
                    logging.info("Max retries reached, returning partial results")
                    break
                
                # Otherwise, retry just the failures
                game_action_pairs = failures[['game_id', 'action_number']].drop_duplicates()
                retry_df = pd.merge(df, game_action_pairs, 
                                   left_on=['game_id', 'actionNumber'], 
                                   right_on=['game_id', 'action_number'])
                
                logging.info(f"Retrying {len(retry_df)} requests")
                retry_results = await ping_nba_urls_async(retry_df)
                
                # Replace failures with retry results
                result_df = pd.concat([
                    result_df[~result_df['action_number'].isin(failures['action_number'])],
                    retry_results
                ])
            
            # Save final results
            save_checkpoint(result_df, "nba_ping_results_final.csv")
            break
            
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed with error: {str(e)}")
            if attempt == retries - 1:
                raise e
            await asyncio.sleep(5)  # Wait a bit before retrying
    
    elapsed = time.time() - start_time
    
    logging.info(f"Processed {len(result_df)} rows in {elapsed:.2f} seconds")
    logging.info(f"Average time per request: {elapsed/len(result_df):.4f} seconds")
    
    # Print summary statistics
    success_count = len(result_df[result_df['status'] == 'Success'])
    failure_count = len(result_df) - success_count
    cached_count = len(result_df[result_df.get('cached', False) == True])
    
    print(f"Total requests: {len(result_df)}")
    print(f"Successful: {success_count} ({success_count/len(result_df)*100:.1f}%)")
    print(f"Failed: {failure_count} ({failure_count/len(result_df)*100:.1f}%)")
    print(f"Cached hits: {cached_count} ({cached_count/len(result_df)*100:.1f}%)")
    print(f"Total time: {elapsed:.2f} seconds")
    print(f"Average time per request: {elapsed/len(result_df)*1000:.2f} ms")
    
    return result_df

# Run the async function
if __name__ == "__main__":
    # Assuming all_missing dataframe is already loaded
    all_missing=pd.read_csv('all_missing.csv')
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main(all_missing))