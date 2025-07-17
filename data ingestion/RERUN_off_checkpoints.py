#################################### Rerunning Boxscore Data Based Off Checkpoints ####################################
## Run this script if the RUN_boxscore script fails to finish and there are game ID checkpoints with game IDs already processed
import pandas as pd
import datetime
from time import sleep
from tqdm import tqdm
import logging
from config import (
    get_season_config,
    initialize_script_environment,
    ScriptPaths,
    get_all_game_ids,
    fetch_boxscores_by_game,
    retry_failed_boxscores
)

# Function to rerun boxscore data based on existing checkpoints
def get_processed_game_ids_from_checkpoints(script_env: ScriptPaths):
    processed_game_ids = set()
    logging.info("Checking for existing boxscore checkpoint files to identify processed game IDs...")
    
    # Look for any of the boxscore type checkpoint files
    # We can assume if one type of boxscore data is saved for a game, all types were attempted and saved.
    # Using 'boxscore_traditional_chunk_*.csv' as a representative.
    checkpoint_files = list(script_env.boxscore_checkpoints_dir.glob("boxscore_traditional_chunk_*.csv"))
    
    if not checkpoint_files:
        logging.info("No existing boxscore checkpoint files found. Starting from scratch.")
        return processed_game_ids

    for f in sorted(checkpoint_files): # Sort to process in order
        try:
            df = pd.read_csv(f, dtype={'GAME_ID': str}) # Explicitly read GAME_ID as string
            if 'GAME_ID' in df.columns:
                processed_game_ids.update(df['GAME_ID'].unique().tolist())
                logging.info(f"Loaded {len(df['GAME_ID'].unique())} game IDs from {f.name}")
            else:
                logging.warning(f"Checkpoint file {f.name} does not contain 'GAME_ID' column.")
        except Exception as e:
            logging.error(f"Error reading checkpoint file {f}: {e}")
            
    logging.info(f"Found {len(processed_game_ids)} game IDs already processed from checkpoints.")
    return processed_game_ids

# Main function to run the script
def main():
    script_env = initialize_script_environment()
    logging.info("Starting RERUN_off_checkpoints script...")
    
    # Get all game IDs that have already been processed
    processed_game_ids = get_processed_game_ids_from_checkpoints(script_env)

    # Get all available game IDs
    season, season_types = get_season_config()
    all_game_ids = get_all_game_ids(season, season_types)
    all_game_ids = [str(game_id) for game_id in all_game_ids] # Ensure all_game_ids are also strings

    # Determine which game IDs still need to be processed
    remaining_game_ids = [game_id for game_id in all_game_ids if game_id not in processed_game_ids]
    logging.info(f"Total game IDs to process: {len(all_game_ids)}")
    logging.info(f"Game IDs already processed: {len(processed_game_ids)}")
    logging.info(f"Game IDs remaining to process: {len(remaining_game_ids)}")

    if not remaining_game_ids:
        logging.info("No remaining game IDs to process. All data appears to be up-to-date.")
        return

    failed_game_ids_after_internal_retries = set()
    
    chunk_size = 100
    total_chunks = (len(remaining_game_ids) + chunk_size - 1) // chunk_size

# Grabbing boxscore data in chunks of 100 records based on game IDs
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min((chunk_idx + 1) * chunk_size, len(remaining_game_ids))
        current_chunk_ids = remaining_game_ids[start_idx:end_idx]

        logging.info(f"Processing chunk {chunk_idx + 1}/{total_chunks} ({len(current_chunk_ids)} games) of remaining game IDs.")
        
        aggregated_data_chunk = {k: [] for k in ['advanced','hustle','scoring','traditional','playertrack','usage']}

        pbar = tqdm(current_chunk_ids, desc=f"Fetching boxscores (Chunk {chunk_idx + 1})")
        for idx_in_chunk, game_id in enumerate(pbar, 1):
            pbar.set_description(f"Processing game {idx_in_chunk}/{len(current_chunk_ids)} in chunk {chunk_idx + 1}: {game_id}")
            game_data, success = fetch_boxscores_by_game(game_id) # Internal retries handled here
            if success:
                for key in aggregated_data_chunk.keys():
                    if key in game_data and not game_data[key].empty:
                        game_data[key]['GAME_ID'] = game_id  # tag with game ID
                        aggregated_data_chunk[key].append(game_data[key])
            else:
                logging.warning(f"Game ID {game_id} failed all internal retries. Adding to final retry list.")
                failed_game_ids_after_internal_retries.add(game_id)
        
        # Save checkpoint for the current chunk in the new rerun checkpoints directory
        for k, dfs in aggregated_data_chunk.items():
            if dfs:
                chunk_df = pd.concat(dfs, ignore_index=True)
                checkpoint_path = script_env.boxscore_rerun_checkpoints_dir / f"boxscore_{k}_rerun_chunk_{chunk_idx + 1}.csv"
                chunk_df.to_csv(checkpoint_path, index=False)
                logging.info(f"Saved rerun checkpoint for {k} to {checkpoint_path}")
            else:
                logging.info(f"No data to save for {k} in rerun chunk {chunk_idx + 1}.")
        
        print("Waiting for 3 seconds after chunk processing...")
        sleep(3) # Wait after each chunk

    if failed_game_ids_after_internal_retries:
        logging.warning(f"Initiating final retry for {len(failed_game_ids_after_internal_retries)} games that failed all internal attempts.")
        retried_aggregated_data = {k: [] for k in ['advanced','hustle','scoring','traditional','playertrack','usage']}
        retried_aggregated_data = retry_failed_boxscores(failed_game_ids_after_internal_retries, retried_aggregated_data)
        
        for k, dfs in retried_aggregated_data.items():
            if dfs:
                retried_df = pd.concat(dfs, ignore_index=True)
                retried_checkpoint_path = script_env.boxscore_rerun_checkpoints_dir / f"boxscore_{k}_rerun_retried.csv"
                retried_df.to_csv(retried_checkpoint_path, index=False)
                logging.info(f"Saved retried data for {k} to {retried_checkpoint_path}")

        if failed_game_ids_after_internal_retries:
            logging.error(f"Failed to retrieve boxscore data for {len(failed_game_ids_after_internal_retries)} games even after final retries: {failed_game_ids_after_internal_retries}")

    # Consolidate all boxscore rerun checkpoints
    logging.info("Consolidating boxscore rerun checkpoint files...")
    consolidated_boxscore_data = {k: [] for k in ['advanced','hustle','scoring','traditional','playertrack','usage']}
    
    for k in consolidated_boxscore_data.keys():
        checkpoint_files = list(script_env.boxscore_rerun_checkpoints_dir.glob(f"boxscore_{k}_rerun_chunk_*.csv"))
        retried_file = script_env.boxscore_rerun_checkpoints_dir / f"boxscore_{k}_rerun_retried.csv"
        
        if retried_file.exists():
            checkpoint_files.append(retried_file)

        if checkpoint_files:
            list_dfs = []
            for f in checkpoint_files:
                try:
                    df = pd.read_csv(f)
                    list_dfs.append(df)
                except Exception as e:
                    logging.error(f"Error reading rerun checkpoint file {f}: {e}")
            if list_dfs:
                consolidated_boxscore_data[k] = pd.concat(list_dfs, ignore_index=True)
                logging.info(f"Consolidated {len(list_dfs)} files for {k}.")
            else:
                consolidated_boxscore_data[k] = pd.DataFrame()
        else:
            consolidated_boxscore_data[k] = pd.DataFrame()

    # Save Consolidated Boxscore Data
    for k, df in consolidated_boxscore_data.items():
        if not df.empty:
            final_path = script_env.raw_dir / f"boxscore_{k}_rerun_{season}.csv"
            df.to_csv(final_path, index=False)
            logging.info(f"Consolidated rerun boxscore data for {k} saved to {final_path}")
        else:
            logging.warning(f"No consolidated rerun data for {k} to save.")

    # Move rerun checkpoint files to a subfolder within boxscore_rerun_checkpoints_dir
    logging.info("Moving boxscore rerun checkpoint files to a subfolder.")
    rerun_checkpoint_folder = script_env.boxscore_rerun_checkpoints_dir / datetime.datetime.now().strftime("%Y%m%d_%H%M%S_rerun")
    rerun_checkpoint_folder.mkdir(exist_ok=True)

    for f in script_env.boxscore_rerun_checkpoints_dir.glob("boxscore_*_rerun_*.csv"):
        try:
            f.rename(rerun_checkpoint_folder / f.name)
            logging.info(f"Moved {f.name} to {rerun_checkpoint_folder.name}")
        except Exception as e:
            logging.error(f"Error moving file {f.name}: {e}")

    logging.info("RERUN_off_checkpoints script complete.")

if __name__ == "__main__":
    main()