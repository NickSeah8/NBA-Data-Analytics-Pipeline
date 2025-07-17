import pandas as pd
import logging
import datetime
from config import (
    get_season_config,
    initialize_script_environment,
    fetch_season_boxscores
)

# Main function to run the script
def main():
    # Initialize logging and script paths
    script_env = initialize_script_environment()
    logging.info("Starting data ingestion...")
    
    # Fetch boxscore data in chunks and save checkpoints
    season, season_types = get_season_config()
    fetch_season_boxscores(season, season_types, script_env)

    # Consolidate all boxscore checkpoints
    logging.info("Consolidating boxscore checkpoint files...")
    consolidated_boxscore_data = {k: [] for k in ['advanced','hustle','scoring','traditional','playertrack','usage']}
    
    # Collect all checkpoint files and retry files
    for k in consolidated_boxscore_data.keys():
        checkpoint_files = list(script_env.boxscore_checkpoints_dir.glob(f"boxscore_{k}_chunk_*.csv"))
        retried_file = script_env.boxscore_checkpoints_dir / f"boxscore_{k}_retried.csv"
        
        if retried_file.exists():
            checkpoint_files.append(retried_file)

        if checkpoint_files:
            list_dfs = []
            for f in checkpoint_files:
                try:
                    df = pd.read_csv(f)
                    list_dfs.append(df)
                except Exception as e:
                    logging.error(f"Error reading checkpoint file {f}: {e}")
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
            final_path = script_env.raw_dir / f"boxscore_{k}_{season}.csv"
            df.to_csv(final_path, index=False)
            logging.info(f"Consolidated boxscore data for {k} saved to {final_path}")
        else:
            logging.warning(f"No consolidated data for {k} to save.")

    # Move checkpoint files to a subfolder within boxscore_checkpoints_dir
    logging.info("Moving boxscore checkpoint files to a subfolder.")
    current_run_checkpoint_folder = script_env.boxscore_checkpoints_dir / datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    current_run_checkpoint_folder.mkdir(exist_ok=True)

    for f in script_env.boxscore_checkpoints_dir.glob("boxscore_*.csv"):
        try:
            f.rename(current_run_checkpoint_folder / f.name)
            logging.info(f"Moved {f.name} to {current_run_checkpoint_folder.name}")
        except Exception as e:
            logging.error(f"Error moving file {f.name}: {e}")

    logging.info("Boxscore data ingestion complete.")

if __name__ == "__main__":
    main()