#################################### Appending All Boxscore Data Together ####################################
# Run this script if you neeeded to run RERUN_off_checkpoints and you need to compile all boxscore data files together
import pandas as pd
import os
import glob
import logging
import shutil
from config import (
    get_season_config,
    initialize_script_environment
)

def append_boxscore_files():
    script_env = initialize_script_environment()
    logging.info("Initialized script environment.")

    # Get the current season configuration
    season = get_season_config()
    
    boxscore_types = [
        "advanced", "hustle", "playertrack", "scoring",
        "traditional", "usage"
    ]

    logging.info("Starting the process of appending boxscore files.")

    for b_type in boxscore_types:
        logging.info(f"Processing boxscore type: {b_type}")
        all_files = []

        # Collect boxscore_checkpoint files
        checkpoint_pattern = os.path.join(script_env.boxscore_checkpoints_dir, f"boxscore_{b_type}_chunk_*.csv")
        checkpoint_files = glob.glob(checkpoint_pattern)
        all_files.extend(checkpoint_files)
        logging.info(f"Found {len(checkpoint_files)} checkpoint files for {b_type}.")

        # Collect boxscore_rerun files from raw data folder (not in checkpoints)
        # Targets files like boxscore_advanced_rerun_{season}.csv and excludes any files within the 'checkpoints' subdirectories.
        rerun_pattern = os.path.join(script_env.raw_dir, f"boxscore_{b_type}_rerun_*.csv")
        rerun_files = [f for f in glob.glob(rerun_pattern) if "checkpoints" not in f]
        all_files.extend(rerun_files)
        logging.info(f"Found {len(rerun_files)} rerun files (outside checkpoints) for {b_type}.")

        if not all_files:
            logging.warning(f"No files found for boxscore type: {b_type}. Skipping.")
            continue

        # Read and append all files
        df_list = []
        for f_path in all_files:
            try:
                df = pd.read_csv(f_path)
                df_list.append(df)
                logging.debug(f"Successfully read {f_path}")
            except Exception as e:
                logging.error(f"Error reading file {f_path}: {e}")
        
        if df_list:
            final_df = pd.concat(df_list, ignore_index=True)
            output_path = script_env.data_dir / f"boxscore_{b_type}_final_{season}.csv"
            final_df.to_csv(output_path, index=False)
            logging.info(f"Successfully appended and saved {len(df_list)} files to {output_path}")

            # Move rerun files to the 'rerun files' directory
            for r_file in rerun_files:
                try:
                    shutil.move(r_file, script_env.rerun_files_dir / os.path.basename(r_file))
                    logging.info(f"Moved rerun file {os.path.basename(r_file)} to {script_env.rerun_files_dir}")
                except Exception as e:
                    logging.error(f"Error moving rerun file {r_file}: {e}")
        else:
            logging.error(f"No dataframes to concatenate for boxscore type: {b_type}.")

    logging.info("Finished appending all boxscore files.")

def main():
    append_boxscore_files()

if __name__ == "__main__":
    main()
