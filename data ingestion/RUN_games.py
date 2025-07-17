import logging
import pandas as pd
from config import (
    get_season_config,
    initialize_script_environment,
    get_nba_schedule,
    retry_failed_schedule
)

def main():
    script_env = initialize_script_environment()
    logging.info("Starting game data ingestion...")
    
    season, season_types = get_season_config()
    
    failed_schedule_seasons = set()
    
    # Fetch the NBA schedule with internal retries
    nba_schedule_df, success = get_nba_schedule(season)
    
    if not success:
        failed_schedule_seasons.add(season)
        logging.warning(f"Initial fetch of NBA schedule for season {season} failed. Adding to retry list.")

    # Final retry for any failed schedule fetches
    if failed_schedule_seasons:
        logging.warning(f"Initiating final retry for {len(failed_schedule_seasons)} NBA schedule fetches.")
        retried_schedule_df = retry_failed_schedule(failed_schedule_seasons)
        if not retried_schedule_df.empty:
            nba_schedule_df = pd.concat([nba_schedule_df, retried_schedule_df], ignore_index=True).drop_duplicates()
            logging.info("Successfully retrieved some or all previously failed NBA schedule data.")
        
        if failed_schedule_seasons:
            logging.error(f"Failed to retrieve NBA schedule data for {len(failed_schedule_seasons)} seasons even after final retries: {failed_schedule_seasons}")
    
    # Save the schedule data
    if not nba_schedule_df.empty:
        final_path = script_env.raw_dir / f"nba_schedule_{season}.csv"
        nba_schedule_df.to_csv(final_path, index=False)
        logging.info(f"NBA schedule data saved to {final_path}")
    else:
        logging.warning("No NBA schedule data was retrieved or saved after all attempts.")

    logging.info("Game data ingestion complete.")

if __name__ == "__main__":
    main()