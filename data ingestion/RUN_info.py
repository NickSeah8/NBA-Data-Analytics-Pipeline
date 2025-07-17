#################################### Running Data Ingestion for Players and Teams ####################################
# This script is responsible for gathering all player and team information for the NBA season.

import logging
from config import (
    get_season_config,
    initialize_script_environment,
    get_all_players_info,
    get_all_teams_info
)

# Main function to run the script
def main():
    # Initialize logging and script paths
    script_env = initialize_script_environment()
    logging.info("Starting data ingestion...")
    season, season_types = get_season_config()
    all_players = get_all_players_info(season, season_types)
    all_teams = get_all_teams_info(season, season_types)

    # Save player info
    all_players.to_csv(script_env.raw_dir / f"all_players_{season}.csv", index=False)
    logging.info(f"Saved player info to {script_env.raw_dir / f'all_players_{season}.csv'}")

    # Save team info (including team names, cities, etc.)
    all_teams.to_csv(script_env.raw_dir / f"all_teams_{season}.csv", index=False)
    logging.info(f"Saved team info to {script_env.raw_dir / f'all_teams_{season}.csv'}")

    logging.info("Data ingestion complete.")

if __name__ == "__main__":
    main()