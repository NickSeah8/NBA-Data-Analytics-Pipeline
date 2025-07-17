import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from time import sleep
from tqdm import tqdm
from nba_api.stats.library.parameters import Season
from nba_api.stats.endpoints import (
    commonplayerinfo,
    playergamelogs,
    teamdetails,
    teamgamelogs,
    boxscoreadvancedv2,
    boxscorehustlev2,
    boxscorescoringv2,
    boxscoretraditionalv2,
    boxscoreplayertrackv2,
    boxscoreusagev2,
    leaguegamelog,
    scheduleleaguev2
)

# Function to get the current season and season types
# This function can be modified to change the season or season types as needed.
def get_season_config():
    season = '2024-25'
    season_types = ['Regular Season', 'Playoffs']
    return season, season_types


#################################### Directory and Logging Configuration ####################################
# Class to hold script paths and logging setup
class ScriptPaths:
    def __init__(self):
        self.script_path = Path(__file__).resolve()
        self.project_root = self.script_path.parents[1]
        self.logs_dir = self.project_root / "logging"
        self.data_dir = self.project_root / "data"
        self.raw_dir = self.data_dir / "raw"
        self.checkpoints_dir = self.data_dir / "checkpoints"
        self.boxscore_checkpoints_dir = self.checkpoints_dir / "boxscore_checkpoints"
        self.shots_checkpoints_dir = self.checkpoints_dir / "shots_checkpoints"
        self.games_checkpoints_dir = self.checkpoints_dir / "games_checkpoints"
        self.boxscore_rerun_checkpoints_dir = self.checkpoints_dir / "boxscore_rerun_checkpoints"
        self.rerun_files_dir = self.data_dir / "rerun"

        self.logs_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        self.raw_dir.mkdir(exist_ok=True)
        self.checkpoints_dir.mkdir(exist_ok=True)
        self.boxscore_checkpoints_dir.mkdir(exist_ok=True)
        self.shots_checkpoints_dir.mkdir(exist_ok=True)
        self.games_checkpoints_dir.mkdir(exist_ok=True)
        self.boxscore_rerun_checkpoints_dir.mkdir(exist_ok=True)
        self.rerun_files_dir.mkdir(exist_ok=True)

        # Create log filename with timestamp
        log_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.log_filename = self.logs_dir / f"nba_players_and_teams_{log_timestamp}.log"

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_filename, mode='w'),
                logging.StreamHandler()
            ]
        )

# Function to initialize script paths and logging
def initialize_script_environment():
    return ScriptPaths()


#################################### Player and Team Data Gathering Functions ####################################
# Function to gather all player IDs
def get_all_player_ids(season, season_types):
    logging.info("Gathering player information for the NBA season...")

    all_player_ids = []

    # Collect player IDs from all season types
    for season_type in season_types:
        logging.info(f"Fetching player logs for {season_type}...")
        try:
            logs = playergamelogs.PlayerGameLogs(
                season_nullable=season,
                season_type_nullable=season_type
            )
            df = logs.get_data_frames()[0]
            player_ids = df['PLAYER_ID'].unique().tolist()
            all_player_ids.extend(player_ids)
            sleep(0.6)
        except Exception as e:
            logging.error(f"Failed to fetch player logs for {season_type}: {e}")

    # Remove duplicates
    unique_player_ids = list(set(all_player_ids))
    logging.info(f"Found {len(unique_player_ids)} unique players across all season types.")
    return unique_player_ids

# Function to gather all player info based upon their IDs
def get_all_players_info(season, season_types):
    unique_player_ids = get_all_player_ids(season, season_types)
    logging.info(f"Fetching info for {len(unique_player_ids)} unique players.")
    # Fetch detailed info for each player
    player_data = []

    for i, pid in enumerate(tqdm(unique_player_ids, desc="Fetching player info")):
        try:
            info = commonplayerinfo.CommonPlayerInfo(player_id=pid)
            df = info.common_player_info.get_data_frame()
            player_data.append(df)
            logging.info(f"[{i+1}/{len(unique_player_ids)}] Retrieved info for Player ID {pid}")
            sleep(0.6)  # Avoid rate limit
        except Exception as e:
            logging.error(f"Failed to get info for Player ID {pid}: {e}")

    if player_data:
        all_players_df = pd.concat(player_data, ignore_index=True)
        return all_players_df
    else:
        logging.warning("No player info was retrieved.")
        return pd.DataFrame()

# Function to gather all team IDs based on game logs
def get_all_team_ids(season, season_types):
    logging.info("Gathering team IDs for the NBA season...")
    all_team_ids = []

    for season_type in season_types:
        logging.info(f"Fetching team logs for {season_type}...")
        try:
            logs = teamgamelogs.TeamGameLogs(
                season_nullable=season,
                season_type_nullable=season_type
            )
            df = logs.get_data_frames()[0]
            team_ids = df['TEAM_ID'].unique().tolist()
            all_team_ids.extend(team_ids)
            sleep(0.6)  # optional: throttle requests
        except Exception as e:
            logging.error(f"Failed to fetch team logs for {season_type}: {e}")

    # Remove duplicates
    all_team_ids = list(set(all_team_ids))
    logging.info(f"Found {len(all_team_ids)} unique teams across all season types.")
    return all_team_ids

# Function to gather detailed team information
def get_all_teams_info(season, season_types):
    logging.info("Gathering team information for the NBA season...")
    all_team_ids = get_all_team_ids(season, season_types)

    team_data = []

    for i, tid in enumerate(tqdm(all_team_ids, desc="Fetching team info")):
        try:
            info = teamdetails.TeamDetails(team_id=tid)
            df = info.team_background.get_data_frame()
            if not df.empty:
                team_data.append(df)
            logging.info(f"[{i+1}/{len(all_team_ids)}] Retrieved info for Team ID {tid}")
            sleep(0.6)  # Avoid rate limit
        except Exception as e:
            logging.error(f"Failed to get info for Team ID {tid}: {e}")

    # Return combined DataFrame
    if team_data:
        all_teams_df = pd.concat(team_data, ignore_index=True)
        return all_teams_df
    else:
        logging.warning("No team info was retrieved.")
        return pd.DataFrame()
    

#################################### Game ID and Boxscore Data Gathering Functions ####################################
# Function to get all game IDs for a given season and season types
def get_all_game_ids(season, season_types):
    all_game_ids = []
    logging.info(f"Fetching all game IDs for {season}")
    for season_type in season_types:
        logging.info(f"Getting game IDs for season type: {season_type}")
        logs = leaguegamelog.LeagueGameLog(season=season, season_type_all_star=season_type, timeout=60)
        df = logs.get_data_frames()[0]
        all_game_ids.extend(df['GAME_ID'].unique().tolist())
    unique_game_ids = list(set(all_game_ids))
    logging.info(f"Total unique games found: {len(unique_game_ids)}")
    return unique_game_ids

# Function to fetch boxscore data for a specific game ID with retries
# Any game IDs that fail to fetch data will be logged and retried later
def fetch_boxscores_by_game(game_id, max_attempts=5, retry_delay=5):
    logging.info(f"Fetching boxscore data for game {game_id}")
    data = {}
    for attempt in range(1, max_attempts + 1):
        try:
            data['advanced'] = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id=game_id, timeout=60).get_data_frames()[0]
            data['hustle'] = boxscorehustlev2.BoxScoreHustleV2(game_id=game_id, timeout=60).get_data_frames()[0]
            data['scoring'] = boxscorescoringv2.BoxScoreScoringV2(game_id=game_id, timeout=60).get_data_frames()[0]
            data['traditional'] = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id=game_id, timeout=60).get_data_frames()[0]
            data['playertrack'] = boxscoreplayertrackv2.BoxScorePlayerTrackV2(game_id=game_id, timeout=60).get_data_frames()[0]
            data['usage'] = boxscoreusagev2.BoxScoreUsageV2(game_id=game_id, timeout=60).get_data_frames()[0]
            return data, True
        except Exception as e:
            logging.error(f"Error fetching boxscore for game {game_id} (Attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                print(f"Retrying game {game_id} in {retry_delay} seconds (Attempt {attempt + 1}/{max_attempts})...")
                sleep(retry_delay)
            else:
                logging.error(f"Failed to fetch boxscore for game {game_id} after {max_attempts} attempts.")
                return {}, False

# Function to retry fetching boxscores for failed game IDs
def retry_failed_boxscores(failed_game_ids_set, aggregated_data, max_retries=3):
    logging.info(f"Attempting to retry {len(failed_game_ids_set)} failed game IDs for boxscores (final retry loop).")
    for attempt in range(1, max_retries + 1):
        if not failed_game_ids_set:
            break
        logging.info(f"Final retry attempt {attempt}/{max_retries} for boxscores. Remaining: {len(failed_game_ids_set)}")
        current_failed_ids = list(failed_game_ids_set)
        failed_game_ids_set.clear() # Clear for this attempt, re-add if still fails

        pbar = tqdm(current_failed_ids, desc=f"Final Retrying boxscores (Attempt {attempt})")
        for idx, game_id in enumerate(pbar, 1):
            pbar.set_description(f"Final Retrying game {idx}/{len(current_failed_ids)}: {game_id}")
            game_data, success = fetch_boxscores_by_game(game_id, max_attempts=1) # Only one attempt in this final loop
            if success:
                for key in aggregated_data.keys():
                    if key in game_data and not game_data[key].empty:
                        game_data[key]['GAME_ID'] = game_id
                        aggregated_data[key].append(game_data[key])
            else:
                failed_game_ids_set.add(game_id) # Add back to set if still fails
        sleep(5) # Longer sleep between final retry attempts
    return aggregated_data

# Function to fetch boxscore data for an entire season in chunks based on game IDs
# This function implicitly handles failures by logging and saving retried data to checkpoints
def fetch_season_boxscores(season, season_types, script_env: ScriptPaths):
    game_ids = get_all_game_ids(season, season_types)
    failed_game_ids_after_internal_retries = set()
    
    chunk_size = 100
    total_chunks = (len(game_ids) + chunk_size - 1) // chunk_size

# Grabbing boxscore data in chunks of 100 records based on game IDs
    for chunk_idx in range(total_chunks):
        start_idx = chunk_idx * chunk_size
        end_idx = min((chunk_idx + 1) * chunk_size, len(game_ids))
        current_chunk_ids = game_ids[start_idx:end_idx]

        logging.info(f"Processing chunk {chunk_idx + 1}/{total_chunks} ({len(current_chunk_ids)} games)")
        
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
        
        # Save checkpoint for the current chunk
        for k, dfs in aggregated_data_chunk.items():
            if dfs:
                chunk_df = pd.concat(dfs, ignore_index=True)
                checkpoint_path = script_env.boxscore_checkpoints_dir / f"boxscore_{k}_chunk_{chunk_idx + 1}.csv"
                chunk_df.to_csv(checkpoint_path, index=False)
                logging.info(f"Saved checkpoint for {k} to {checkpoint_path}")
            else:
                logging.info(f"No data to save for {k} in chunk {chunk_idx + 1}.")
        
        print("Waiting for 3 seconds after chunk processing...")
        sleep(3) # Wait after each chunk

# Final retry for any game IDs that failed all initial attempts
    if failed_game_ids_after_internal_retries:
        logging.warning(f"Initiating final retry for {len(failed_game_ids_after_internal_retries)} games that failed all internal attempts.")
        retried_aggregated_data = {k: [] for k in ['advanced','hustle','scoring','traditional','playertrack','usage']}
        retried_aggregated_data = retry_failed_boxscores(failed_game_ids_after_internal_retries, retried_aggregated_data)
        
        for k, dfs in retried_aggregated_data.items():
            if dfs:
                retried_df = pd.concat(dfs, ignore_index=True)
                retried_checkpoint_path = script_env.boxscore_checkpoints_dir / f"boxscore_{k}_retried.csv"
                retried_df.to_csv(retried_checkpoint_path, index=False)
                logging.info(f"Saved retried data for {k} to {retried_checkpoint_path}")

        if failed_game_ids_after_internal_retries:
            logging.error(f"Failed to retrieve boxscore data for {len(failed_game_ids_after_internal_retries)} games even after final retries: {failed_game_ids_after_internal_retries}")


#################################### NBA Season Schedule Data Gathering Functions ####################################
# Function to get the NBA schedule for a specific season with retries
def get_nba_schedule(season, max_attempts=5, retry_delay=5):
    logging.info(f"Attempting to fetch NBA schedule for season {season} with {max_attempts} retries.")
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"Fetching NBA schedule (Attempt {attempt}/{max_attempts})...")
            schedule = scheduleleaguev2.ScheduleLeagueV2(season=season, timeout=60)
            df = schedule.get_data_frames()[0]
            # Filter columns to only include up to 'POINTSLEADERS_3'
            if 'POINTSLEADERS_3' in df.columns:
                df = df.loc[:, :'POINTSLEADERS_3']
            else:
                logging.warning("POINTSLEADERS_3 column not found in NBA schedule data. Returning all available columns.")
            logging.info(f"Successfully fetched NBA schedule for season {season}.")
            return df, True
        except Exception as e:
            logging.error(f"Error fetching NBA schedule for season {season} (Attempt {attempt}/{max_attempts}): {e}")
            if attempt < max_attempts:
                print(f"Retrying NBA schedule fetch in {retry_delay} seconds (Attempt {attempt + 1}/{max_attempts})...")
                sleep(retry_delay)
            else:
                logging.error(f"Failed to fetch NBA schedule for season {season} after {max_attempts} attempts.")
                return pd.DataFrame(), False

def retry_failed_schedule(failed_seasons_set, max_retries=3):
    logging.info(f"Attempting to retry {len(failed_seasons_set)} failed NBA schedule fetches (final retry loop).")
    retried_schedule_df = pd.DataFrame()
    for attempt in range(1, max_retries + 1):
        if not failed_seasons_set:
            break
        logging.info(f"Final retry attempt {attempt}/{max_retries} for NBA schedule. Remaining: {len(failed_seasons_set)}")
        current_failed_seasons = list(failed_seasons_set)
        failed_seasons_set.clear() # Clear for this attempt, re-add if still fails

        for season in tqdm(current_failed_seasons, desc=f"Final Retrying NBA schedule (Attempt {attempt})"):
            schedule_df, success = get_nba_schedule(season, max_attempts=1) # Only one attempt in this final loop
            if success and not schedule_df.empty:
                retried_schedule_df = pd.concat([retried_schedule_df, schedule_df], ignore_index=True)
            else:
                failed_seasons_set.add(season) # Add back to set if still fails
        sleep(5) # Longer sleep between final retry attempts
    return retried_schedule_df
