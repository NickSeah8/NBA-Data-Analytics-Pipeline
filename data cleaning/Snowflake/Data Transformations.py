#############################   THIS IS THE SCRIPT USED TO TRANSFORM THE RAW DATA IN SNOWFLAKE USING SNOWPARK   #############################
import os
import logging
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, udf
from snowflake.snowpark.types import FloatType, IntegerType, StringType, DateType

def player_changes(session):
    # Read player data from raw tables
    df = session.table("RAW_PLAYERS_2024_25")

    # Filter for only active players
    df_active = df.filter(col("ROSTERSTATUS") == "Active")
    
    # Selecting certain columns
    df_selected = df_active.select(
        "PERSON_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "BIRTHDATE",
        "SCHOOL",
        "COUNTRY",
        "HEIGHT",
        "WEIGHT",
        "SEASON_EXP",
        "JERSEY",
        "POSITION",
        "TEAM_ID",
        "DLEAGUE_FLAG",
        "DRAFT_YEAR",
        "DRAFT_ROUND",
        "DRAFT_NUMBER",
        "GREATEST_75_FLAG"
    )
    
    # Save as a new table
    df_selected.write.save_as_table("NBA_PROCESSED_2024_25.PLAYERS_PROCESSED_2024_25", mode="overwrite")

def team_changes(session):
    # Read team data from raw tables
    df = session.table("RAW_TEAMS_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        "TEAM_ID",
        "ABBREVIATION",
        "CITY",
        "NICKNAME",
        "YEARFOUNDED",
        "ARENA",
        "ARENACAPACITY",
        "OWNER",
        "GENERALMANAGER",
        "HEADCOACH",
        "DLEAGUEAFFILIATION"
    )

    # Save as a table in schema
    df_selected.write.save_as_table("NBA_PROCESSED_2024_25.TEAMS_PROCESSED_2024_25", mode="overwrite")
    
def advanced_changes(session, convert_min_udf):  
    # Read advanced data from raw tables
    df = session.table("RAW_ADVANCED_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        "GAME_ID",
        "TEAM_ID",
        "PLAYER_ID",
        "MIN",
        "E_OFF_RATING",
        "OFF_RATING",
        "E_DEF_RATING",
        "DEF_RATING",
        "E_NET_RATING",
        "NET_RATING",
        "AST_PCT",
        "AST_TOV",
        "AST_RATIO",
        "OREB_PCT",
        "DREB_PCT",
        "REB_PCT",
        "TM_TOV_PCT",
        "EFG_PCT",
        "TS_PCT",
        "USG_PCT",
        "E_USG_PCT",
        "E_PACE",
        "PACE",
        "PACE_PER40",
        "POSS",
        "PIE"
    )

    # Altering minutes data
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))

    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.ADVANCED_PROCESSED_2024_25", mode="overwrite")    

def hustle_changes(session, convert_min_udf):
    # Read hustle data from raw tables
    df = session.table("RAW_HUSTLE_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        col("gameId").alias("GAME_ID"),
        col("teamId").alias("TEAM_ID"),
        col("personId").alias("PLAYER_ID"),
        col("minutes").alias("MIN"),
        col("points").alias("PTS"),
        col("contestedShots").alias("CONTESTED_SHOTS"),
        col("contestedShots2pt").alias("CONTESTED_SHOTS_2PT"),
        col("contestedShots3pt").alias("CONTESTED_SHOTS_3PT"),
        col("deflections").alias("DEFLECTIONS"),
        col("chargesDrawn").alias("CHARGES_DRAWN"),
        col("screenAssists").alias("SCREEN_ASSISTS"),
        col("screenAssistPoints").alias("SCREEN_ASSIST_POINTS"),
        col("looseBallsRecoveredOffensive").alias("LOOSEBALLS_RECOVERED_OFFENSIVE"),
        col("looseBallsRecoveredDefensive").alias("LOOSEBALLS_RECOVERED_DEFENSIVE"),
        col("looseBallsRecoveredTotal").alias("LOOSEBALLS_RECOVERED_TOTAL"),
        col("offensiveBoxOuts").alias("OFFENSIVE_BOXOUTS"),
        col("defensiveBoxOuts").alias("DEFENSIVE_BOXOUTS"),
        col("boxOutPlayerTeamRebounds").alias("BOXOUT_PLAYER_TEAM_REBOUNDS"),
        col("boxOutPlayerRebounds").alias("BOXOUT_PLAYER_REBOUNDS"),
        col("boxOuts").alias("BOXOUTS")
    )
    
    # Altering minutes data
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))

    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.HUSTLE_PROCESSED_2024_25", mode="overwrite")

def playertrack_changes(session, convert_min_udf):
    # Read playertrack data from raw tables
    df = session.table("RAW_PLAYERTRACK_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        "GAME_ID",
        "TEAM_ID",
        "PLAYER_ID",
        "MIN",
        "SPD",
        "DIST",
        "ORBC",
        "DRBC",
        "RBC",
        "TCHS",
        "SAST",
        "FTAST",
        "PASS",
        "AST",
        "CFGM",
        "CFGA",
        "CFG_PCT",
        "UFGM",
        "UFGA",
        "UFG_PCT",
        "FG_PCT",
        "DFGM",
        "DFGA",
        "DFG_PCT"
    )    

    # Altering minutes data
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))

    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.PLAYERTRACK_PROCESSED_2024_25", mode="overwrite")
    
def scoring_changes(session, convert_min_udf):    
    # Read scoring data from raw tables
    df = session.table("RAW_SCORING_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        "GAME_ID",
        "TEAM_ID",
        "PLAYER_ID",
        "MIN",
        "PCT_FGA_2PT",
        "PCT_FGA_3PT",
        "PCT_PTS_2PT",
        "PCT_PTS_2PT_MR",
        "PCT_PTS_3PT",
        "PCT_PTS_FB",
        "PCT_PTS_FT",
        "PCT_PTS_OFF_TOV",
        "PCT_PTS_PAINT",
        "PCT_AST_2PM",
        "PCT_UAST_2PM",
        "PCT_AST_3PM",
        "PCT_UAST_3PM",
        "PCT_AST_FGM",
        "PCT_UAST_FGM"
    )    

    # Cast data types
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))

    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.SCORING_PROCESSED_2024_25", mode="overwrite")

def traditional_changes(session, convert_min_udf):
    # Read traditional data from staging
    df = session.table("RAW_TRADITIONAL_2024_25")
    
    # Selecting certain columns
    df_selected = df.select(
        "GAME_ID",
        "TEAM_ID",
        "PLAYER_ID",
        "MIN",   
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "OREB",
        "DREB",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TO",
        "PF",
        "PTS",
        "PLUS_MINUS"
    )

    # Cast data types
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))
            
    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.TRADITIONAL_PROCESSED_2024_25", mode="overwrite")

def usage_changes(session, convert_min_udf):
    # Read usage data from staging
    df = session.table("RAW_USAGE_2024_25")

    # Selecting certain columns
    df_selected = df.select(
        "GAME_ID",
        "TEAM_ID",
        "PLAYER_ID",
        "MIN",
        "USG_PCT",
        "PCT_FGM",
        "PCT_FGA",
        "PCT_FG3M",
        "PCT_FG3A",
        "PCT_FTM",
        "PCT_FTA",
        "PCT_OREB",
        "PCT_DREB",
        "PCT_REB",
        "PCT_AST",
        "PCT_TOV",
        "PCT_STL",
        "PCT_BLK",
        "PCT_BLKA",
        "PCT_PF",
        "PCT_PFD",
        "PCT_PTS"
    )

    # Cast data types
    df_transformed = df_selected \
    .with_column("MIN", convert_min_udf(col("MIN")).cast(FloatType()))

    # Save as a table in schema
    df_transformed.write.save_as_table("NBA_PROCESSED_2024_25.USAGE_PROCESSED_2024_25", mode="overwrite")

def schedule_changes(session):
    # Read schedule data from staging
    df = session.table("RAW_SCHEDULE_2024_25")

    # Selecting certain columns
    df_selected = df.select(
        col("seasonYear").alias("SEASON"),
        col("gameDate").alias("GAME_DATE"),
        col("gameId").alias("GAME_ID"),
        col("gameCode").alias("GAME_CODE"),
        col("homeTeam_teamId").alias("HOME_TEAM_ID"),
        col("homeTeam_score").alias("HOME_TEAM_SCORE"),
        col("awayTeam_teamId").alias("AWAY_TEAM_ID"),
        col("awayTeam_score").alias("AWAY_TEAM_SCORE"),
        col("pointsLeaders_0_personId").alias("POINTS_LEADER_ID"),
        col("pointsLeaders_0_points").alias("POINTS_LEADER_POINTS")
    )

    # Save as a table in schema
    df_selected.write.save_as_table("NBA_PROCESSED_2024_25.SCHEDULE_PROCESSED_2024_25", mode="overwrite")

def main(session: Session):
    logging.basicConfig(level=logging.INFO)

    # Converting minutes column to float and streamlining format
    logging.info("Registering UDF")
    convert_min_udf = udf(
        lambda min_str: (
            0.0 if min_str is None else
            (lambda parts: float(parts[0]) if len(parts) == 1 else
                float(parts[0]) + float(parts[1]) / 60 if len(parts) == 2 else
                float(parts[0]) * 60 + float(parts[1]) + float(parts[2]) / 60 if len(parts) == 3 else
                0.0
            )(min_str.split(':'))
        ),
        input_types=[StringType()],
        return_type=FloatType(),
        session=session
    )

    logging.info("Transforming players table")
    player_changes(session)

    logging.info("Transforming teams table")
    team_changes(session)

    logging.info("Transforming advanced table")
    advanced_changes(session, convert_min_udf)

    logging.info("Transforming hustle table")
    hustle_changes(session, convert_min_udf)

    logging.info("Transforming playertrack table")
    playertrack_changes(session, convert_min_udf)

    logging.info("Transforming scoring table")
    scoring_changes(session, convert_min_udf)

    logging.info("Transforming traditional table")
    traditional_changes(session, convert_min_udf)

    logging.info("Transforming usage table")
    usage_changes(session, convert_min_udf)

    logging.info("Transforming schedule table")
    schedule_changes(session)

    return session.table("NBA_PROCESSED_2024_25.PLAYERS_PROCESSED_2024_25").limit(10)

if __name__ == "__main__":
    connection_parameters = {
        "account": os.getenv("<ACCOUNT IDENTIFIER>"),
        "user": os.getenv("<USER>"),
        "password": os.getenv("<PASSWORD>"),
        "role": os.getenv("<ROLE>"),
        "warehouse": os.getenv("<WAREHOUSE>"),
        "database": os.getenv("<DATABASE>"),
        "schema": os.getenv("<SCHEMA>"),
    }

    session = Session.builder.configs(connection_parameters).create()
    main(session)