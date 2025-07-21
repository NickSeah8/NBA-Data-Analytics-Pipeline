---------------------   THIS SCRIPT IS USED TO CREATE THE TABLES IN SNOWFLAKE AND COPY THE RAW DATA INTO THEM   ---------------------
-- Creating file format
CREATE OR REPLACE FILE FORMAT nba_pipeline_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = true;

-- Creating and copying player data into table
CREATE OR REPLACE TABLE RAW_PLAYERS_2024_25 (
    PERSON_ID INT NOT NULL,
    FIRST_NAME STRING NULL,
    LAST_NAME STRING NULL,
    DISPLAY_FIRST_LAST STRING NULL,
    DISPLAY_LAST_COMMA_FIRST STRING NULL,
    DISPLAY_FI_LAST STRING NULL,
    PLAYER_SLUG STRING NULL,
    BIRTHDATE DATE NULL,
    SCHOOL STRING NULL,
    COUNTRY STRING NULL,
    LAST_AFFILIATION STRING NULL,
    HEIGHT STRING NULL,
    WEIGHT FLOAT NULL,
    SEASON_EXP INT NULL,
    JERSEY STRING NULL,
    POSITION STRING NULL,
    ROSTERSTATUS STRING NULL,
    GAMES_PLAYED_CURRENT_SEASON_FLAG STRING NULL,
    TEAM_ID INT NULL,
    TEAM_NAME STRING NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CODE STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYERCODE STRING NULL,
    FROM_YEAR INT NULL,
    TO_YEAR INT NULL,
    DLEAGUE_FLAG STRING NULL,
    NBA_FLAG STRING NULL,
    GAMES_PLAYED_FLAG STRING NULL,
    DRAFT_YEAR STRING NULL,
    DRAFT_ROUND STRING NULL,
    DRAFT_NUMBER STRING NULL,
    GREATEST_75_FLAG STRING NULL
);

COPY INTO RAW_PLAYERS_2024_25
FROM @RAW_STAGE/raw_all_players_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying team data into table
CREATE OR REPLACE TABLE RAW_TEAMS_2024_25 (
    TEAM_ID INT NOT NULL,
    ABBREVIATION STRING NULL,
    NICKNAME STRING NULL,
    YEARFOUNDED INT NULL,
    CITY STRING NULL,
    ARENA STRING NULL,
    ARENACAPACITY INT NULL,
    OWNER STRING NULL,
    GENERALMANAGER STRING NULL,
    HEADCOACH STRING NULL,
    DLEAGUEAFFILIATION STRING NULL
);

COPY INTO RAW_TEAMS_2024_25
FROM @RAW_STAGE/raw_all_teams_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying advanced data into table
CREATE OR REPLACE TABLE RAW_ADVANCED_2024_25 (
    GAME_ID INT NOT NULL,
    TEAM_ID INT NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYER_ID INT NOT NULL,
    PLAYER_NAME STRING NULL,
    NICKNAME STRING NULL,
    START_POSITION STRING NULL,
    COMMENT STRING NULL,
    MIN STRING NULL,
    E_OFF_RATING FLOAT NULL,
    OFF_RATING FLOAT NULL,
    E_DEF_RATING FLOAT NULL,
    DEF_RATING FLOAT NULL,
    E_NET_RATING FLOAT NULL,
    NET_RATING FLOAT NULL,
    AST_PCT FLOAT NULL,
    AST_TOV FLOAT NULL,
    AST_RATIO FLOAT NULL,
    OREB_PCT FLOAT NULL,
    DREB_PCT FLOAT NULL,
    REB_PCT FLOAT NULL,
    TM_TOV_PCT FLOAT NULL,
    EFG_PCT FLOAT NULL,
    TS_PCT FLOAT NULL,
    USG_PCT FLOAT NULL,
    E_USG_PCT FLOAT NULL,
    E_PACE FLOAT NULL,
    PACE FLOAT NULL,
    PACE_PER40 FLOAT NULL,
    POSS FLOAT NULL,
    PIE FLOAT NULL
);

COPY INTO RAW_ADVANCED_2024_25
FROM @RAW_STAGE/raw_boxscore_advanced_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying hustle data into table
CREATE OR REPLACE TABLE RAW_HUSTLE_2024_25 (
    gameId INT NOT NULL,
    teamId INT NULL,
    teamCity STRING NULL,
    teamName STRING NULL,
    teamTricode STRING NULL,
    teamSlug STRING NULL,
    personId INT NOT NULL,
    firstName STRING NULL,
    familyName STRING NULL,
    nameI STRING NULL,
    playerSlug STRING NULL,
    position STRING NULL,
    comment STRING NULL,
    jerseyNum INT NULL,
    minutes STRING NULL,
    points INT NULL,
    contestedShots INT NULL,
    contestedShots2pt INT NULL,
    contestedShots3pt INT NULL,
    deflections INT NULL,
    chargesDrawn INT NULL,
    screenAssists INT NULL,
    screenAssistPoints INT NULL,
    looseBallsRecoveredOffensive INT NULL,
    looseBallsRecoveredDefensive INT NULL,
    looseBallsRecoveredTotal INT NULL,
    offensiveBoxOuts INT NULL,
    defensiveBoxOuts INT NULL,
    boxOutPlayerTeamRebounds INT NULL,
    boxOutPlayerRebounds INT NULL,
    boxOuts INT NULL,
    GAME_ID INT NOT NULL
);

COPY INTO RAW_HUSTLE_2024_25
FROM @RAW_STAGE/raw_boxscore_hustle_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying playertrack data into table
CREATE OR REPLACE TABLE RAW_PLAYERTRACK_2024_25 (
    GAME_ID INT NOT NULL,
    TEAM_ID INT NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYER_ID INT NOT NULL,
    PLAYER_NAME STRING NULL,
    START_POSITION STRING NULL,
    COMMENT STRING NULL,
    MIN STRING NULL,
    SPD FLOAT NULL,
    DIST FLOAT NULL,
    ORBC INT NULL,
    DRBC INT NULL,
    RBC INT NULL,
    TCHS INT NULL,
    SAST INT NULL,
    FTAST INT NULL,
    PASS INT NULL,
    AST INT NULL,
    CFGM INT NULL,
    CFGA INT NULL,
    CFG_PCT FLOAT NULL,
    UFGM INT NULL,
    UFGA INT NULL,
    UFG_PCT FLOAT NULL,
    FG_PCT FLOAT NULL,
    DFGM INT NULL,
    DFGA INT NULL,
    DFG_PCT FLOAT NULL
);

COPY INTO RAW_PLAYERTRACK_2024_25
FROM @RAW_STAGE/raw_boxscore_playertrack_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying scoring data into table
CREATE OR REPLACE TABLE RAW_SCORING_2024_25 (
    GAME_ID INT NOT NULL,
    TEAM_ID INT NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYER_ID INT NOT NULL,
    PLAYER_NAME STRING NULL,
    NICKNAME STRING NULL,
    START_POSITION STRING NULL,
    COMMENT STRING NULL,
    MIN STRING NULL,
    PCT_FGA_2PT FLOAT NULL,
    PCT_FGA_3PT FLOAT NULL,
    PCT_PTS_2PT FLOAT NULL,
    PCT_PTS_2PT_MR FLOAT NULL,
    PCT_PTS_3PT FLOAT NULL,
    PCT_PTS_FB FLOAT NULL,
    PCT_PTS_FT FLOAT NULL,
    PCT_PTS_OFF_TOV FLOAT NULL,
    PCT_PTS_PAINT FLOAT NULL,
    PCT_AST_2PM FLOAT NULL,
    PCT_UAST_2PM FLOAT NULL,
    PCT_AST_3PM FLOAT NULL,
    PCT_UAST_3PM FLOAT NULL,
    PCT_AST_FGM FLOAT NULL,
    PCT_UAST_FGM FLOAT NULL
);

COPY INTO RAW_SCORING_2024_25
FROM @RAW_STAGE/raw_boxscore_scoring_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying traditional data into table
CREATE OR REPLACE TABLE RAW_TRADITIONAL_2024_25 (
    GAME_ID INT NOT NULL,
    TEAM_ID INT NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYER_ID INT NOT NULL,
    PLAYER_NAME STRING NULL,
    NICKNAME STRING NULL,
    START_POSITION STRING NULL,
    COMMENT STRING NULL,
    MIN STRING NULL,
    FGM INT NULL,
    FGA INT NULL,
    FG_PCT FLOAT NULL,
    FG3M INT NULL,
    FG3A INT NULL,
    FG3_PCT FLOAT NULL,
    FTM INT NULL,
    FTA INT NULL,
    FT_PCT FLOAT NULL,
    OREB INT NULL,
    DREB INT NULL,
    REB INT NULL,
    AST INT NULL,
    STL INT NULL,
    BLK INT NULL,
    "TO" INT NULL,
    PF INT NULL,
    PTS INT NULL,
    PLUS_MINUS INT NULL
);

COPY INTO RAW_TRADITIONAL_2024_25
FROM @RAW_STAGE/raw_boxscore_traditional_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying usage data into table
CREATE OR REPLACE TABLE RAW_USAGE_2024_25 (
    GAME_ID INT NOT NULL,
    TEAM_ID INT NULL,
    TEAM_ABBREVIATION STRING NULL,
    TEAM_CITY STRING NULL,
    PLAYER_ID INT NOT NULL,
    PLAYER_NAME STRING NULL,
    NICKNAME STRING NULL,
    START_POSITION STRING NULL,
    COMMENT STRING NULL,
    MIN STRING NULL,
    USG_PCT FLOAT NULL,
    PCT_FGM FLOAT NULL,
    PCT_FGA FLOAT NULL,
    PCT_FG3M FLOAT NULL,
    PCT_FG3A FLOAT NULL,
    PCT_FTM FLOAT NULL,
    PCT_FTA FLOAT NULL,
    PCT_OREB FLOAT NULL,
    PCT_DREB FLOAT NULL,
    PCT_REB FLOAT NULL,
    PCT_AST FLOAT NULL,
    PCT_TOV FLOAT NULL,
    PCT_STL FLOAT NULL,
    PCT_BLK FLOAT NULL,
    PCT_BLKA FLOAT NULL,
    PCT_PF FLOAT NULL,
    PCT_PFD FLOAT NULL,
    PCT_PTS FLOAT NULL
);

COPY INTO RAW_USAGE_2024_25
FROM @RAW_STAGE/raw_boxscore_usage_final_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

-- Creating and copying schedule data into table
CREATE OR REPLACE TABLE RAW_SCHEDULE_2024_25 (
    leagueId STRING NOT NULL,
    seasonYear STRING NOT NULL,
    gameDate DATE NOT NULL,
    gameId INT NOT NULL,
    gameCode STRING NOT NULL,
    gameStatus INT NULL,
    gameStatusText STRING NULL,
    gameSequence INT NULL,
    gameDateEst DATE NULL,
    gameTimeEst STRING NULL,
    gameDateTimeEst TIMESTAMP NULL,
    gameDateUTC DATE NULL,
    gameTimeUTC STRING NULL,
    gameDateTimeUTC TIMESTAMP NULL,
    awayTeamTime STRING NULL,
    homeTeamTime STRING NULL,
    day STRING NULL,
    monthNum INT NULL,
    weekNumber INT NULL,
    weekName STRING NULL,
    ifNecessary BOOLEAN NULL,
    seriesGameNumber STRING NULL,
    gameLabel STRING NULL,
    gameSubLabel STRING NULL,
    seriesText STRING NULL,
    arenaName STRING NULL,
    arenaState STRING NULL,
    arenaCity STRING NULL,
    postponedStatus STRING NULL,
    branchLink STRING NULL,
    gameSubtype STRING NULL,
    isNeutral BOOLEAN NULL,
    homeTeam_teamId INT NOT NULL,
    homeTeam_teamName STRING NULL,
    homeTeam_teamCity STRING NULL,
    homeTeam_teamTricode STRING NULL,
    homeTeam_teamSlug STRING NULL,
    homeTeam_wins INT NULL,
    homeTeam_losses INT NULL,
    homeTeam_score INT NULL,
    homeTeam_seed INT NULL,
    awayTeam_teamId INT NOT NULL,
    awayTeam_teamName STRING NULL,
    awayTeam_teamCity STRING NULL,
    awayTeam_teamTricode STRING NULL,
    awayTeam_teamSlug STRING NULL,
    awayTeam_wins INT NULL,
    awayTeam_losses INT NULL,
    awayTeam_score INT NULL,
    awayTeam_seed INT NULL,
    pointsLeaders_0_personId INT NULL,
    pointsLeaders_0_firstName STRING NULL,
    pointsLeaders_0_lastName STRING NULL,
    pointsLeaders_0_teamId INT NULL,
    pointsLeaders_0_teamCity STRING NULL,
    pointsLeaders_0_teamName STRING NULL,
    pointsLeaders_0_teamTricode STRING NULL,
    pointsLeaders_0_points FLOAT NULL,
    pointsLeaders_1_personId INT NULL,
    pointsLeaders_1_firstName STRING NULL,
    pointsLeaders_1_lastName STRING NULL,
    pointsLeaders_1_teamId INT NULL,
    pointsLeaders_1_teamCity STRING NULL,
    pointsLeaders_1_teamName STRING NULL,
    pointsLeaders_1_teamTricode STRING NULL,
    pointsLeaders_1_points FLOAT NULL,
    pointsLeaders_2_personId INT NULL,
    pointsLeaders_2_firstName STRING NULL,
    pointsLeaders_2_lastName STRING NULL,
    pointsLeaders_2_teamId INT NULL,
    pointsLeaders_2_teamCity STRING NULL,
    pointsLeaders_2_teamName STRING NULL,
    pointsLeaders_2_teamTricode STRING NULL,
    pointsLeaders_2_points FLOAT NULL,
    pointsLeaders_3_personId INT NULL,
    pointsLeaders_3_firstName STRING NULL,
    pointsLeaders_3_lastName STRING NULL,
    pointsLeaders_3_teamId INT NULL,
    pointsLeaders_3_teamCity STRING NULL,
    pointsLeaders_3_teamName STRING NULL,
    pointsLeaders_3_teamTricode STRING NULL,
    pointsLeaders_3_points FLOAT NULL,
    nationalBroadcasters_broadcasterScope STRING NULL,
    nationalBroadcasters_broadcasterMedia STRING NULL,
    nationalBroadcasters_broadcasterId STRING NULL,
    nationalBroadcasters_broadcasterDisplay STRING NULL,
    nationalBroadcasters_broadcasterAbbreviation STRING NULL,
    nationalBroadcasters_broadcasterDescription STRING NULL,
    nationalBroadcasters_tapeDelayComments STRING NULL,
    nationalBroadcasters_broadcasterVideoLink STRING NULL,
    nationalBroadcasters_broadcasterTeamId STRING NULL,
    nationalBroadcasters_broadcasterRanking INT NULL,
    nationalRadioBroadcasters_0_broadcasterScope STRING NULL,
    nationalRadioBroadcasters_0_broadcasterMedia STRING NULL,
    nationalRadioBroadcasters_0_broadcasterId STRING NULL,
    nationalRadioBroadcasters_0_broadcasterDisplay STRING NULL,
    nationalRadioBroadcasters_0_broadcasterAbbreviation STRING NULL,
    nationalRadioBroadcasters_0_broadcasterDescription STRING NULL,
    nationalRadioBroadcasters_0_tapeDelayComments STRING NULL,
    nationalRadioBroadcasters_0_broadcasterVideoLink STRING NULL,
    nationalRadioBroadcasters_0_broadcasterTeamId STRING NULL,
    nationalRadioBroadcasters_0_broadcasterRanking INT NULL,
    nationalRadioBroadcasters_1_broadcasterScope STRING NULL,
    nationalRadioBroadcasters_1_broadcasterMedia STRING NULL,
    nationalRadioBroadcasters_1_broadcasterId STRING NULL,
    nationalRadioBroadcasters_1_broadcasterDisplay STRING NULL,
    nationalRadioBroadcasters_1_broadcasterAbbreviation STRING NULL,
    nationalRadioBroadcasters_1_broadcasterDescription STRING NULL,
    nationalRadioBroadcasters_1_tapeDelayComments STRING NULL,
    nationalRadioBroadcasters_1_broadcasterVideoLink STRING NULL,
    nationalRadioBroadcasters_1_broadcasterTeamId STRING NULL,
    nationalRadioBroadcasters_1_broadcasterRanking INT NULL,
    homeTvBroadcasters_broadcasterScope STRING NULL,
    homeTvBroadcasters_broadcasterMedia STRING NULL,
    homeTvBroadcasters_broadcasterId STRING NULL,
    homeTvBroadcasters_broadcasterDisplay STRING NULL,
    homeTvBroadcasters_broadcasterAbbreviation STRING NULL,
    homeTvBroadcasters_broadcasterDescription STRING NULL,
    homeTvBroadcasters_tapeDelayComments STRING NULL,
    homeTvBroadcasters_broadcasterVideoLink STRING NULL,
    homeTvBroadcasters_broadcasterTeamId STRING NULL,
    homeTvBroadcasters_broadcasterRanking INT NULL,
    homeRadioBroadcasters_broadcasterScope STRING NULL,
    homeRadioBroadcasters_broadcasterMedia STRING NULL,
    homeRadioBroadcasters_broadcasterId STRING NULL,
    homeRadioBroadcasters_broadcasterDisplay STRING NULL,
    homeRadioBroadcasters_broadcasterAbbreviation STRING NULL,
    homeRadioBroadcasters_broadcasterDescription STRING NULL,
    homeRadioBroadcasters_tapeDelayComments STRING NULL,
    homeRadioBroadcasters_broadcasterVideoLink STRING NULL,
    homeRadioBroadcasters_broadcasterTeamId STRING NULL,
    homeRadioBroadcasters_broadcasterRanking INT NULL,
    homeOttBroadcasters_broadcasterScope STRING NULL,
    homeOttBroadcasters_broadcasterMedia STRING NULL,
    homeOttBroadcasters_broadcasterId STRING NULL,
    homeOttBroadcasters_broadcasterDisplay STRING NULL,
    homeOttBroadcasters_broadcasterAbbreviation STRING NULL,
    homeOttBroadcasters_broadcasterDescription STRING NULL,
    homeOttBroadcasters_tapeDelayComments STRING NULL,
    homeOttBroadcasters_broadcasterVideoLink STRING NULL,
    homeOttBroadcasters_broadcasterTeamId STRING NULL,
    homeOttBroadcasters_broadcasterRanking INT NULL,
    awayTvBroadcasters_broadcasterScope STRING NULL,
    awayTvBroadcasters_broadcasterMedia STRING NULL,
    awayTvBroadcasters_broadcasterId STRING NULL,
    awayTvBroadcasters_broadcasterDisplay STRING NULL,
    awayTvBroadcasters_broadcasterAbbreviation STRING NULL,
    awayTvBroadcasters_broadcasterDescription STRING NULL,
    awayTvBroadcasters_tapeDelayComments STRING NULL,
    awayTvBroadcasters_broadcasterVideoLink STRING NULL,
    awayTvBroadcasters_broadcasterTeamId STRING NULL,
    awayTvBroadcasters_broadcasterRanking INT NULL,
    awayRadioBroadcasters_broadcasterScope STRING NULL,
    awayRadioBroadcasters_broadcasterMedia STRING NULL,
    awayRadioBroadcasters_broadcasterId STRING NULL,
    awayRadioBroadcasters_broadcasterDisplay STRING NULL,
    awayRadioBroadcasters_broadcasterAbbreviation STRING NULL,
    awayRadioBroadcasters_broadcasterDescription STRING NULL,
    awayRadioBroadcasters_tapeDelayComments STRING NULL,
    awayRadioBroadcasters_broadcasterVideoLink STRING NULL,
    awayRadioBroadcasters_broadcasterTeamId STRING NULL,
    awayRadioBroadcasters_broadcasterRanking INT NULL,
    awayOttBroadcasters_broadcasterScope STRING NULL,
    awayOttBroadcasters_broadcasterMedia STRING NULL,
    awayOttBroadcasters_broadcasterId STRING NULL,
    awayOttBroadcasters_broadcasterDisplay STRING NULL,
    awayOttBroadcasters_broadcasterAbbreviation STRING NULL,
    awayOttBroadcasters_broadcasterDescription STRING NULL,
    awayOttBroadcasters_tapeDelayComments STRING NULL,
    awayOttBroadcasters_broadcasterVideoLink STRING NULL,
    awayOttBroadcasters_broadcasterTeamId STRING NULL,
    awayOttBroadcasters_broadcasterRanking INT NULL
);

COPY INTO RAW_SCHEDULE_2024_25
FROM @RAW_STAGE/raw_nba_schedule_2024-25.csv
FILE_FORMAT = (FORMAT_NAME = 'nba_pipeline_csv_format')
ON_ERROR = 'CONTINUE';

SHOW WAREHOUSES;

LIST @RAW_STAGE;

DROP TABLE RAW_USAGE_2024_25