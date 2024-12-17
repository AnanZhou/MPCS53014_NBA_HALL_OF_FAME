-- Step 1: Create an external table to map the CSV data
DROP TABLE IF EXISTS zhoua_nba_player_stats_csv;

CREATE EXTERNAL TABLE zhoua_nba_player_stats_csv(
    Player_Name STRING,
    PLAYER_ID INT,
    SEASON_ID STRING,
    LEAGUE_ID STRING,
    TEAM_ID INT,
    TEAM_ABBREVIATION STRING,
    PLAYER_AGE INT,
    GP INT,
    GS INT,
    MIN FLOAT,
    FGM FLOAT,
    FGA FLOAT,
    FG_PCT FLOAT,
    FG3M FLOAT,
    FG3A FLOAT,
    FG3_PCT FLOAT,
    FTM FLOAT,
    FTA FLOAT,
    FT_PCT FLOAT,
    OREB FLOAT,
    DREB FLOAT,
    REB FLOAT,
    AST FLOAT,
    STL FLOAT,
    BLK FLOAT,
    TOV FLOAT,
    PF FLOAT,
    PTS INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/tmp/zhoua/player_career_stats/'
TBLPROPERTIES (
    "skip.header.line.count" = "1"
);

-- Step 2: Create an ORC table to store the data in a more efficient format
DROP TABLE IF EXISTS zhoua_nba_player_stats_orc;

CREATE TABLE zhoua_nba_player_stats_orc(
    Player_Name STRING,
    PLAYER_ID INT,
    SEASON_ID STRING,
    LEAGUE_ID STRING,
    TEAM_ID INT,
    TEAM_ABBREVIATION STRING,
    PLAYER_AGE INT,
    GP INT,
    GS INT,
    MIN FLOAT,
    FGM FLOAT,
    FGA FLOAT,
    FG_PCT FLOAT,
    FG3M FLOAT,
    FG3A FLOAT,
    FG3_PCT FLOAT,
    FTM FLOAT,
    FTA FLOAT,
    FT_PCT FLOAT,
    OREB FLOAT,
    DREB FLOAT,
    REB FLOAT,
    AST FLOAT,
    STL FLOAT,
    BLK FLOAT,
    TOV FLOAT,
    PF FLOAT,
    PTS INT
)
STORED AS ORC;

-- Step 3: Insert data from the CSV table into the ORC table
INSERT OVERWRITE TABLE zhoua_nba_player_stats_orc
SELECT *
FROM zhoua_nba_player_stats_csv;

-- Step 4: Run a test query to validate the ORC table
SELECT Player_Name, SEASON_ID, PTS, REB, AST 
FROM zhoua_nba_player_stats_orc
LIMIT 10;
