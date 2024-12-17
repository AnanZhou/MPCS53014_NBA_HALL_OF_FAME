-- create a batch view of player_detailed_stats in Hbase
-- 'player_name_season_id' as row key
-- 'details' as first column family
-- 'career_stats' as second column family
-- Step 1: Create the HBase-backed External Table

CREATE EXTERNAL TABLE zhoua_player_detailed_stats_hbase (
    player_name_season_id STRING, -- Composite key: player_name + season_id
    player_id INT,
    team_abbreviation STRING,
    player_age INT,
    gp INT,
    gs INT,
    min FLOAT,
    fgm FLOAT,
    fga FLOAT,
    fg_pct FLOAT,
    fg3m FLOAT,
    fg3a FLOAT,
    fg3_pct FLOAT,
    ftm FLOAT,
    fta FLOAT,
    ft_pct FLOAT,
    oreb FLOAT,
    dreb FLOAT,
    reb FLOAT,
    ast FLOAT,
    stl FLOAT,
    blk FLOAT,
    tov FLOAT,
    pf FLOAT,
    pts INT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' = ':key,details:player_id,details:team_abbreviation,details:player_age,career_stats:gp,career_stats:gs,career_stats:min,career_stats:fgm,career_stats:fga,career_stats:fg_pct,career_stats:fg3m,career_stats:fg3a,career_stats:fg3_pct,career_stats:ftm,career_stats:fta,career_stats:ft_pct,career_stats:oreb,career_stats:dreb,career_stats:reb,career_stats:ast,career_stats:stl,career_stats:blk,career_stats:tov,career_stats:pf,career_stats:pts'
)
TBLPROPERTIES (
    'hbase.table.name' = 'zhoua_player_detailed_stats_hbase'
);

-- Step 2: Insert Data into the HBase-backed Table
INSERT OVERWRITE TABLE zhoua_player_detailed_stats_hbase
SELECT
    CONCAT(player_name, '_', season_id) AS player_name_season_id, -- Generate composite row key
    player_id,
    team_abbreviation,
    player_age,
    gp,
    gs,
    min,
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk,
    tov,
    pf,
    pts
FROM zhoua_player_detailed_stats;

-- test query in hbase
get 'zhoua_player_detailed_stats_hbase', 'James Lebron_2017-18'

