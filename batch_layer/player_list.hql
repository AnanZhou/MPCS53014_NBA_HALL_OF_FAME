-- Step 1: Create an external table to map the CSV data
DROP TABLE IF EXISTS zhoua_nba_players_csv;

CREATE EXTERNAL TABLE zhoua_nba_players_csv(
    ID STRING,
    Name STRING,
    Number STRING,
    Country STRING,
    Position STRING,
    Age STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/tmp/zhoua/player_list/'

TBLPROPERTIES (
    "skip.header.line.count" = "1"
);


-- Step 2: Create an ORC table to store the data in a more efficient format
CREATE TABLE IF NOT EXISTS zhoua_nba_players_orc(
    ID INT,
    Name STRING,
    Number INT,
    Country STRING,
    Position STRING,
    Age INT
)
STORED AS ORC;

-- Step 3: Insert data from the CSV table into the ORC table
INSERT OVERWRITE TABLE zhoua_nba_players_orc
SELECT *
FROM zhoua_nba_players_csv;

-- run a test query

