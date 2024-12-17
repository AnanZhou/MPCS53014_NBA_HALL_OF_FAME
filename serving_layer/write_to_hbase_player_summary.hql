-- create a batch view of player_summary in Hbase(utilize hbase counter)
-- remember to drop table that is not using hbase counter to avoid confusion
-- 'name' as row key
-- 'details' as column family
-- choose total_points to be incrementable

-- Step 1: Drop the existing HBase-backed table if it exists
DROP TABLE IF EXISTS zhoua_player_summary_hbase;

-- Step 2: Create the new HBase-backed table with player_name as the row key
CREATE EXTERNAL TABLE zhoua_player_summary_hbase (
    name STRING,                                -- Row key
    player_id INT,
    number INT,
    country STRING,
    position STRING,
    age INT,
    current_total_points BIGINT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    'hbase.columns.mapping' =
    ':key,details:player_id,details:number,details:country,details:position,details:age,details:current_total_points#b'
)
TBLPROPERTIES (
    'hbase.table.name' = 'zhoua_player_summary_hbase'
);

-- Step 3: Insert data into the new HBase-backed table
INSERT OVERWRITE TABLE zhoua_player_summary_hbase
SELECT
    name,                                      -- Use player_name as the row key
    player_id,
    number,
    country,
    position,
    age,
    total_points_until_2023_2024
FROM zhoua_player_summary;



