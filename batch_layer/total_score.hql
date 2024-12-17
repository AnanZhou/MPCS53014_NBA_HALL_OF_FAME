-- Step 1: Create an external table to map the CSV data
DROP TABLE IF EXISTS zhoua_total_points_csv;

CREATE EXTERNAL TABLE zhoua_total_points_csv(
    Player_Name STRING,
    Total_Points_Until_2023_2024 INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/tmp/zhoua/total_score/'
TBLPROPERTIES (
    "skip.header.line.count" = "1"
);

-- Step 2: Create an ORC table to store the data in a more efficient format
DROP TABLE IF EXISTS zhoua_total_points_orc;

CREATE TABLE zhoua_total_points_orc(
    Player_Name STRING,
    Total_Points_Until_2023_2024 INT
)
STORED AS ORC;

-- Step 3: Insert data from the CSV table into the ORC table
INSERT OVERWRITE TABLE zhoua_total_points_orc
SELECT *
FROM zhoua_total_points_csv;

-- Step 4: Run a test query to validate the ORC table
SELECT Player_Name, Total_Points_Until_2023_2024 
FROM zhoua_total_points_orc
LIMIT 10;
