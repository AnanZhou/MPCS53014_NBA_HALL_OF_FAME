**Andy's Hall of Fame**

This project, built using the Lambda Architecture, tracks the basic information and career statistics of ten NBA players. It displays each player's current total score, season, age, and key performance metrics—such as assists and blocks—to highlight their abilities.

**Data source**

'NBA players' provides basic information about ten NBA players, including their jersey number, country, position, and age.

'NBA player career statistics' provides detailed information and statistics for ten NBA players, as officially recorded by the NBA. The dataset includes data starting from each player's first year in the league and is sourced via the nba_api. It captures player details, team associations, performance metrics (e.g., points, assists, and rebounds), and game statistics for each season.

'NBA player total score up to 2023-2024 season' reflect the cumulative points each player has achieved since entering the league.


Three datasets were exported as CSV files. One of these datasets, which records daily NBA game statistics in 2024-25 season for each player, retrieves real-time data via the Sports Basketball API.

**Project Structure**
This project implements the three layers of the Lambda Architecture:

Batch Layer: Processes large volumes of historical data to generate accurate batch views.
Speed Layer: Handles real-time data streams to provide immediate insights, though these may be less accurate compared to the batch layer.
Serving Layer: Combines and exposes both batch and speed views to users.
Additionally, a front-end web application is used to present these views through a graphical interface.

Below is a detailed walkthrough, along with instructions and explanations of key decisions made during this project.

1. **Data Ingestion**:get data in csv format and put it into HDFS system
Script 'get_career_stats.py' retrieves career statistics for ten NBA players using the nba_api. It maps player names to their IDs, fetches stats for each player, and consolidates the data into a single Pandas DataFrame with player names added. The resulting dataset is saved as a CSV file.

Script 'get_player.py' retrieves basic information for ten NBA players using the Sports API and saves the resulting dataset as a CSV file. The purpose of this script is to create a reference table that will be useful when fetching real-time data from the same API. Using consistent player IDs simplifies tracking and data integration.

Run 'upload_data.sh' in cluster which automates downloading CSV files from Google Drive, validating them, and uploading them to an HDFS directory.

2. **Store data in hive**: convert csv files into more efficient ORC files
run player_list.hql , player_career_stats.hql, total_score.hql in hive cluster.

3. **Create zhoua_player_summary table**
use join_player_with_scores.hql join player basic information table and total score table to provide basic information and total points up to 2023-24 season in one table.

4. **Create a batch view in HBase**
use write_to_hbase_stats.hql to create batch view zhoua_player_detailed_stats_hbase

5. **Create kafka topic:** current-season-stats-zhoua
kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic current-season-stats-zhoua --bootstrap-server $KAFKABROKERS

6. **Ingest real-time game statistics for the 2024-25 season of ten selected NBA players from the Sports API into a Kafka topic**
upload uberjar to Hadoop cluster, location:'/home/sshuser/zhoua/nba/target'
run it as java in our cluster: java -cp nba-stats-producer-1.0-SNAPSHOT.jar com.zhoua.NBAPlayerStatsProducer $KAFKABROKERS

This java program extract and aggregate total points, total rebounds, and total assits from SportsAPI.

7. **Reads from kafka and update speed view**
create a table zhoua_player_summary_hbase
Make one column 'total_points' to be incrementable in zhoua_player_summary_hbase_current

upload uberjar to cluster and run spark-submit job, location: /home/sshuser/zhoua/speed_layer/target

spark-submit --master local[2] --driver-java-options "-
Dlog4j.configuration=file:///home/hadoop/ss.log4j.proper
ties" --class NBAPlayerStatsConsumer
uber-nba_speed_layer-1.0-SNAPSHOT.jar  $KAFKABROKERS

I decided to set a longer batch interval in my code, opting for 180 seconds instead of 2 seconds, as the NBA data for the current 2024-25 season is updated daily. (While a much longer interval would be more appropriate, I chose a relatively shorter one for ease of testing.) This longer interval ensures data is processed exactly once when new updates arrive, avoiding redundant processing. However, it also means that updates are reflected in downstream systems only once per day, sacrificing the flexibility to handle ad hoc updates or changes in real time.

We now have a batch view for players career statistics and a speed view for players with current total score

8. **Web App**: access in http://10.0.0.38:3082
The Node.js web application allows users to select a player from a drop-down list and displays basic information, such as player ID, jersey number, country, position, age, and total current points. It also provides detailed career information starting from the player's rookie season.

Due to issues with the HBase 'scan', I configured my Firefox browser to use a SOCKS proxy (localhost:9876) and set up an SSH tunnel using the following command:
SSH tunel:  ssh -i "C:\Users\anan1\.ssh\MPCS53014_key" -C2qTnNf -D 9876 sshuser@hbase-mpcs53014-2024-ssh.azurehdinsight.net

I uploaded my web application code to the /zhoua/web directory and ran the following command on the cluster:  
node app.js 3082 https://hbase-mpcs53014-2024.azurehdinsight.net/hbaserest $KAFKABROKERS

Despite these efforts, the scan issue persisted. I tried modifying the prefix filter format to JSON, but the API still reported a "bad request," indicating the problem wasn't format-related. Testing with a smaller table showed the issue wasn't due to table size or server capacity. Finally, verifying table accessibility in the HBase shell confirmed that the scan function worked there, suggesting the problem was with the HBase REST API itself.

As a workaround, I switched to an iterative get request approach. I created a list of seasons from 2003-04 to 2023-24 and queried HBase for each player iteratively. While this method resolved the issue, it is less efficient and not scalable for handling large datasets in a Big Data context.





