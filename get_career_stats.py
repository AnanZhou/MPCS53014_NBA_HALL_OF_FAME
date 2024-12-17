
# get career stats from ten player including 2024-2025 
from nba_api.stats.endpoints import playercareerstats
import pandas as pd

# Mapping of player names to their IDs
player_ids = {
    "James Lebron": "2544",
    "Antetokounmpo Giannis": "203507",
    "Curry Stephen": "201939",
    "Durant Kevin": "201142",
    "Jokic Nikola": "203999",
    "Doncic Luka": "1629029",
    "Anthony Davis": "203076",
    "Leonard Kawhi": "202695",
    "Harden James": "201935",
    "Irving Kyrie": "202681"
}

# Create a DataFrame to store all players' career stats
all_career_stats = pd.DataFrame()

# Loop through each player
for player_name, player_id in player_ids.items():
    print(f"Fetching career stats for {player_name} (ID: {player_id})...")
    try:
        # Fetch career stats for the player
        career = playercareerstats.PlayerCareerStats(player_id=player_id)
        career_data = career.get_data_frames()[0]  # Get career stats as a DataFrame
        
        # Add a column for the player's name
        career_data['Player Name'] = player_name
        
        # Append to the master DataFrame
        all_career_stats = pd.concat([all_career_stats, career_data], ignore_index=True)
        
    except Exception as e:
        print(f"Error fetching data for {player_name}: {e}")

# Reorder columns to make player name the first column
columns = ['Player Name'] + [col for col in all_career_stats.columns if col != 'Player Name']
all_players_stats = all_career_stats[columns]

# Save the combined data to a CSV file
csv_file = "nba_players_career_stats.csv"
all_players_stats.to_csv(csv_file, index=False)
print(f"Career stats for all players have been saved to {csv_file}")







# Below is test script area
#-----------------------------------------------------------------------------------------------------------------------------------------------------------
""" from nba_api.stats.endpoints import playercareerstats

# LeBron James's player ID is 2544
career = playercareerstats.PlayerCareerStats(player_id='2544')

# Get career stats as a DataFrame
career_data = career.get_data_frames()[0]
print(career_data)
 """

""" import json
from nba_api.live.nba.endpoints import ScoreBoard

# Fetch today's scoreboard
games = ScoreBoard()

# Get JSON data
json_data = games.get_json()

# Save JSON data to a file
with open("scoreboard_data.json", "w") as file:
    file.write(json_data)

print("JSON saved to scoreboard_data.json")

 """
