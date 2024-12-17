# get basic information from ten players
import requests
import csv

# API Endpoint
url = "https://v1.basketball.api-sports.io/players"

# Your API Key
api_key = "00a179a8374cf8486761b08a87b9e5ce"  # Replace with your API key

# List of player names to search for
players = [
    "James Lebron",
    "Antetokounmpo Giannis",
    "Curry Stephen",
    "Durant Kevin",
    "Jokic Nikola",
    "Doncic Luka",
    "Davis Anthony",
    "Leonard Kawhi",
    "Harden James",
    "Irving Kyrie"
]

# Headers for authentication
headers = {
    "x-rapidapi-host": "v1.basketball.api-sports.io",
    "x-rapidapi-key": api_key
}

# CSV file to save results
csv_file = "nba_players.csv"

# Write the headers for the CSV
with open(csv_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    # Define the column headers
    writer.writerow(["ID", "Name", "Number", "Country", "Position", "Age"])

# Loop through each player and fetch their data
for player_name in players:
    print(f"Fetching data for {player_name}...")
    
    # Query parameters
    params = {"search": player_name}
    
    try:
        # Make the GET request
        response = requests.get(url, headers=headers, params=params)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()  # Parse JSON response
            
            # Check for results
            if data.get("results") > 0:
                with open(csv_file, mode="a", newline="") as file:
                    writer = csv.writer(file)
                    
                    for player in data.get("response", []):
                        # Write player information to the CSV
                        writer.writerow([
                            player['id'],
                            player['name'],
                            player['number'],
                            player['country'],
                            player['position'],
                            player['age']
                        ])
                print(f"Added data for {player_name}.")
            else:
                print(f"No results found for {player_name}.")
        else:
            print(f"Error: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"An error occurred while fetching data for {player_name}: {e}")

print(f"Data has been saved to {csv_file}")
