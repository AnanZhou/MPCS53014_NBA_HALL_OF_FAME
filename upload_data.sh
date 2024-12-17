#!/bin/bash

# Temporary directory for downloads
temp_dir="/tmp/downloaded_data"
mkdir -p "$temp_dir"

# Target HDFS directory
hdfs_directory="/tmp/zhoua/nbadata"

# List of Google Drive file IDs and corresponding file names
declare -A files=(
    ["1EQ1vAEh4t9xRhUTB6E-uio6FLf8axRCS"]="player_career_stats.csv"
    ["1Ny2L7vQiB8ZycGZDBEbmuhKymm_fg02x"]="player_list.csv"
    ["1nsdmqpWSnaZxZkR0E1S0MEQDiruxflcS"]="total_score.csv"
)

# Ensure the HDFS directory exists
echo "Creating HDFS directory if it doesn't exist..."
hdfs dfs -mkdir -p "$hdfs_directory"

# Loop through the files and process each one
for file_id in "${!files[@]}"; do
    file_name="${files[$file_id]}"
    
    echo "Downloading file $file_name from Google Drive..."
    curl -L "https://drive.google.com/uc?export=download&id=$file_id" -o "$temp_dir/$file_name"

    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to download file $file_name from Google Drive."
        continue
    fi

    # Validate the downloaded file to ensure it's not HTML
    if file "$temp_dir/$file_name" | grep -qi 'HTML'; then
        echo "Error: Downloaded file $file_name is not a valid CSV. Check Google Drive permissions or link."
        continue
    fi

    echo "File $file_name downloaded successfully to $temp_dir/$file_name."

    echo "Uploading $file_name to HDFS at $hdfs_directory..."
    hdfs dfs -put -f "$temp_dir/$file_name" "$hdfs_directory"

    if [[ $? -ne 0 ]]; then
        echo "Error: Failed to upload $file_name to HDFS."
        continue
    fi

    echo "File $file_name successfully uploaded to HDFS at $hdfs_directory/$file_name."
done

# Cleanup temporary directory
echo "Cleaning up temporary files..."
rm -rf "$temp_dir"

echo "All steps completed successfully!"
