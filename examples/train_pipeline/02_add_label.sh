#!/bin/bash

folder_path="./database"

# Loop through CSV files in the folder
for file_path in "$folder_path"/*.csv; do
    # Extract file name without extension
    file_name=$(basename "$file_path")
    file_name_no_extension="${file_name%.csv}"

    # Extract label from file name using awk
    label=$(echo "$file_name_no_extension" | awk -F'.' '{print $1}')

    # Add the label to the CSV files
    awk \
        -v label="$label" \
        -F',' 'BEGIN {FS=OFS=","} NR==1 {print $0, "label"} NR>1 {print $0, label}' \
        "$file_path" > "${file_path}.tmp"
    mv "${file_path}.tmp" "$file_path"

done