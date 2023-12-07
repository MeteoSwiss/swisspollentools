#!/bin/bash

train_percentage=70
validation_percentage=15
test_percentage=15

# Specify the path to the folder containing CSV files
folder_path="./database"

# Get a list of CSV files in the folder
csv_files=("$folder_path"/*.csv)

# Check if there are at least two files
if [ ${#csv_files[@]} -lt 2 ]; then
    echo "Error: At least two CSV files are required for comparison."
    exit 1
fi

# Get the header of the first CSV file
first_file="${csv_files[0]}"
header=$(head -n 1 "$first_file")

# Check the header of each file
for file_path in "${csv_files[@]}"; do
    current_header=$(head -n 1 "$file_path")
    
    # Compare headers
    if [ "$header" != "$current_header" ]; then
        echo "Error: Headers in CSV files do not match."
        exit 1
    fi
done

output_file_merged="$folder_path/all_data.csv.merged"
# Append data from each file to the new file
for file_path in "${csv_files[@]}"; do
    # Skip the header when appending data
    tail -n +2 "$file_path" >> "$output_file_merged"
done

output_file_shuffled="$folder_path/all_data.csv.shuffled"
shuf "$output_file_merged" > "$output_file_shuffled"

total_lines=$(wc -l < "$output_file_shuffled")
train_lines=$((total_lines * train_percentage / 100))
validation_lines=$((total_lines * validation_percentage / 100))
test_lines=$((total_lines - train_lines - validation_lines))

output_file_train_tmp="$folder_path/train.csv.tmp"
output_file_validation_tmp="$folder_path/validation.csv.tmp"
output_file_test_tmp="$folder_path/test.csv.tmp"
sed -n "1,$((train_lines))p" "$output_file_shuffled" > $output_file_train_tmp
sed -n "$((train_lines)),$((train_lines+validation_lines))p" "$output_file_shuffled" > $output_file_validation_tmp
sed -n "$((train_lines+validation_lines)),$((train_lines+validation_lines+test_lines))p" "$output_file_shuffled" > $output_file_test_tmp

output_file_train="$folder_path/train.csv"
output_file_validation="$folder_path/validation.csv"
output_file_test="$folder_path/test.csv"
echo "$header" > "$output_file_train"
echo "$header" > "$output_file_validation"
echo "$header" > "$output_file_test"
cat $output_file_train_tmp >> $output_file_train
cat $output_file_validation_tmp >> $output_file_validation
cat $output_file_test_tmp >> $output_file_test

rm $output_file_merged $output_file_shuffled $output_file_train_tmp $output_file_validation_tmp $output_file_test_tmp