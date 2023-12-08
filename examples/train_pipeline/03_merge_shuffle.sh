#!/bin/bash

DB_FOLDER_PATH="./data"
TRAIN_PERCENTAGE=70
VALIDATION_PERCENTAGE=15
TEST_PERCENTAGE=15

################################################################################
# List all the files
################################################################################

csv_files=("$DB_FOLDER_PATH"/*.csv)

################################################################################
# Extract column names (header) and assert files consistency
################################################################################

if [ ${#csv_files[@]} -lt 2 ]; then
    echo "Error: At least two CSV files are required for comparison."
    exit 1
fi

first_file="${csv_files[0]}"
header=$(head -n 1 "$first_file")

for file_path in "${csv_files[@]}"; do
    current_header=$(head -n 1 "$file_path")
    
    # Compare headers
    if [ "$header" != "$current_header" ]; then
        echo "Error: Headers in CSV files do not match."
        exit 1
    fi
done

################################################################################
# Merge all the files and shuffle the entries
################################################################################
output_file_merged="$DB_FOLDER_PATH/all_data.csv.merged"
output_file_shuffled="$DB_FOLDER_PATH/all_data.csv.shuffled"

for file_path in "${csv_files[@]}"; do
    # Skip the header when appending data
    tail -n +2 "$file_path" >> "$output_file_merged"
done

shuf "$output_file_merged" > "$output_file_shuffled"

################################################################################
# Split the data in train/test/validation splits
################################################################################

output_file_train="$DB_FOLDER_PATH/train.csv"
output_file_validation="$DB_FOLDER_PATH/validation.csv"
output_file_test="$DB_FOLDER_PATH/test.csv"

output_file_train_tmp="$output_file_train.tmp"
output_file_validation_tmp="$output_file_validation.tmp"
output_file_test_tmp="$output_file_test.tmp"

total_lines=$(wc -l < "$output_file_shuffled")
train_lines=$((total_lines * TRAIN_PERCENTAGE / 100))
validation_lines=$((total_lines * VALIDATION_PERCENTAGE / 100))
test_lines=$((total_lines - train_lines - validation_lines))

sed -n "1,$((train_lines))p" \
    "$output_file_shuffled" > $output_file_train_tmp
sed -n "$((train_lines)),$((train_lines+validation_lines))p" \
    "$output_file_shuffled" > $output_file_validation_tmp
sed -n "$((train_lines+validation_lines)),$((train_lines+validation_lines+test_lines))p" \
    "$output_file_shuffled" > $output_file_test_tmp

echo "$header" > "$output_file_train"
echo "$header" > "$output_file_validation"
echo "$header" > "$output_file_test"

cat $output_file_train_tmp >> $output_file_train
cat $output_file_validation_tmp >> $output_file_validation
cat $output_file_test_tmp >> $output_file_test

rm \
    $output_file_merged $output_file_shuffled \
    $output_file_train_tmp $output_file_validation_tmp \
    $output_file_test_tmp
