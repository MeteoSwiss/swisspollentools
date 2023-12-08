# SwissPollenTools | Training Pipeline Example

This directory showcases an illustration of the complete training pipeline, encompassing the generation of a dataset from Polleno records with the labeling of samples through bash scripts. Additionally, it involves merging and shuffling data, also achieved with bash scripts. The subsequent step involves the implementation of a basic model trained using the SwissPollenTools library.

## 1. Creation of the Dataset
`01_create_dataset.py`

In this example, we presume that our data is organized according to the following structure:

```
./data/
  |--species_1/
        |--record_1.zip
        |--record_2.zip
  |--species_2/
        |--...
  |...
  |--species_n/
        |--record_1.zip
```

We define a glob pattern for file selection and a regex pattern for label extraction:

- glob: `./data/*/record_*.zip`
- regex: `\.\/data\/(?P<label>\w+)\/record_*.zip`

The regex pattern captures the species name as a label, utilized for file saving.

We then configure various tools used for dataset creation (Extraction and ToCSV). For the Extraction step, reading Polleno .zip records, we specify metadata, fluorescence keys, record properties, and associated filters. For the ToCSV step, saving the extracted data, we define the output directory.

We formulate the ToCSV pipeline, akin to the inference pipeline, and instantiate it. For convenience, the output file path is integrated into the ToCSVPipeline.

Finally, we execute the pipeline for input files obtained with the glob pattern and output files obtained with the regex pattern.

## 2. Adding the label, Merging and Shuffling 
`02_add_labe.sh`, `03_merge_shuffle.sh`

Label addition to the CSV file is accomplished through bash commands, extracting labels from file names using awk to append new columns to the .csv files.

The merging and shuffling script involves the following steps:

### A. Listing, Extracting Column Names, and Ensuring File Consistency
```
csv_files=("$folder_path"/*.csv)

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
```

### B. Merge All Files and Shuffle Entries
```
output_file_merged="$folder_path/all_data.csv.merged"
output_file_shuffled="$folder_path/all_data.csv.shuffled"

for file_path in "${csv_files[@]}"; do
    # Skip the header when appending data
    tail -n +2 "$file_path" >> "$output_file_merged"
done

shuf "$output_file_merged" > "$output_file_shuffled"
```

### C. Split Data into Train/Test/Validation Splits
```
output_file_train="$folder_path/train.csv"
output_file_validation="$folder_path/validation.csv"
output_file_test="$folder_path/test.csv"

output_file_train_tmp="$output_file_train.tmp"
output_file_validation_tmp="$output_file_validation.tmp"
output_file_test_tmp="$output_file_test.tmp"

total_lines=$(wc -l < "$output_file_shuffled")
train_lines=$((total_lines * train_percentage / 100))
validation_lines=$((total_lines * validation_percentage / 100))
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
```

## 3. Train the model
`04_train_model.py`

To train the model, we first define a model using tensorflow. Extending the tf.keras.Model class, we must define two functions:

- init(self, *args, **kwargs)
    - The super().__init__() method is mandatory!
- call(self, input, training=False, *args, **kwargs)
    - The input is in the form, if the fields are present

```
input = {
    "fluorescence_data": {
        "relative_spectra": np.ndarray(-1, 3, 5)
        "average_mean": np.ndarray(-1, 3, 5)
        "average_std": np.ndarray(-1, 3, 5)
    },
    "rec0": np.ndarray(-1, 200, 200),
    "rec1": np.ndarray(-1, 200, 200),
}
```

The model determines the number of output categories.

Configuration for the Extraction tool and Train tool includes batch sizes, number of epochs and fluorescence keys for model training.

We instantiate the model, optimizer, and loss function. The training pipeline, analogous to the Inference Pipeline, is defined and executed.

Note that the loading of the dataset can be time consuming using CSV files.