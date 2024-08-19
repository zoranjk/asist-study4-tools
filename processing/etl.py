''' data etl functions '''

import os
import json
import csv
import zipfile
import pandas as pd
from tqdm import tqdm

def extract_unique_subtypes_with_examples(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.metadata')]
    unique_subtypes = {}  # Use a dictionary to map subtypes to example messages

    # Iterate through each file in the specified folder
    for file_index, filename in enumerate(tqdm(files), start=1):
        file_path = os.path.join(folder_path, filename)

        # Open and read the file with utf-8 encoding specified
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_number, line in enumerate(file, 1):
                try:
                    content = json.loads(line)
                    # Extract the sub_type
                    sub_type = content.get('msg', {}).get('sub_type', '')
                    if sub_type and sub_type not in unique_subtypes:  # Check if sub_type is not already recorded
                        # Store the first occurrence of each sub_type along with its line example
                        unique_subtypes[sub_type] = f"Line {line_number}: {line.strip()}"
                except json.JSONDecodeError:
                    print('json decode error')
                    continue  # Skip invalid JSON lines

    return unique_subtypes


def write_subtypes_to_csv(folder_path, output_file):
    # Write the unique subtypes and their example messages to a CSV file
    print("Writing unique message subtype to file...")
    unique_subtypes_with_examples = extract_unique_subtypes_with_examples(folder_path)

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['SubType', 'ExampleMessage'])  # Column headers
        for subtype, example_message in sorted(unique_subtypes_with_examples.items()):
            writer.writerow([subtype, example_message])


def extract_and_rename_csv_files(source_dir, destination_dir):
    print("Writing intervention measures CSVs...")
    os.makedirs(destination_dir, exist_ok=True)

    for file in tqdm(os.listdir(source_dir)):
        if file.endswith('.zip'):
            zip_file_path = os.path.join(source_dir, file)
            folder_name = os.path.splitext(file)[0]

            try:
                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    # Extract only 'intervention_measures.csv' if it exists in the zip
                    if 'intervention_measures.csv' in zip_ref.namelist():
                        zip_ref.extract('intervention_measures.csv', destination_dir)
                        extracted_file = 'intervention_measures.csv'
                        new_file_name = f"{folder_name}_{extracted_file}"
                        os.rename(os.path.join(destination_dir, extracted_file),
                                  os.path.join(destination_dir, new_file_name))
            except zipfile.BadZipFile as e:
                print(f"Failed to process {file} due to a zipfile error: {e}")
                continue  # Continue to the next file


def write_intervention_measures_content(directory_path, output_path):
    print("Writing intervention measures content CSV...")
    # Placeholder for content entries and their associated information
    entries = []

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    empty_files = 0

    # Initialize a progress bar
    for file in tqdm(csv_files):
        try:
            # Construct the full file path
            file_path = os.path.join(directory_path, file)

            # Load the CSV file
            df = pd.read_csv(file_path)

            # Extract the required columns
            file_entries = df[
                ['Content', 'Agent', 'Timestamp', 'InterventionId', 'TrialId', 'TeamId', 'PlayerId']].values.tolist()

            # Append to the main entries list
            entries.extend(file_entries)
        except pd.errors.EmptyDataError:
            # print(f"Skipping empty or invalid file: {file}")
            empty_files += 1
            continue
        except Exception as e:
            print(f"An error occurred while processing {file}: {e}")

    # Convert the list of entries to a DataFrame
    all_entries_df = pd.DataFrame(entries, columns=['Content', 'Agent', 'Timestamp', 'InterventionId', 'TrialId', 'TeamId',
                                                    'PlayerId'])

    # Write the DataFrame to a new CSV file
    all_entries_df.to_csv(output_path, index=False)

    print(f"Skipped {empty_files} empty files.")


def write_intervention_measures_content_unique(directory_path, output_path):
    print("Deduplicating intervention measures content...")
    # Placeholder for unique content entries and their agents
    unique_entries = []

    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    empty_files = 0

    # Initialize a progress bar
    for file in tqdm(csv_files):
        # Construct the full file path
        file_path = os.path.join(directory_path, file)

        try:
            # Load the CSV file
            df = pd.read_csv(file_path)

            # Iterate through each row in the dataframe
            for _, row in df.iterrows():
                content = row['Content']
                agent = row['Agent']

                # Check if the content is not already in the list
                if not any(entry['Content'] == content for entry in unique_entries):
                    unique_entries.append({'Content': content, 'Agent': agent})
        except pd.errors.EmptyDataError:
            empty_files += 1
            continue

    # Convert the list of unique entries to a DataFrame
    unique_df = pd.DataFrame(unique_entries)

    # Write the DataFrame to a new CSV file
    unique_df.to_csv(output_path, index=False)

    print(f"Skipped {empty_files} empty files.")

    