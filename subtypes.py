import os
import json
import csv
from tqdm import tqdm


def extract_unique_subtypes_with_examples(folder_path):
    files = [f for f in os.listdir(folder_path) if f.endswith('.metadata')]
    total_files = len(files)  # Total number of files to process
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

        # Print the progress
        progress_percentage = (file_index / total_files) * 100
        # print(f"Processed {file_index}/{total_files} files ({progress_percentage:.2f}%)")

    return unique_subtypes


def write_subtypes_to_csv(folder_path, output_file):
    # Write the unique subtypes and their example messages to a CSV file
    unique_subtypes_with_examples = extract_unique_subtypes_with_examples(folder_path)

    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['SubType', 'ExampleMessage'])  # Column headers
        for subtype, example_message in sorted(unique_subtypes_with_examples.items()):
            writer.writerow([subtype, example_message])

if __name__ == "__main__":
    folder_path = 'E:\\ASIST Study 4\\Study4_MetadataFiles_Unique'
    output_file = 'unique_message_subtypes_with_examples.csv'

    # Write to CSV
    write_subtypes_to_csv(folder_path, output_file)

    print(f"Unique message subtypes with example messages have been written to {output_file}.")
