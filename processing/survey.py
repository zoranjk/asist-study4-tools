''' module for processing survey data '''

import os
import zipfile
import pandas as pd
from tqdm import tqdm


def extract_specific_files(zip_file_path, destination_dir, file_names):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Extract only the specified files if they exist in the zip
        extracted_files = []
        for file_name in file_names:
            if file_name in zip_ref.namelist():
                zip_ref.extract(file_name, destination_dir)
                extracted_files.append(os.path.join(destination_dir, file_name))
        return extracted_files


def read_trial_id_from_measures(file_path):
    try:
        df = pd.read_csv(file_path)
        # Check for the presence of either 'TrialID', 'Trial_ID', or 'TrialId'
        possible_columns = ['TrialID', 'Trial_ID', 'TrialId']
        trial_id_column = next((col for col in possible_columns if col in df.columns), None)
        if trial_id_column:
            return df[trial_id_column].iloc[0]
        else:
            print(f"None of the expected trial ID columns found in {file_path}.")
            return None
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return None


def process_individual_measures(trial_id, individual_measures_path, destination_dir):
    if trial_id:
        df = pd.read_csv(individual_measures_path)
        df.insert(0, 'trial_id', trial_id)
        output_file_name = f"{trial_id}_individual_measures.csv"
        df.to_csv(os.path.join(destination_dir, output_file_name), index=False)
        # print(f"Processed and saved {output_file_name}")


def extract_and_process_files(source_dir, destination_dir):
    # if not os.path.exists(destination_dir):
    os.makedirs(destination_dir, exist_ok=True)
    # print(f"Created destination directory: {destination_dir}")

    for file in tqdm(os.listdir(source_dir)):
        if file.endswith('.zip'):
            # print(f"Processing zip file: {file}")
            zip_file_path = os.path.join(source_dir, file)
            extracted_files = extract_specific_files(zip_file_path, destination_dir,
                                                     ['trial_measures.csv', 'individual_measures.csv'])

            trial_measures_path = [f for f in extracted_files if 'trial_measures.csv' in f]
            individual_measures_path = [f for f in extracted_files if 'individual_measures.csv' in f]

            if trial_measures_path:
                trial_id = read_trial_id_from_measures(trial_measures_path[0])
                if trial_id and individual_measures_path:
                    process_individual_measures(trial_id, individual_measures_path[0], destination_dir)
                else:
                    print("individual_measures.csv not found or trial_id could not be determined.")
            else:
                print("trial_measures.csv not found.")

if __name__ == "__main__":
    source_directory = 'E:\\ASIST Study 4\\DataRawZips-2-8-2024'
    destination_directory = 'E:\\ASIST Study 4\\Study4_IndividualSurveys_CSVs'

    extract_and_process_files(source_directory, destination_directory)

