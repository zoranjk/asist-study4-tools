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

def combine_individual_measures(individual_survey_dir_path, output_file_path):
    # Define the folder containing the CSV files
    # folder_path = 'E:\\ASIST Study 4\\Study4_IndividualSurveys_CSVs'

    # List all CSV files in the directory
    csv_files = [file for file in os.listdir(individual_survey_dir_path) if file.endswith('_individual_measures.csv')]

    # Initialize an empty list to store DataFrames
    dfs = []

    # Loop through the CSV files and append them to the dfs list
    for file in tqdm(csv_files):
        file_path = os.path.join(individual_survey_dir_path, file)
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Concatenate all DataFrames in the list, retaining all columns
    combined_df = pd.concat(dfs, axis=0, ignore_index=True, sort=False)

    # Define the path for the output file
    # output_file_path = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_individual_measures_combined.csv'

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file_path, index=False)

    # print(f'Combined CSV saved to {output_file_path}')


def write_individual_measures_unique(individual_measures_combined_file_path, output_file_path):
    # Load the CSV file
    # file_path = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_individual_measures_combined.csv'
    data = pd.read_csv(individual_measures_combined_file_path)

    # Initialize a list to hold the data for the new DataFrame
    compiled_data = []

    # Iterate over each unique PLAYER_ID to compile its information
    for player_id in tqdm(data['PLAYER_ID'].unique()):
        player_data = data[data['PLAYER_ID'] == player_id]

        # Get all trial_ids for this PLAYER_ID
        unique_trial_ids = player_data['trial_id'].unique()

        # Initialize the info dictionary with necessary information from the first row
        info = player_data.iloc[0].to_dict()

        # Initialize Number_of_Trials
        info['Number_of_Trials'] = len(unique_trial_ids)

        # Iterate over each unique trial_id and teamed_with_ids to create dynamic columns
        for i, trial_id in enumerate(unique_trial_ids, start=1):
            # Get all unique PLAYER_IDs associated with this trial_id, excluding the current PLAYER_ID
            teamed_with_ids = data[(data['trial_id'] == trial_id) & (data['PLAYER_ID'] != player_id)]['PLAYER_ID'].unique()

            # Dynamic column names for trial_id and associated PLAYER_IDs
            trial_column_name = f'associated_trial_id_{i}'
            teamed_column_name = f'Teamed_With_{i}'

            # Populate info dictionary with trial_id and teamed_with_ids
            info[trial_column_name] = trial_id
            info[teamed_column_name] = ', '.join(map(str, teamed_with_ids))

        # Append this compiled info to our list
        compiled_data.append(info)

    # Convert compiled_data to a DataFrame
    compiled_df = pd.DataFrame(compiled_data)

    # Rename 'trial_id' column to 'source_trial_id' in the DataFrame
    if 'trial_id' in compiled_df.columns:
        compiled_df.rename(columns={'trial_id': 'profile_source_trial_id'}, inplace=True)

    # Columns to be deleted
    columns_to_delete = [
        'PRSS-1', 'PRSS-2', 'PRSS-3', 'PRSS-4', 'PRSS-5', 'PRSS-6', 'PRSS-7', 'PRSS-8', 'PRSS-9',
        'SATIS-1', 'SATIS-2', 'SATIS-3', 'SATIS-4', 'SATIS-5',
        'SELF_EFF-1', 'SELF_EFF-2', 'SELF_EFF-3', 'SELF_EFF-4', 'SELF_EFF-5', 'SELF_EFF-6', 'SELF_EFF-7', 'SELF_EFF-8',
        'EVAL-1', 'EVAL-2', 'EVAL-3', 'EVAL-4', 'EVAL-5', 'EVAL-6',
        'TEAM_FAMIL-1', 'TEAM_FAMIL-2', 'TEAM_FAMIL-3'
    ]

    # Delete specified columns if they exist in DataFrame to avoid KeyError
    compiled_df = compiled_df.drop(columns=[col for col in columns_to_delete if col in compiled_df.columns], errors='ignore')

    # Save the compiled DataFrame to a new CSV file
    # output_file_path = 'C:\\Post-doc Work\\ASIST Study 4\\Study_4_individual_measures_UniqueOnly.csv'
    compiled_df.to_csv(output_file_path, index=False)

    # print(f"Unique player profiles survey measures saved to {output_file_path}")



if __name__ == "__main__":
    source_directory = 'E:\\ASIST Study 4\\DataRawZips-2-8-2024'
    destination_directory = 'E:\\ASIST Study 4\\Study4_IndividualSurveys_CSVs'

    extract_and_process_files(source_directory, destination_directory)

