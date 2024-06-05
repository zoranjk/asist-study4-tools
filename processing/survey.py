''' module for processing survey data '''

import os
import zipfile
import pandas as pd
import numpy as np
from tqdm import tqdm

#############################################
# functions for processing individual surveys
#############################################

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


def extract_and_process_files(source_dir, destination_dir):
    os.makedirs(destination_dir, exist_ok=True)

    for file in tqdm(os.listdir(source_dir)):
        if file.endswith('.zip'):
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


#############################################
# functions for combining individual measures
#############################################

def combine_individual_measures(individual_survey_dir_path, output_file_path):
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

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file_path, index=False)


##########################################
# functions for individual measures unique
##########################################

def write_individual_measures_unique(individual_measures_combined_file_path, output_file_path):
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
    compiled_df.to_csv(output_file_path, index=False)


#######################################################
### functions for individual measures calculated unique
#######################################################

def load_and_rename_columns(file_path):
    # Load the dataset
    df = pd.read_csv(file_path)

    # Rename columns
    df.rename(columns={
        'DEMO-1': 'Age',
        'DEMO-2': 'Sex',
        'DEMO-5': 'Ethnicity'
    }, inplace=True)

    return df


def calculate_averages(df):
    # Calculate 'PsychCollect_avg' for complete responses
    psych_collect_cols = [f'PSY_COL-{i}' for i in range(1, 16)]
    df['PsychCollect_avg'] = df[psych_collect_cols].apply(lambda row: row.mean() if row.notnull().all() else None,
                                                          axis=1)

    # Calculate 'SociableDom_avg' for complete responses
    sociable_dom_cols = [f'SOC_DOM-{i}' for i in range(1, 16)]
    df['SociableDom_avg'] = df[sociable_dom_cols].apply(lambda row: row.mean() if row.notnull().all() else None, axis=1)

    return df


def calculate_reading_mind_score(df):
    conditions = {
        'RMIE_short-1': [2],
        'RMIE_short-2': [2],
        'RMIE_short-3': [3],
        'RMIE_short-4': [1],
        'RMIE_short-5': [1],
        'RMIE_short-6': [1],
        'RMIE_short-7': [1],
        'RMIE_short-8': [2],
        'RMIE_short-9': [1],
        'RMIE_short-10': [4],
    }

    def score(row):
        if row[conditions.keys()].isnull().all():
            return np.nan  # Return NaN if all RMIE responses are missing
        return sum(row[col] in valid_answers for col, valid_answers in conditions.items() if col in row)

    df['ReadingMind_score'] = df.apply(score, axis=1)

    return df


def calculate_spatial_ability_avg(df):
    sbsod_cols = [f'SBSOD-{i}' for i in range(1, 16)]
    reverse_coded_items = [2, 6, 8, 10, 11, 12, 13, 15]

    def reverse_code(item, value):
        if item in reverse_coded_items:
            return abs(8 - value)
        return value

    def spatial_ability_score(row):
        scores = []
        for i, col in enumerate(sbsod_cols, 1):
            if not np.isnan(row[col]):
                scores.append(reverse_code(i, row[col]))
        if scores:
            return np.mean(scores)
        return None  # Return None if there are no SBSOD responses

    df['SpatialAbility_avg'] = df.apply(spatial_ability_score, axis=1)

    return df


def calculate_mc_prof_avg(df):
    mc_prof_cols = [f'MC_PROF_15-{i}' for i in range(1, 16)]  # Include all columns through MC_PROF_15-15

    def mc_prof_score(row):
        if row[mc_prof_cols].isnull().all():
            return np.nan  # Return NaN if all MC_PROF_15 responses are missing

        # Initialize the score with NaN, which will be updated if there are valid responses
        score = np.nan

        # Check for non-null values and perform the calculation only if they exist
        if not row[mc_prof_cols[:7]].isnull().any():
            # Applying the transformation as per the Excel formula
            transformed_scores = (
                                         np.nansum([
                                             row['MC_PROF_15-1'] * 5,
                                             row['MC_PROF_15-2'] * 5,
                                             row['MC_PROF_15-3'] * 5,
                                             row['MC_PROF_15-4'] * 14.285,
                                             row['MC_PROF_15-5'] * 14.285,
                                             row['MC_PROF_15-6'] * 14.285,
                                             row['MC_PROF_15-7'] * 14.285
                                         ]) + np.nansum(row[mc_prof_cols[7:]])) / 15
            score = transformed_scores

        return score

    df['MCProf_avg'] = df.apply(mc_prof_score, axis=1)

    return df


def calculate_above_median(df, columns):
    # Calculate the median for the specified columns
    medians = df[columns].median()

    # For each column, create a new binary column indicating whether the score is above or equal to the median
    for col in columns:
        median_col_name = f'{col}_above_median'
        df[median_col_name] = df[col].ge(medians[col]).astype(int)

    return df


def calculate_potential_scores_and_categories(df):
    # Calculate 'Teamwork_potential_score'
    df['Teamwork_potential_score'] = df['PsychCollect_avg_above_median'] \
                                     + df['SociableDom_avg_above_median'] \
                                     + df['ReadingMind_score_above_median']

    # Calculate 'Taskwork_potential_score'
    df['Taskwork_potential_score'] = df['SpatialAbility_avg_above_median'] \
                                     + df['MCProf_avg_above_median']

    # Categorize 'Teamwork_potential_category'
    df['Teamwork_potential_category'] = df['Teamwork_potential_score'].apply(
        lambda x: 'High Teamwork Pot' if x >= 2 else 'Low Teamwork Pot'
    )

    # Categorize 'Taskwork_potential_category_liberal'
    df['Taskwork_potential_category_liberal'] = df['Taskwork_potential_score'].apply(
        lambda x: 'High Taskwork Pot' if x >= 1 else 'Low Taskwork Pot'
    )

    # Categorize 'Taskwork_potential_category_conservative'
    df['Taskwork_potential_category_conservative'] = df['Taskwork_potential_score'].apply(
        lambda x: 'High Taskwork Pot' if x == 2 else 'Low Taskwork Pot'
    )

    return df


def track_individual_missing_data(df, columns):
    for col in columns:
        missing_col_name = f'{col}_missing'
        # Create a binary column indicating if the data is missing for the participant
        df[missing_col_name] = df[col].isnull().astype(int).apply(lambda x: 'Missing' if x == 1 else 'Not Missing')
    return df


def write_individual_measures_calculated_unique(individual_measures_unique_file_path, output_file_path):
    # Load the dataset and rename columns
    df = load_and_rename_columns(individual_measures_unique_file_path)

    # Perform the existing calculations
    df = calculate_averages(df)
    df = calculate_reading_mind_score(df)
    df = calculate_spatial_ability_avg(df)
    df = calculate_mc_prof_avg(df)

    # Calculate if scores are above or equal to the median for each measure
    df = calculate_above_median(df, [
        'PsychCollect_avg', 'SociableDom_avg', 'ReadingMind_score',
        'SpatialAbility_avg', 'MCProf_avg'
    ])

    # Calculate potential scores and categories
    df = calculate_potential_scores_and_categories(df)

    # Track missing data for individual participants
    columns_to_check_for_missing = [
        'PsychCollect_avg', 'SociableDom_avg', 'ReadingMind_score',
        'SpatialAbility_avg', 'MCProf_avg'
    ]
    df = track_individual_missing_data(df, columns_to_check_for_missing)

    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_file_path, index=False)


##########################################################
# functions for writing individual trial measures combined
##########################################################

def write_individual_trial_measures_combined(processed_trial_summary_dir_path, output_file_path):
    # List files in the directory
    files = os.listdir(processed_trial_summary_dir_path)

    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate through each file in the directory
    for file in tqdm(files):
        if file.endswith('_IndivLevel.csv'):  # Check if file ends with '_IndivLevel.csv'
            file_path = os.path.join(processed_trial_summary_dir_path, file)
            # Read the CSV file
            df = pd.read_csv(file_path)
            # Append DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames in the list
    combined_data = pd.concat(dfs, ignore_index=True)

    # Save the combined data to a new CSV file
    combined_data.to_csv(output_file_path, index=False)

    