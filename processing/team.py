''' module for processing team data '''

import os
import pandas as pd
from tqdm import tqdm

#############################################
# functions for collating team trial measures
#############################################

def collate_team_trial_measures(processed_trial_summary_dir_path, output_file_path):
    # List files in the directory
    files = os.listdir(processed_trial_summary_dir_path)

    # Initialize an empty list to store DataFrames
    dfs = []

    # Iterate through each file in the directory
    for file in tqdm(files):
        if file.endswith('_TeamLevel.csv'):  # Check if file ends with '_TeamLevel.csv'
            file_path = os.path.join(processed_trial_summary_dir_path, file)
            # Read the CSV file
            df = pd.read_csv(file_path)
            # Append DataFrame to the list
            dfs.append(df)

    # Concatenate all DataFrames in the list
    combined_data = pd.concat(dfs, ignore_index=True)

    # Save the combined data to a new CSV file
    combined_data.to_csv(output_file_path, index=False)

    # print("Combined trial level summary data saved to:", output_file_path)