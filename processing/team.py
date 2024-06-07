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


#####################################################
# functions for calculating trial level team profiles
#####################################################

def load_data(file_path):
    # Load the dataset
    df = pd.read_csv(file_path)
    return df


def calculate_team_averages_and_members(df, group_by_col, source_target_map):
    # Group by 'trial_ID' and calculate the mean for the specified columns
    team_averages = df.groupby(group_by_col).agg({source: 'mean' for source in source_target_map.keys()}).reset_index()

    # Calculate the sum, max, min, and range of 'Number_of_Trials' for each group
    team_trials_stats = df.groupby(group_by_col)['Number_of_Trials'].agg(['sum', 'max', 'min']).reset_index()
    team_trials_stats['Number_of_Trials_Range'] = team_trials_stats['max'] - team_trials_stats['min']
    team_trials_stats.rename(columns={'sum': 'Team_Number_of_Trials_Sum', 'max': 'Number_of_Trials_Max', 'min': 'Number_of_Trials_Min'}, inplace=True)

    # Merge the team averages with the team trials stats
    team_averages = pd.merge(team_averages, team_trials_stats, on=group_by_col)

    # Rename columns to the target variable names
    team_averages.rename(columns=source_target_map, inplace=True)

    # Additionally, create a list of participant_IDs for each trial_id
    team_members = df.groupby(group_by_col)['participant_ID'].apply(list).reset_index()
    team_members.rename(columns={'participant_ID': 'Team_Members'}, inplace=True)

    # Merge the team averages with the team members based on 'trial_id'
    team_profiles = pd.merge(team_averages, team_members, on=group_by_col)

    return team_profiles


def calculate_team_above_median(team_profiles, variables):
    # Calculate the median for each variable
    medians = team_profiles[variables].median()

    # Create binary columns for each variable to indicate if the team score is above or equal to the median
    for var in variables:
        above_median_var = f'{var}_above_median'
        team_profiles[above_median_var] = (team_profiles[var] >= medians[var]).astype(int)

    return team_profiles


def calculate_team_potential_categories(team_profiles):
    # Calculate team potential categories based on the sum of certain above_median columns
    teamwork_vars = [
        'Team_PsychCollect_avg_above_median',
        'Team_SociableDom_avg_above_median',
        'Team_ReadingMind_score_above_median'
    ]
    taskwork_vars_liberal = [
        'Team_SpatialAbility_avg_above_median',
        'Team_MCProf_avg_above_median'
    ]

    taskwork_vars_conservative = taskwork_vars_liberal  # Same variables, different condition

    # Teamwork potential category
    team_profiles['Team_teamwork_potential_score'] = team_profiles[teamwork_vars].sum(axis=1)
    team_profiles['Team_teamwork_potential_category'] = team_profiles['Team_teamwork_potential_score'].apply(
        lambda x: 'High Teamwork Pot' if x >= 2 else 'Low Teamwork Pot'
    )

    # Taskwork potential category - liberal
    team_profiles['Team_taskwork_potential_score_liberal'] = team_profiles[taskwork_vars_liberal].sum(axis=1)
    team_profiles['Team_taskwork_potential_category_liberal'] = team_profiles[
        'Team_taskwork_potential_score_liberal'].apply(
        lambda x: 'High Taskwork Pot' if x >= 1 else 'Low Taskwork Pot'
    )

    # Taskwork potential category - conservative
    team_profiles['Team_taskwork_potential_score_conservative'] = team_profiles[taskwork_vars_conservative].sum(axis=1)
    team_profiles['Team_taskwork_potential_category_conservative'] = team_profiles[
        'Team_taskwork_potential_score_conservative'].apply(
        lambda x: 'High Taskwork Pot' if x == 2 else 'Low Taskwork Pot'
    )

    return team_profiles


def calculate_common_categories(df, group_by_col):
    # Function to calculate the most common category for teamwork and taskwork potentials
    def most_common(x):
        # Using pandas mode function which can handle non-numeric data
        modes = x.mode()
        if not modes.empty:
            return modes[0]  # Return the first mode in case of multiple modes
        else:
            return None

    common_categories = df.groupby(group_by_col)[
        ['Team_teamwork_potential_category', 'Team_taskwork_potential_category_liberal', 'Team_taskwork_potential_category_conservative']
    ].agg(most_common).reset_index()

    common_categories.rename(columns={
        'Team_teamwork_potential_category': 'Team_teamwork_potential_category_fromAggregate',
        'Team_taskwork_potential_category_liberal': 'Team_taskwork_potential_category_fromLiberalAggregate',
        'Team_taskwork_potential_category_conservative': 'Team_taskwork_potential_category_fromConservativeAggregate'
    }, inplace=True)

    return common_categories


def calculate_prss_team_averages(df):
    # Group by 'trial_id' and calculate the mean for PRSS-related columns
    prss_averages = df.groupby('trial_id').agg({
        'PRSS_avg': 'mean',
        'PRSS_transition': 'mean',
        'PRSS_action': 'mean',
        'PRSS_interpersonal': 'mean'
    }).reset_index()

    # Rename the columns to the specified target variable names
    prss_averages.rename(columns={
        'PRSS_avg': 'PRSS_TeamAvg',
        'PRSS_transition': 'PRSS_transition_TeamAvg',
        'PRSS_action': 'PRSS_action_TeamAvg',
        'PRSS_interpersonal': 'PRSS_interpersonal_TeamAvg'
    }, inplace=True)

    return prss_averages


def calculate_additional_team_averages(df):
    # Define the columns for which we need to calculate averages
    additional_columns = [
        'SATIS_score', 'SATIS_workedTogether', 'SATIS_teamPlan', 'SATIS_teamAgain',
        'SATIS_teamCapable', 'EFF_workEthic', 'EFF_overcomeProblems', 'EFF_planStrategy',
        'EFF_maintainPositivity', 'EFF_disposeBombs', 'EFF_speedRun', 'EFF_knowledgeCoord',
        'EFF_roleCoord', 'advisorEVAL_improvedScore', 'AdvisorEVAL_improvedCoord',
        'advisorEVAL_comfortDependingOn', 'advisorEVAL_understoodRecommends', 'advisorEVAL_wasTrustworthy'
    ]

    # Group by 'trial_id' and calculate the mean for each specified column
    additional_averages = df.groupby('trial_id').agg({col: 'mean' for col in additional_columns}).reset_index()

    # Rename the columns to include '_TeamAvg' suffix for clarity
    additional_averages.rename(columns={col: f'{col}_TeamAvg' for col in additional_columns}, inplace=True)

    return additional_averages


def calculate_trial_level_team_profiles(individual_player_profiles_trial_measures_combined_file_path,
                                        output_file_path):
    # Load the dataset
    df = load_data(individual_player_profiles_trial_measures_combined_file_path)

    # Define the mapping from source variables to target variables after averaging across team members
    source_target_map = {
        'PsychCollect_avg': 'Team_PsychCollect_avg',
        'SociableDom_avg': 'Team_SociableDom_avg',
        'ReadingMind_score': 'Team_ReadingMind_score',
        'SpatialAbility_avg': 'Team_SpatialAbility_avg',
        'MCProf_avg': 'Team_MCProf_avg',
        # Add any other variables you need to map here
    }

    # Calculate team averages and members
    team_profiles = calculate_team_averages_and_members(df, 'trial_id', source_target_map)

    # Calculate PRSS-related team averages
    prss_averages = calculate_prss_team_averages(df)
    team_profiles = pd.merge(team_profiles, prss_averages, on='trial_id', how='left')

    # Calculate additional team averages
    additional_averages = calculate_additional_team_averages(df)
    # Debugging print statement to check columns before merging
    # print("Columns in additional_averages before merging:", additional_averages.columns.tolist())
    team_profiles = pd.merge(team_profiles, additional_averages, on='trial_id', how='left')
    # Debugging print statement to check columns after merging
    # print("Columns in team_profiles after merging with additional_averages:", team_profiles.columns.tolist())

    # Calculate if team averages are above or equal to the median for each measure
    team_variables = list(source_target_map.values())
    team_profiles = calculate_team_above_median(team_profiles, team_variables)

    # Calculate team potential categories
    team_profiles = calculate_team_potential_categories(team_profiles)

    # Calculate common categories based on individual classifications
    # Pass 'team_profiles' instead of 'df' here
    common_categories = calculate_common_categories(team_profiles, 'trial_id')
    team_profiles = pd.merge(team_profiles, common_categories, on='trial_id', how='left')

    # Save the new DataFrame to a CSV file
    team_profiles.to_csv(output_file_path, index=False)
    # print(f'Team profiles saved to {output_file_path}')
