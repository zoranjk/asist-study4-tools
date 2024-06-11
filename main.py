from processing import download, extract, dedup, etl, metadata, survey, team, timeseries

import json
import os

if __name__ == "__main__":

    # load config
    with open("config/config.json") as config_file:
        config = json.load(config_file)

    # paths
    data_dir_path = "data"
    download_dir_path = os.path.join(data_dir_path, "download")
    metadata_dir_path = os.path.join(data_dir_path, "metadata")
    metadata_unique_dir_path = os.path.join(data_dir_path, "metadata_unique")
    message_subtypes_unique_file_path = os.path.join(data_dir_path, "unique_message_subtypes_with_examples.csv")
    intervention_measures_dir_path = os.path.join(data_dir_path, "intervention_measures")
    intervention_measures_file_path = os.path.join(data_dir_path, "intervention_measures.csv")
    intervention_measures_unique_file_path = os.path.join(data_dir_path, "intervention_measures_unique.csv")
    processed_trial_summary_dir_path = os.path.join(data_dir_path, "processed_trial_summary")
    individual_surveys_dir_path = os.path.join(data_dir_path, "individual_surveys")
    individual_measures_combined_file_path = os.path.join(data_dir_path, "individual_measures_combined.csv")
    individual_measures_unique_file_path = os.path.join(data_dir_path, "individual_measures_unique.csv")
    individual_measures_calculated_unique_file_path = os.path.join(data_dir_path, "individual_measures_calculated_unique.csv")
    individual_trial_measures_combined_file_path = os.path.join(data_dir_path, "individual_trial_measures_combined.csv")
    individual_player_profiles_trial_measures_combined_file_path = os.path.join(data_dir_path, "individual_player_profiles_trial_measures_combined.csv")
    teams_alignment_results_combined_file_path = os.path.join(data_dir_path, "teams_alignment_results_combined.csv")
    trial_measures_team_combined_file_path = os.path.join(data_dir_path, "trial_measures_team_combined.csv")
    trial_level_team_profiles_file_path = os.path.join(data_dir_path, "trial_level_team_profiles.csv")
    team_player_profiles_trial_measures_combined_file_path = os.path.join(data_dir_path, "team_player_profiles_trial_measures_combined.csv")
    # TODO: this file path isn't necessary until i figure out those missing CSV files
    # team_trials_summary_profiles_surveys_repeats_file_path = os.path.join(data_dir_path, "team_trials_summary_profiles_surveys_repeats.csv")
    processed_time_series_dir_path = os.path.join(data_dir_path, "processed_time_series")
    processed_time_series_cleaned_dir_path = os.path.join(data_dir_path, "processed_time_series_cleaned")
    processed_time_series_cleaned_profiled_dir_path = os.path.join(data_dir_path, "processed_time_series_cleaned_profiled")
    processed_trial_summary_dir_path = os.path.join(data_dir_path, "processed_trial_summary")
    trial_summary_profiled_file_path = os.path.join(data_dir_path, "trial_summary_profiled.csv")

    # print("Downloading dataset...")
    # download.download_dataverse_dataset(config['dataset']['persistent_id'],
    #                                     download_dir_path)

    # print("Extracting metadata files...")
    # extract.extract_metadata(download_dir_path,
    #                          metadata_dir_path)

    # print("Deduplicating metadata...")
    # dedup.save_unique_files(metadata_dir_path,
    #                         metadata_unique_dir_path)

    # print("Writing unique message subtype to file...")
    # etl.write_subtypes_to_csv(metadata_unique_dir_path,
    #                           message_subtypes_unique_file_path)

    # print("Writing intervention measures CSVs...")
    # etl.extract_and_rename_csv_files(download_dir_path,
    #                                  intervention_measures_dir_path)

    # print("Writing intervention measures content CSV...")
    # etl.write_intervention_measures_content(intervention_measures_dir_path,
    #                                         intervention_measures_file_path)

    # print("Deduplicating intervention measures content...")
    # etl.write_intervention_measures_content_unique(intervention_measures_dir_path,
    #                                                intervention_measures_unique_file_path)
    
    # print("Processing metadata files...")
    # metadata.process_metadata_files(metadata_dir_path,
    #                                 processed_trial_summary_dir_path)

    # print("Processing individual surveys...")
    # survey.extract_and_process_files(download_dir_path,
    #                                  individual_surveys_dir_path)

    # print("Combining individual measures...")
    # survey.combine_individual_measures(individual_surveys_dir_path,
    #                                    individual_measures_combined_file_path)
    
    # print("Writing unique individual measures...")
    # survey.write_individual_measures_unique(individual_measures_combined_file_path,
    #                                         individual_measures_unique_file_path)

    # print("Writing calculated unique individual measures...")
    # survey.write_individual_measures_calculated_unique(individual_measures_unique_file_path,
    #                                                    individual_measures_calculated_unique_file_path)

    # print("Writing combined individual trial measures...")
    # survey.write_individual_trial_measures_combined(processed_trial_summary_dir_path,
    #                                                 individual_trial_measures_combined_file_path)

    # print("Writing combined individual player profile trial measures...")
    # survey.write_individual_player_profile_trial_measures_combined(individual_measures_calculated_unique_file_path,
    #                                                                individual_trial_measures_combined_file_path,
    #                                                                individual_measures_combined_file_path,
    #                                                                individual_player_profiles_trial_measures_combined_file_path)
    
    # print("Doing in-place post-hoc calculations on combined individual player profile trial measures...")
    # survey.post_hoc_calculate(individual_player_profiles_trial_measures_combined_file_path)

    # print("Writing combined team alignment results...")
    # survey.align_individual_player_profiles_trial_measures_combined(individual_player_profiles_trial_measures_combined_file_path,
    #                                                                 teams_alignment_results_combined_file_path)

    # print("Collating team trial measures...")
    # team.collate_team_trial_measures(processed_trial_summary_dir_path,
    #                                  trial_measures_team_combined_file_path)
    
    # print("Calculating trial level team profiles...")
    # team.calculate_trial_level_team_profiles(individual_player_profiles_trial_measures_combined_file_path,
    #                                          trial_level_team_profiles_file_path)
    
    # print("Writing combined team player profiles trial measures...")
    # team.write_team_player_profiles_trial_measures_combined(trial_measures_team_combined_file_path,
    #                                                         trial_level_team_profiles_file_path,
    #                                                         teams_alignment_results_combined_file_path,
    #                                                         team_player_profiles_trial_measures_combined_file_path)
    
    # print("Identifying repeat teams in combined team player profiles trial measures...")
    # team.identify_repeat_teams(team_player_profiles_trial_measures_combined_file_path)

    # # TODO: Figure out where to get Study_4_Teams_TrialSummary_Profiles_Surveys_Repeats_ForAnalysis.csv
    # # for this and next function call.
    # print("Writing teams trials summary profiles survey repeats...")
    # team.write_teams_trial_summary_profiles_survey_repeats(team_player_profiles_trial_measures_combined_file_path,
    #                                                        teams_trial_summary_profiles_surveys_file_path)
    
    # print("Writing teams trials summary profiles survey scores repeats...")
    # team.write_teams_trial_summary_profiles_survey_scores_repeats()

    # print("Integrating combined team trial measures into combined individual player profiles trial measures...")
    # team.integrate_individual_player_profiles_trial_measures_combined(trial_measures_team_combined_file_path,
    #                                                                   individual_player_profiles_trial_measures_combined_file_path)
    
    # print("Processing time series messages...")
    # timeseries.extract_and_write_time_series(metadata_unique_dir_path,
    #                                          processed_time_series_dir_path)

    # print("Cleaning time series messages...")
    # timeseries.clean_time_series(processed_time_series_dir_path,
    #                              processed_time_series_cleaned_dir_path)
    
    # print("Adding profiles to time series data...")
    # timeseries.add_profiles_to_time_series(processed_time_series_cleaned_dir_path,
    #                                        individual_player_profiles_trial_measures_combined_file_path,
    #                                        team_player_profiles_trial_measures_combined_file_path,
    #                                        processed_time_series_cleaned_profiled_dir_path)

    # print("Summarizing time series events...")
    # timeseries.summarize_events(processed_time_series_cleaned_profiled_dir_path,
    #                             processed_trial_summary_dir_path)

    print("Writing profiled trial summaries...")
    timeseries.collate_summaries(processed_trial_summary_dir_path,
                                 trial_summary_profiled_file_path)
