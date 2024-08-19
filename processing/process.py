from processing import extract, dedup, etl, metadata, survey, team, timeseries
import os
from pathlib import Path
from tkinter import messagebox

def process(dl_dir_text, data_dir_text):
    confirmed = messagebox.askokcancel("Are you sure?", 'This takes a while, to continue select "OK" once you are sure the dataset and analysis directories are set properly.')
    if not confirmed:
        return

    download_dir_path = Path(dl_dir_text.get())
    data_dir_path = Path(data_dir_text.get())

    # paths
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
    teams_player_profiles_trial_measures_combined_file_path = os.path.join(data_dir_path, "teams_player_profiles_trial_measures_combined.csv")
    # TODO: make sure these file paths are correct after implementing the changes for analysis files
    teams_trial_summary_profiles_surveys_for_analysis_file_path = os.path.join(data_dir_path, "teams_trial_summary_profiles_surveys_for_analysis.csv")
    teams_trial_summary_profiles_surveys_repeats_for_analysis_file_path = os.path.join(data_dir_path, "teams_trial_summary_profiles_surveys_repeats_for_analysis.csv")
    trial_data_file_path = os.path.join(data_dir_path, "trial_data.csv")
    teams_trial_summary_profiles_surveys_scores_repeats_for_analysis_file_path = os.path.join(data_dir_path, "teams_trial_summary_profiles_surveys_scores_repeats_for_analysis.csv")
    processed_time_series_dir_path = os.path.join(data_dir_path, "processed_time_series")
    processed_time_series_cleaned_dir_path = os.path.join(data_dir_path, "processed_time_series_cleaned")
    processed_time_series_cleaned_profiles_dir_path = os.path.join(data_dir_path, "processed_time_series_cleaned_profiles")
    processed_trial_summary_dir_path = os.path.join(data_dir_path, "processed_trial_summary")
    trial_summary_profiles_file_path = os.path.join(data_dir_path, "trial_summary_profiles.csv")
    trial_summary_profiles_post_processed_file_path = os.path.join(data_dir_path, "trial_summary_profiles_post_processed.csv")
    trial_summary_profiles_cleaned_file_path = os.path.join(data_dir_path, "trial_summary_profiles_cleaned.csv")
    teams_trial_summary_profiles_surveys_file_path = os.path.join(data_dir_path, "teams_trial_summary_profiles_surveys.csv")
    processed_time_series_split_dir_path = os.path.join(data_dir_path, "processed_time_series_split")
    player_state_items_objects_dir_path = os.path.join(processed_time_series_split_dir_path, "player_states_items_objects")
    player_state_flocking_dir_path = os.path.join(processed_time_series_split_dir_path, "player_states_flocking")
    flocking_dir_path = os.path.join(processed_time_series_split_dir_path, "flocking")
    team_behaviors_asi_flocking_dir_path = os.path.join(processed_time_series_split_dir_path, "team_behaviors_asi_flocking")
    team_behaviors_flocking_dir_path = os.path.join(processed_time_series_split_dir_path, "team_behaviors_flocking")
    team_behaviors_asi_dir_path = os.path.join(processed_time_series_split_dir_path, "team_behaviors_asi")
    individual_player_profiles_team_alignment_trial_measures_for_analysis_file_path = os.path.join(data_dir_path, "individual_player_profiles_team_alignment_trial_measures_for_analysis.csv")
    analysis_dir_path = os.path.join(data_dir_path, "analysis")
    individual_players_analysis_dir_path = os.path.join(analysis_dir_path, "individual_players")
    player_profiles_anova_results_combined_analyses_file_path = os.path.join(individual_players_analysis_dir_path, "player_profiles_ANOVA_results_combined_analyses.docx")


    extract.extract_metadata(download_dir_path,
                             metadata_dir_path)

    dedup.save_unique_files(metadata_dir_path,
                            metadata_unique_dir_path)

    etl.write_subtypes_to_csv(metadata_unique_dir_path,
                              message_subtypes_unique_file_path)

    etl.extract_and_rename_csv_files(download_dir_path,
                                     intervention_measures_dir_path)

    etl.write_intervention_measures_content(intervention_measures_dir_path,
                                            intervention_measures_file_path)

    etl.write_intervention_measures_content_unique(intervention_measures_dir_path,
                                                   intervention_measures_unique_file_path)
    
    metadata.process_metadata_files(metadata_dir_path,
                                    processed_trial_summary_dir_path)

    survey.extract_and_process_files(download_dir_path,
                                     individual_surveys_dir_path)

    survey.combine_individual_measures(individual_surveys_dir_path,
                                       individual_measures_combined_file_path)
    
    survey.write_individual_measures_unique(individual_measures_combined_file_path,
                                            individual_measures_unique_file_path)

    survey.write_individual_measures_calculated_unique(individual_measures_unique_file_path,
                                                       individual_measures_calculated_unique_file_path)

    survey.write_individual_trial_measures_combined(processed_trial_summary_dir_path,
                                                    individual_trial_measures_combined_file_path)

    survey.write_individual_player_profile_trial_measures_combined(individual_measures_calculated_unique_file_path,
                                                                   individual_trial_measures_combined_file_path,
                                                                   individual_measures_combined_file_path,
                                                                   individual_player_profiles_trial_measures_combined_file_path)
    
    survey.post_hoc_calculate(individual_player_profiles_trial_measures_combined_file_path)

    survey.align_individual_player_profiles_trial_measures_combined(individual_player_profiles_trial_measures_combined_file_path,
                                                                    teams_alignment_results_combined_file_path)

    team.collate_team_trial_measures(processed_trial_summary_dir_path,
                                     trial_measures_team_combined_file_path)
    
    team.calculate_trial_level_team_profiles(individual_player_profiles_trial_measures_combined_file_path,
                                             trial_level_team_profiles_file_path)
    
    team.write_team_player_profiles_trial_measures_combined(trial_measures_team_combined_file_path,
                                                            trial_level_team_profiles_file_path,
                                                            teams_alignment_results_combined_file_path,
                                                            teams_player_profiles_trial_measures_combined_file_path)
    
    team.identify_repeat_teams(teams_player_profiles_trial_measures_combined_file_path)

    team.integrate_individual_player_profiles_trial_measures_combined(trial_measures_team_combined_file_path,
                                                                      individual_player_profiles_trial_measures_combined_file_path)
    
    timeseries.extract_and_write_time_series(metadata_unique_dir_path,
                                             processed_time_series_dir_path)

    timeseries.clean_time_series(processed_time_series_dir_path,
                                 processed_time_series_cleaned_dir_path)
    
    timeseries.add_profiles_to_time_series(processed_time_series_cleaned_dir_path,
                                           individual_player_profiles_trial_measures_combined_file_path,
                                           teams_player_profiles_trial_measures_combined_file_path,
                                           processed_time_series_cleaned_profiles_dir_path)

    timeseries.summarize_events(processed_time_series_cleaned_profiles_dir_path,
                                processed_trial_summary_dir_path)

    timeseries.collate_summaries(processed_trial_summary_dir_path,
                                 trial_summary_profiles_file_path)

    timeseries.post_process_trial_summaries(trial_summary_profiles_file_path,
                                            trial_summary_profiles_post_processed_file_path,
                                            trial_summary_profiles_cleaned_file_path,
                                            trial_level_team_profiles_file_path,
                                            teams_trial_summary_profiles_surveys_file_path)
    
    timeseries.split_time_series(processed_time_series_cleaned_profiles_dir_path,
                                 player_state_items_objects_dir_path,
                                 player_state_flocking_dir_path,
                                 flocking_dir_path,
                                 team_behaviors_asi_flocking_dir_path,
                                 team_behaviors_flocking_dir_path,
                                 team_behaviors_asi_dir_path)
    
    timeseries.split_flocking_time_series(team_behaviors_flocking_dir_path)

    timeseries.write_store_time_removed(team_behaviors_flocking_dir_path)
    
    # TODO: need to rework these with correct teams_trial_summary_profiles_surveys_for_analysis.csv
    # currently don't have the correct version of this file, needs to be converted from the
    # non-"for_analysis" version.
    # need to add function that converts the file to the correct column names.

    # analysis.rename_teams_trial_summary_profiles_surveys_for_analysis(teams_trial_summary_profiles_surveys_file_path,
    #                                                                   teams_trial_summary_profiles_surveys_for_analysis_file_path)

    # print("Writing teams trials summary profiles survey repeats...")
    # team.write_teams_trial_summary_profiles_survey_repeats(teams_player_profiles_trial_measures_combined_file_path,
    #                                                        teams_trial_summary_profiles_surveys_for_analysis_file_path,
    #                                                        teams_trial_summary_profiles_surveys_repeats_for_analysis_file_path)

    # print("Writing trial data...")
    # team.write_trial_data(trial_measures_team_combined_file_path,
    #                       teams_trial_summary_profiles_surveys_repeats_for_analysis_file_path,
    #                       trial_data_file_path)

    # print("Writing teams trials summary profiles survey scores repeats...")
    # team.write_teams_trial_summary_profiles_survey_scores_repeats(trial_data_file_path,
    #                                                               teams_trial_summary_profiles_surveys_repeats_for_analysis_file_path,
    #                                                               teams_trial_summary_profiles_surveys_scores_repeats_for_analysis_file_path)

    # everything here and up to the TODO depends on the above changes.


    # TODO: function to rename columns of individual_player_profiles_team_alignment_trial_measures.csv to create
    # the for_analysis version.
    # print("Renaming columns of some files for analysis...")
    # analysis.rename...

    # print("Writing individual players analyses...")
    # analysis.write_individual_analyses_anova(individual_player_profiles_team_alignment_trial_measures_for_analysis_file_path,
    #                                          individual_players_analysis_dir_path,
    #                                          player_profiles_anova_results_combined_analyses_file_path)