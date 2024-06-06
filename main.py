import download
import extract
import dedup
import etl
from processing import metadata, survey

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

    # print("Writing unique calculated individual measures...")
    # survey.write_individual_measures_calculated_unique(individual_measures_combined_file_path,
    #                                                    individual_measures_calculated_unique_file_path)

    # print("Writing combined individual trial measures...")
    # survey.write_individual_trial_measures_combined(processed_trial_summary_dir_path,
    #                                                 individual_trial_measures_combined_file_path)

    print("Writing combined individual player profile trial measures...")
    survey.write_individual_player_profile_trial_measures_combined(individual_measures_calculated_unique_file_path,
                                                                   individual_trial_measures_combined_file_path,
                                                                   individual_measures_combined_file_path,
                                                                   individual_player_profiles_trial_measures_combined_file_path)