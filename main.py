import download
import extract
import dedup
import subtypes

import json

if __name__ == "__main__":

    # load config
    with open("config/config.json") as config_file:
        config = json.load(config_file)

    print("Downloading dataset...")
    pid = config['dataset']['persistent_id']
    download.download_dataverse_dataset(pid)

    print("Extracting metadata files...")
    download_path = pid
    metadata_path = f'{pid}_metadata'
    extract.extract_metadata(download_path, metadata_path)

    print("Deduplicating metadata...")
    dedup_metadata_path = f'{metadata_path}_unique'
    dedup.save_unique_files(metadata_path, dedup_metadata_path)

    print("Writing unique message subtype to file...")
    subtypes_file_path = 'unique_message_subtypes_with_examples.csv'
    subtypes.write_subtypes_to_csv(dedup_metadata_path, subtypes_file_path)