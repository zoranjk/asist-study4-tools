import download
import json

if __name__ == "__main__":

    # load config
    with open("config/config.json") as config_file:
        config = json.load(config_file)

    # download dataset
    print("Downloading dataset...")
    download.download_dataverse_dataset(config['dataset']['persistent_id'])