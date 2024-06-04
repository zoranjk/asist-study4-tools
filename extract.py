''' module for extracting zipped folder contents '''
import os
import zipfile
from tqdm import tqdm


def extract_metadata(zip_folder_path, output_path):
    os.makedirs(output_path, exist_ok=True)

    for file_name in tqdm(os.listdir(zip_folder_path)):
        if file_name.endswith('.zip'):
            zip_path = os.path.join(zip_folder_path, file_name)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.endswith('.metadata'):
                        source = zip_ref.open(member)
                        target = open(os.path.join(output_path, os.path.basename(member)), "wb")
                        with source, target:
                            target.write(source.read())


def extract_and_rename_csv_files(source_dir, destination_dir):
    os.makedirs(destination_dir, exist_ok=True)

    for file in tqdm(os.listdir(source_dir)):
        if file.endswith('.zip'):
            zip_file_path = os.path.join(source_dir, file)
            folder_name = os.path.splitext(file)[0]

            try:
                # print(f"Processing zip file: {file}")

                with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                    # Extract only 'intervention_measures.csv' if it exists in the zip
                    if 'intervention_measures.csv' in zip_ref.namelist():
                        zip_ref.extract('intervention_measures.csv', destination_dir)
                        # print(f"Extracted intervention_measures.csv from {file} to {destination_dir}")

                        extracted_file = 'intervention_measures.csv'
                        new_file_name = f"{folder_name}_{extracted_file}"
                        os.rename(os.path.join(destination_dir, extracted_file),
                                  os.path.join(destination_dir, new_file_name))
                        # print(f"Renamed {extracted_file} to {new_file_name}")
                    else:
                        pass
                        # print(f"No 'intervention_measures.csv' found in {file}.")
            except zipfile.BadZipFile as e:
                # print(f"Failed to process {file} due to a zipfile error: {e}")
                continue  # Continue to the next file
