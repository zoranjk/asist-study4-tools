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
