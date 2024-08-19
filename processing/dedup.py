''' data deduplication functions '''

import os
import hashlib
import shutil
from tqdm import tqdm

def compute_checksum(file_path, chunk_size=8192):
    """Compute the checksum of a file."""
    hash_algorithm = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            hash_algorithm.update(data)
    return hash_algorithm.hexdigest()


def find_unique_files(folder_path):
    """Find unique files in the given folder."""
    file_checksums = {}
    unique_files = []
    files_processed = 0

    for root, _, files in os.walk(folder_path):
        for filename in tqdm(files):
            file_path = os.path.join(root, filename)
            checksum = compute_checksum(file_path)
            if checksum not in file_checksums:
                unique_files.append(file_path)
                file_checksums[checksum] = file_path
            files_processed += 1
    return unique_files


def save_unique_files(folder_path, output_folder):
    """Save unique files to the output folder."""
    print("Deduplicating metadata...")
    os.makedirs(output_folder, exist_ok=True)
    unique_files = find_unique_files(folder_path)

    for file_path in unique_files:
        filename = os.path.basename(file_path)
        output_path = os.path.join(output_folder, filename)
        shutil.copyfile(file_path, output_path)

