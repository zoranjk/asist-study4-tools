import os
import requests
from tqdm import tqdm


def download_file(url, file_name):
    response = requests.get(url)
    with open(file_name, "wb") as file:
        file.write(response.content)
        # print(f"Downloaded: {file_name}")


def download_dataverse_dataset(persistent_id:str):
    # base_url = "https://dataverse.asu.edu/api/datasets/:persistentId/?persistentId="
    url = f"https://dataverse.asu.edu/api/datasets/:persistentId/?persistentId=doi:10.48349/ASU/{persistent_id}"

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error retrieving dataset details. Status code: {response.status_code}")
        return

    dataset = response.json()
    files = dataset["data"]["latestVersion"]["files"]
    directory = persistent_id
    os.makedirs(directory, exist_ok=True)

    for file in tqdm(files):
        file_id = file["dataFile"]["id"]
        file_name = file["dataFile"]["filename"]
        download_url = f"https://dataverse.asu.edu/api/access/datafile/{file_id}?persistentId=doi:10.48349/ASU/{persistent_id}"
        download_path = os.path.join(directory, file_name)
        download_file(download_url, download_path)
        # print("For loop activated")
        # print(file_name)


if __name__ == "__main__":
    download_dataverse_dataset()
    print("Download completed")