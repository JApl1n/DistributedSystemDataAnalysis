import os
import requests

url = "https://atlas-opendata.web.cern.ch/atlas-opendata/13TeV/GamGam/Data/"
samplesList = ['data15_periodD','data15_periodE','data15_periodF','data15_periodG','data15_periodH','data15_periodJ','data16_periodA','data16_periodB','data16_periodC','data16_periodD','data16_periodE','data16_periodF','data16_periodG','data16_periodK','data16_periodL']


# Target directory
DATA_DIR = "./data"

def DownloadFile(path, dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    filename = os.path.basename(path)
    filepath = os.path.join(dest_folder, filename)

    if not os.path.exists(filepath):
        print(f"Downloading {filename}...")
        response = requests.get(path, stream=True)
        if response.status_code == 200:
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded {filename} to {filepath}")
        else:
            print(f"Failed to download {filename}. HTTP Status: {response.status_code}")
    else:
        print(f"{filename} already exists. Skipping download.")

if __name__ == "__main__":
    for name in samplesList:
        path = url + name + ".root"
        DownloadFile(path, DATA_DIR)
