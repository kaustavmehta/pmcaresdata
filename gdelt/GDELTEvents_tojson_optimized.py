""" 
This is some base code that uses the GDELTPyR library to download datasets of our
requirement (based on tables GDELT has + date range, among others) and exported filtered
data that contains events identified to have taken place in India. This isn't a 
guarantee of the news site being Indian, but the probability of it reporting in/about
India is high enough, and this will be assumed to be a comprehensive "enough" dataset.
"""

# Using GDELTPyR library from https://github.com/linwoodc3/gdeltPyR/tree/proxy_fix
# Have to clone the above .git and then follow their build instructions to install the
# proxy fix GDELTPyR to avoid the proxy error the default module throws up.

import gdelt
import pandas as pd
import numpy as np
import time, gc, argh 
from tqdm import tqdm
import requests

# Format of day = '2020 12 17' (YYYY MM DD)

def downloadINGDELT(day):
    "Downloads and returns filtered Indian English and Translingual GDELT Events data appended into one Pandas dataframe"

    start = time.time()
    attempt = 0

    # Instantiating to query GDELT V2
    gd2 = gdelt.gdelt(version=2)

    # Downloading and reading English GDELT Events Data 
    tqdm.write("{0} {1} {2}".format("Downloading and reading English GDELT Events data for", day, "..."))
    
    while True:
        try:
            eng = pd.read_json(gd2.Search([day], table='events', coverage=True, output='json'))
            tqdm.write("Downloaded.")
        except requests.exceptions.RequestException:
            attempt =+ 1
            tqdm.write("Error during English download, retrying #{0}...".format(str(attempt)))
            if 'eng' in locals() is True: 
                del eng
                gc.collect()
            time.sleep(5)
            continue
        except:
            raise
        break

    attempt = 0

    #Filtering out irrelevant columns
    extractedEng = eng.get(['GLOBALEVENTID',
        'DATEADDED',
        'SOURCEURL',
        'EventCode',
        'CAMEOCodeDescription',
        'QuadClass',
        'GoldsteinScale',
        'AvgTone',
        'ActionGeo_CountryCode',
        'ActionGeo_FullName', 
        'ActionGeo_Lat', 
        'ActionGeo_Long'])

    # Deleting (future) unused data variable
    del eng
    tqdm.write("Dropped irrelevant columns and cleared memory. (English)")

    # Downloading and reading Translingual GDELT Events Data
    tqdm.write("{0} {1} {2}".format("Downloading and reading Translingual GDELT Events data for", day, "..."))
    
    while True:
        try:
            trans = pd.read_json(gd2.Search([day], table='events', translation=True, coverage=True, output='json'))
            tqdm.write("Downloaded.")
        except requests.exceptions.RequestException:
            attempt =+ 1
            tqdm.write("Error during Translingual download, retrying #{0}...".format(str(attempt)))
            if 'trans' in locals() is True:
                del trans
                gc.collect()
            time.sleep(5)
            continue
        except:
            raise
        break

    # Deleting irrelevant columns from Trans and merging English and Translingual dataframes
    tqdm.write("Dropping irrelevant columns (Translingual) and merging datasets.")
    data = extractedEng.append(trans.get(['GLOBALEVENTID',
        'DATEADDED',
        'SOURCEURL',
        'EventCode',
        'CAMEOCodeDescription',
        'QuadClass',
        'GoldsteinScale',
        'AvgTone',
        'ActionGeo_CountryCode',
        'ActionGeo_FullName', 
        'ActionGeo_Lat', 
        'ActionGeo_Long']))
    
    # Deleting (future) unused variables to save memory footprint
    del gd2
    del extractedEng

    tqdm.write("Filtering data...")

    # Creating a dataframe from Indian only geolocated events and deduplicating based on source URL
    dedup_data = data.loc[data['ActionGeo_CountryCode'] == 'IN'].drop_duplicates(subset=['SOURCEURL']) 
    
    # Deleting data variable to save memory footprint
    del data

    # By default the index auto-detected is non-unique, and therefore won't be exported
    # by Pandas to JSON (or anything). This resets the index and is a default iterating
    # numeric index starting from 1 to N

    dedup_data.reset_index(drop=True, inplace=True)

    # Exporting using bz2 compression because it seems to compress text better
    # Also, Pandas can natively perform inferred on-the-fly decompression to read the .bz2 files
    tqdm.write("{0} {1} {2}".format("Exporting JSON for", day, "..."))
    dedup_data.to_json('/home/kaustav/Desktop/data/gdelt/data/IN_' + day + '.json.bz2')

    end = time.time()
    delta =  end - start
    
    tqdm.write("{0} {1} {2} {3} {4}".format("Exported", str(len(dedup_data)), "rows of data successfully in", str(delta), "seconds."))
    tqdm.write("Performing garbage collection...")
    tqdm.write("----------------------------------------------")
    del dedup_data
    del start, end, delta     
    
    # Invoking garbage collection to ensure no lagging unused data in memory

    gc.collect()

def main(start, end):
    
    "Enter start and end dates as strings. Preferably, YYYY-MM-DD."
    error = 0
    date_range = pd.date_range(start=start, end=end)

    for i in tqdm(date_range):
        downloadINGDELT(str(i.date()))

    print("{0} {1}".format(error, "errors during execution."))
    print("All done. Exiting...")

# Assembling the argh auto-translation into argparse
parser = argh.ArghParser()
parser.add_commands([main])

if __name__ == "__main__":
    parser.dispatch()
