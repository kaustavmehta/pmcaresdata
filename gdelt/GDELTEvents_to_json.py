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

# Format of day = '2020 12 17' (YYYY MM DD)

def filterIN(dataset):
    
    "Function returns a Pandas dataframe with relevant, usable Indian GDELT data filtered based on ActionGeo_CountryCode"

    # Reading the JSON from memory
    tqdm.write("Reading JSON download from memory...")
    df = pd.read_json(dataset)
    
    extractedDf = df.get(['GLOBALEVENTID',
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

    tqdm.write("Dropped irrelevant columns.")

    tqdm.write("Filtering data...")

    # Creating a dataframe from Indian only geolocated events
    df_IN = extractedDf.loc[extractedDf['ActionGeo_CountryCode'] == 'IN'] 
    
    # Deduplication based on source URL
    dedup_IN = df_IN.drop_duplicates(subset=['SOURCEURL'])
    
    tqdm.write("Saving memory (1)...")
    
    # Deleting variables to save memory footprint
    del df_IN
    del df
    
    # Invoking garbage collection to ensure no lagging unused data in memory
    gc.collect()

    # Returns a Pandas dataframe
    return dedup_IN

def downloadINGDELT(day):
    "Downloads and returns filtered Indian English and Translingual GDELT Events data appended into one Pandas dataframe"

    # Instantiating to query GDELT V2
    gd2 = gdelt.gdelt(version=2)

    # Downloading and returning filtered English GDELT Events Data 
    tqdm.write("{0} {1} {2}".format("Downloading English GDELT Events data for", day, "..."))
    eng = filterIN(dataset = gd2.Search([day], table='events', coverage=True, output='json'))

    # Downloading and returning filtered Translingual GDELT Events Data
    tqdm.write("{0} {1} {2}".format("Downloading Translingual GDELT Events data for", day, "..."))
    trans = filterIN(dataset = gd2.Search([day], table='events', translation=True, coverage=True, output='json'))

    # Merging English and Translingual dataframes
    tqdm.write("Merging datasets...")
    data = eng.append(trans)

    # By default the index auto-detected is non-unique, and therefore won't be exported
    # by Pandas to JSON (or anything). This resets the index and is a default iterating
    # numeric index starting from 1 to N

    data.reset_index(drop=True, inplace=True)

    # Deleting variables to save memory footprint
    tqdm.write("Saving memory (2)...")
    del eng
    del trans
    del gd2
    
    # Invoking garbage collection
    gc.collect()

    # Returns a Pandas dataframe
    return data

def export(day):
    "Uses the helper functions downloadINGDELT and filterIN to export a JSON with filtered, deduplicated and relevant Indian GDELT Events data."

    start = time.time()

    data = downloadINGDELT(day)

    # Exporting using bz2 compression because it seems to compress text better
    # Also, Pandas can natively perform inferred on-the-fly decompression to read the .bz2 files
    tqdm.write("{0} {1} {2}".format("Exporting JSON for", day, "..."))
    data.to_json('/home/kaustav/Desktop/data/gdelt/data/IN_' + day + '.json.bz2')

    end = time.time()
    delta =  end - start
    
    tqdm.write("{0} {1} {2} {3} {4}".format("Exported", str(len(data)), "rows of data successfully in", str(delta), "seconds."))
    tqdm.write("Saving memory (3)...")

    del data
    gc.collect()

def main(start, end):
    
    "Enter start and end dates as strings. Preferably, YYYY-MM-DD."

    date_range = pd.date_range(start=start, end=end)

    for i in tqdm(date_range):
        for attempt in range(1,6):
            try:
                time.sleep(2)
                gc.collect()
                export(str(i.date()))
            except:
                tqdm.write("{0} {1}".format("Attempt: #", str(attempt)))
                continue
            break

    print("All done. Exiting...")

# Assembling the argh auto-translation into argparse
parser = argh.ArghParser()
parser.add_commands([main])

if __name__ == "__main__":
    parser.dispatch()
