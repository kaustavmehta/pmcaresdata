import pandas as pd
import newspaper
import requests
import time
import html5lib
import argh
import gc

from tqdm import tqdm
from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool as ThreadPool

def httpCode(url):
    "Returns the HTTP Status Code for an input URL."
    tqdm.write("Checking if page exists...")
    try:
        # Requesting for only the HTTP header without downloading the page
        # If the page doesn't exist (404), it's a waste of resources to try scraping.
        req = requests.head(url, timeout=30)
        return int(req.status_code)
    
    except:
        tqdm.write("Connection related Error/Timeout, skipping...")
        return int(404)


def getdata(url):
    "Helper function to download a URL's HTML to pass into beautifulsoup." 
    r = requests.get(url) 
    return r.text

def extract(url, code):
    "Extracts the title, author(s), keywords, summary, text, top image, images, videos, raw HTML and returned HTTP code for a given URL."
    
    tqdm.write("{0} {1}".format("Running newspaper3k pass...", url))
    
    try:
        # Newspaper3k extraction using Article object
        article = newspaper.Article(url)
        article.download()
        article.parse()
        article.nlp()
        
        title = article.title
        author = article.authors
        
        keywords = article.keywords
        summary = article.summary
        
        text = article.text
        topImage = article.top_image
        images = article.images
        videos = article.movies
        
        html = ""
        html = article.html

        # Sometimes newspaper3k cannot extract/doesn't return the raw HTML so beautifulsoup is used
        # as a failover mechanism to store the HTML. This will be useful for data validation and verification
        # because a substantial percentage of non-English languages aren't perfectly extracted by newspaper3k
        # and therefore manual site-wise extraction pipelines would have to be created to deal with them
        # in addition to using alternative NLP algorithms for multi-lingual stopwords and summarization.
        if html == "":
            
            htmldata = getdata(url) 
            # html5lib is the slowest parser, but simulates a browser and so should be more reliable
            soup = BeautifulSoup(htmldata, 'html5lib') 
            tqdm.write("Newspaper3k extraction completed, but used bs4 for html.")
            # Returns a 10 valued tuple
            return title, author, keywords, summary, text, topImage, images, videos, soup, code
        
        else:
            tqdm.write("Newspaper3k extraction completed.")
            # Returns a 10 valued tuple
            return title, author, keywords, summary, text, topImage, images, videos, html, code
    
    except:
        # In the event something goes wrong, the field is marked empty. This could either mean a failed server response
        # or that the scraping process is blocked by the server. Alternatively it could be any issue with n3k, parsing or downloading.
        # @TODO create failover networking mechanisms to accomodate timeout/connection errors/etc and to wait
        # until they are resolved so that there is confidence in data validity (retriving a HTML at minimum).
        return "", "", "", "", "", "", "", "", "", ""

def verify(url):
    "Checks if the site exists before proceeding to perform scraping."
    code = httpCode(url)
    
    if code == 404:
        tqdm.write("Got 404, skipping...")
        # Returns 10 valued tuple, and so at minimum, for each site we have a HTTP code.
        return "", "", "", "", "", "", "", "", "", code
    else:
        tqdm.write("Non-404 code returned: performing extraction...")
        return extract(url, code)

def execute(df):
    
    tqdm.write("Creating Pool...")
    # Creating a 24 thread pool for the execution in one process. 2-3 threads per CPU core should be fine.
    # In any case, the OS takes care of managing thread execution, so it's difficult to screw something up here
    # (I've tried 2000 threads for lolz - it's fast, but you will wait for a long time for all the threads to pool their data). 
    # This also takes care of memory usage. I've ran 5 processes of this code just fine without any issues.
    pool = ThreadPool(24)
    
    tqdm.write("Initiating scraping...")
    # Deploying function on the thread pool
    processed = pool.map(verify, df['SOURCEURL'])
    
    # Cleaning up and closing the pool.
    pool.close()
    pool.join()
    tqdm.write("Closing Pool...")

    tqdm.write("Converting to pandas dataframe...")
    # ThreadPoolExecutor returns an array (here, an array of tuples) and so conversion is necessary
    processed_df = pd.DataFrame(processed)
    
    # Adding human-readable column names for each respective tuple value
    processed_df.columns = ['Title', 
                            'Author', 
                            'Keywords', 
                            'n3kSummary', 
                            'ExtractedText', 
                            'TopImage', 
                            'Images', 
                            'Videos', 
                            'HTML', 
                            'httpCode']
    
    # Deleting original array to save memory
    del processed
    # Running garbage collection
    gc.collect()
    # Returns a pandas dataframe with all the extracted data
    return df.join(processed_df)

def generateFiles(start, end):
    "Lazy method to generate filenames that correspond with the output of the GDELT script."
    # @TODO: must be updated for autodetection of files in a pointed folder. So far this only
    # applies to this naming scheme which implies that it only applies to Indian data.
    dateRange = pd.date_range(start=start, end=end)
    filenames = []
    for i in dateRange:
        filename = "IN" + "_" + str(i.date()) + ".json.bz2"
        filenames.append(filename)
    
    # Returns an array of filenames
    return filenames

def main(start, end, pathToData='/home/kaustav/Desktop/data/gdelt/data/', pathToHTML='/home/kaustav/Desktop/data/gdelt/hydrated/html/', pathToExport='/home/kaustav/Desktop/data/gdelt/hydrated/'):
    "Generates filenames between the given start and end dates and extracts data for each URL for each day, generating two day-wise datasets for HTML and non-HTML data."
    files = generateFiles(start, end)
    
    for jsonFile in tqdm(files):
        filepath = pathToData + jsonFile

        # Read compressed JSON that was created by the GDELT script
        df = pd.read_json(filepath)
        tqdm.write("{0} {1} {2}".format("Reading", jsonFile, "..."))
        tqdm.write("{0} {1}".format("Number of URLs: ", len(df['SOURCEURL'])))
        
        start = time.time()
        
        tqdm.write("Downloading and hydrating dataset...")
        # Begins extraction process and the resultant dataframe is returned to variable 'data'
        data = execute(df)
        tqdm.write("{0} {1} {2}".format("Completed in", time.time() - start, "seconds."))

        # Creating an empty dataframe
        html = pd.DataFrame()
        # Copying over HTML and httpCode columns from data before deleting them from data
        html['HTML'] = data['HTML']
        html['httpCode'] = data['httpCode']

        # Deleting HTML and httpCode columns from data so it can be exported as a separate datafile
        data = data.drop(['HTML', 'httpCode'], axis=1)
        
        # Naming schemes for the hydrated datafiles and the HTMl datafiles
        hydratedFilename = jsonFile[:13] + "-hyd.csv.xz"
        htmlFilename = jsonFile[:13] + "-hyd_html.csv.xz"

        hydFilePath = pathToExport + hydratedFilename
        htmlFilePath = pathToHTML + htmlFilename
        
        tqdm.write("Writing HTML data...")
        # Writing out HTML file as CSV: CSV was chosen because it's more storage efficient for large data
        html.to_csv(htmlFilePath)
        # Deleting the HTML dataframe to save memory
        del html

        tqdm.write("Writing remaining data...")
        # Writing out the hydrated file as CSV: CSV was chosen because it's more storage efficient for large data
        data.to_csv(hydFilePath)
        # Deleting the hydrated dataframe to save memory
        del data
        # Collecting garbage
        gc.collect()
        duration = time.time() - start
        tqdm.write("{0} {1} {2}".format("Rehydration took", duration, "seconds."))

# Assembling the argh auto-translation into argparse
parser = argh.ArghParser()
parser.add_commands([main])

if __name__ == "__main__":
    # Executes the program from terminal
    parser.dispatch()
