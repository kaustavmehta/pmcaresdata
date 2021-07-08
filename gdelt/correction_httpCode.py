import pandas as pd
from multiprocessing.dummy import Pool as ThreadPool

def generateFilenames(startDate, endDate):
    # Returns an array of the tuple (hydratedFilename, htmlFilename)
    array = []
    dates = pd.date_range(startDate, endDate)
    
    for i in dates:
        hydratedFilename = '{0}{1}{2}'.format('IN_', str(i.date()), '-hyd.csv.xz')
        htmlFilename = '{0}{1}{2}'.format('IN_', str(i.date()), '-hyd_html.csv.xz')
        array.append((hydratedFilename, htmlFilename))
        
    return array

def addHttpCode(filenames):

    global hydratedPath
    global htmlPath
    global updatedPath
    
    for i in filenames:
        hydratedFile = hydratedPath + i[0]
        htmlFile = htmlPath + i[1]
        
        html = pd.read_csv(htmlFile)
        hydrated = pd.read_csv(hydratedFile)
        
        hydrated['httpCode'] = html['httpCode']
        
        del html
        
        hydrated.to_csv('{0}{1}{2}'.format(updatedPath, i[0][:17], '_http.csv.xz'))
        
        del hydrated
        
        print('Corrected {0}'.format(i[0]))
    
    print('Done.')

def verify(df):
    return 'httpCode' in df.columns

def MultiTaddHttpCode(filenames):
    global hydratedPath
    global htmlPath
    global updatedPath
    
    hydratedFile = hydratedPath + filenames[0]
    htmlFile = htmlPath + filenames[1]

    html = pd.read_csv(htmlFile)
    hydrated = pd.read_csv(hydratedFile)

    hydrated['httpCode'] = html['httpCode']

    del html

    hydrated.to_csv('{0}{1}{2}'.format(updatedPath, filenames[0][:17], '_http.csv.xz'))

    del hydrated

    print('Corrected {0}'.format(filenames[0]))
    
    print('Done.')

if __name__ == "__main__":
    
    hydratedPath = '/home/kaustav/Desktop/data/gdelt/hydrated/'
    htmlPath = '/home/kaustav/Desktop/data/gdelt/hydrated_html/'
    updatedPath = '/home/kaustav/Desktop/data/gdelt/hydrated_httpcode/'

    filenames = generateFilenames('2019-11-01', '2021-06-01')

    pool = ThreadPool(8)
    pool.map(MultiTaddHttpCode, filenames)
    pool.close()
    pool.join()


