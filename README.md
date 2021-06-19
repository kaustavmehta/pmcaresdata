# Data and Computation
The Data and Computation Team's GitHub Repo to store the code and processed datasets for GDELT and Twitter data.

## Instructions to get GDELT running

### If you want to download your own date range (~2-3 days per program run)

- Create and activate a new Conda environment:
  - ```conda create -n env_name```
  - ```conda activate env_name```

- Install prerequiste libraries and modules:
  - ```pip install pandas, numpy, argh, tqdm```

- Clone/download the following gdeltPyR repo:
  - ```https://github.com/linwoodc3/gdeltPyR/tree/proxy_fix``` 

- ```cd``` into the downloaded folder using either Terminal/Command Prompt/Powershell
  - run ```python setup.py develop```

  - #### Important: Don't delete or move the downloaded gdeltPyR folder. The above command creates a symbolic link to 'install' it in your Python installation within your activated Conda environment.

- Verify that gdeltPyR works by launching the Python interpreter and running ```import gdelt```

- Download the ```GDELTEvents_tojson_optimized.py``` from this repo and open it in an editor and scroll down to ```line 125```:

 ```python    
  dedup_data.to_json('/home/kaustav/Desktop/data/gdelt/data/IN_' + day + '.json.bz2')
 ```

- Change the path from ```/home/kaustav/Desktop/...``` to your own folder path. Leave ```IN_' + day + '.json.bz2'``` unchanged unless you want uncompressed outputs. 
- You can also change the ```pandas``` function from ```_data.to_json``` to ```_data.to_csv``` if you want .csv outputs. Bear in mind, you will have to change ```.json.bz2``` to ```.csv.bz2``` (or leave out the ```.bz2``` if you want uncompressed).

- Run it using ```python GDELTEvents_tojson_optimized.py "start date" "end date"```
- It is important that the dates are separated by a space (the program takes two positional arguments this way) and that the dates are in the format ```YYYY-MM-DD``` and are encapsulated within quotes.

### If you want to download a date range that is much longer (~weeks/months/year):

- Navigate to the downloaded ```gdeltPyR``` folder and enter the ```gdelt``` subdirectory.
- Open up ```base.py``` in an editor and go to ```line 625```:

  - ```python
            else:

            if self.table == 'events':

                pool = Pool(processes=cpu_count())
                downloaded_dfs = list(pool.imap_unordered(eventWork,
                                                          self.download_list))
            else:

                pool = NoDaemonProcessPool(processes=cpu_count())
                downloaded_dfs = list(pool.imap_unordered(_mp_worker,
                                                          self.download_list,
                                                          ))
            pool.close()
            pool.terminate()
            pool.join()
     ```
- If your computer has ~4-8 GB of RAM, add ```maxtasksperchild=25``` after both mentions of ```processes=cpu_count()``` like the following example:
  - ```python
            else:

              if self.table == 'events':

                  pool = Pool(processes=cpu_count(), maxtasksperchild=25)
                  downloaded_dfs = list(pool.imap_unordered(eventWork,
                                                            self.download_list))
              else:

                  pool = NoDaemonProcessPool(processes=cpu_count(), maxtasksperchild=25)
                  downloaded_dfs = list(pool.imap_unordered(_mp_worker,
                                                            self.download_list,
                                                            ))
              pool.close()
              pool.terminate()
              pool.join()
    ``` 
- If you have >8 GB of RAM and aren't going to use your computer, you can increase ```maxtasksperchild``` between 50 and 250. Lower values of ```maxtasksperchild``` will decrease the speed of parallel downloads of GDELT Events data (1 day of Events data = 4 files per hour * 24 hours = 96 files, each compressed file ~0.75 MB - 25 MB).

- This program is very memory intensive, so it is recommended to run some process monitor like Task Manager (Windows), Htop (Linux) or Activity Monitor (Mac). 
- The program has error contingency mechanisms for handling download errors (timeouts, no reply, unresolved domain, etc) and won't store a processed data unless its complete. Therefore, you can stop the program at any time and resume the program by adjusting the supplied ```start``` and ```end``` dates at execution.
