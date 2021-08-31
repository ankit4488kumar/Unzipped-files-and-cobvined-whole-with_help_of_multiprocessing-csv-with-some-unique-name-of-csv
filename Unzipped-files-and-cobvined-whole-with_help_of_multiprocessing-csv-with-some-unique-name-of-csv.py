# -*- coding: utf-8 -*-
"""
Created on Sun Aug 29 18:12:47 2021

@author: ankit
"""

import os
import pandas as pd 
from multiprocessing import Pool
import glob
import zipfile
import os
my_path="C:/Users/ankit/Downloads/my_task/Que1"

number_of_file=0 
for file in glob.glob(my_path + "/*.zip"):
    print(file)
    number_of_file += 1
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(my_path)
        
print("successfully unzipped all the files")


# wrap your csv importer in a function that can be mapped
def read_csv(filelist):
    'converts a filename to a pandas dataframe'
    return pd.read_csv(my_path+'/'+filelist)


def main():

    # get a list of file names
    #files = os.listdir('.')
    #file_list = [filename for filename in files if filename.split('.')[1]=='csv']
    filelist = [file for file in os.listdir(my_path) if file.startswith('abc')]

    # set up your pool
    with Pool(processes=2) as pool: # or whatever your hardware can support

        # have your pool map the file names to dataframes
        df_list = pool.map(read_csv, filelist)

        # reduce the list of dataframes to a single dataframe
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df.to_csv(my_path+'/combined_csv_with_multiprocessing.csv', index=False, encoding='utf-8-sig')
        print("all required csv has been combined")

if __name__ == '__main__':
    main()
