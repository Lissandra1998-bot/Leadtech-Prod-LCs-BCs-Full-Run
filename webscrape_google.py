
"""
Google Run

  Take the first percentage of full name based on the length of the full name. 
  Then run this new cut name into google anf save all results in csv.
"""

import pandas as pd
import numpy as np

from datetime import datetime as dt

import os
import csv
import re
import logging
import optparse

import time
import requests
#from search_engines import Google

import tqdm
import logging


def run_google_ws(df_google):
    logging.basicConfig(filename='temp/logging_google.log', level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
    logger=logging.getLogger(__name__)
    
    df_google['togoogle'] = df_google['togoogle'].fillna('')

    df_google['length'] = df_google['togoogle'].str.len()
    df_google['togoogle_40cut'] = df_google['togoogle'].apply(lambda x: x[:int(len(x)*0.4)])
    df_google['togoogle_75cut'] = df_google['togoogle'].apply(lambda x: x[:int(len(x)*0.75)])

    df_google['google_dynamic_cut'] = np.where(df_google['length']<30, df_google['togoogle'],       
                                      np.where((df_google['length']>30) & (df_google['length']<=40), df_google['togoogle_75cut'],
                                      np.where((df_google['length']>40), df_google['togoogle_40cut'], 
                                              'Error cutting result')))

    df_google['togoogle_actual'] = np.where(df_google['result_1_pos_cut'] == -1, df_google['google_dynamic_cut'],
                                           np.where(df_google['result_1_pos_cut'] != -1, df_google['result_1_dyn_split'], 
                                            'Error_with_name'))    

    df_google.isna().sum()
    df_google[df_google['togoogle'] == 'gigantex corp japan']

    df_result = pd.DataFrame(columns = ['id','raw_message','to_google_final','titles','text','links', 'result_no'])

    date_ = dt.now().strftime('%d%m_%H%M')

    df_result.to_csv(f'Results/Google/Google_results_{date_}.csv', index=False)

    count=0
    
    print(df_google.shape)
    
    for idx,row in df_google.iterrows():
#        try:
        x = row['togoogle_actual']
        df_temp = pd.DataFrame()
        engine = Google()
        results = engine.search(x, pages=1)  
        time.sleep(1)
        count+=1
        links = results.links()
        titles = results.titles()
        text = results.text()

        df_temp['titles'] = titles
        df_temp['text'] = text
        df_temp['links'] = links
        df_temp['to_google_final'] = x
        df_temp['raw_message'] = row['raw_message']
        df_temp['result_no'] = list(range(1,len(titles)+1))
        df_temp['id'] = row['id']

        df_temp  = df_temp[df_result.columns]
        df_result = df_result.append(df_temp)
        print()
        print(row['raw_message'])
        print(x)  
        print(titles)  
        df_temp.to_csv(f'Results/Google/Google_results_nov_dec_{date_}.csv', mode = 'a',  index=False, header=False)
#        except Exception as e:
#          logger.error(e)
#          raise e

    return df_result




