import time
start = time.time()

from datetime import datetime as dt

from IPython.display import display
import pandas as pd

pd.options.display.max_columns = None
pd.set_option('max_colwidth', -1)

import pandas as pd
import difflib
from random import randint
from nlp_dataprocessing import *
from tqdm import tqdm
#from pyhive import hive
from itertools import chain
import datetime

def run_nlp_g(df, preprocess = False):

  # #### UNIQUE MESSAGE CATALOGUE - TO MATCH LATER FOR DEDUPING
#  df = df_raw.copy()
  
  df['mergeon'] = df['raw_message'].str.strip()+df['cntry'].str.strip()
  unique_message = df[['raw_message','cntry']].drop_duplicates().reset_index(drop=True)
  unique_message['mergeon'] = unique_message['raw_message'].str.strip()+unique_message['cntry'].str.strip()
  unique_message = unique_message.reset_index()
#  unique_message =unique_message.drop_duplicates('mergeon')

  # create unique id for rawmessages
  unique_message['unique_id'] = unique_message['index']+1
  unique_message = unique_message[['unique_id', 'raw_message','cntry','mergeon']]

  # merge unique ids to df 0114IML201600609 
  df = df.merge(unique_message[['unique_id','mergeon']], on='mergeon', how='left')

  ### 5. EXTRACT COMPANY NAMES

  TO_EXTRACT = process(unique_message[['unique_id','raw_message']])

  if preprocess:
    return TO_EXTRACT
  
  result = predict(TO_EXTRACT)


  df_final_result_3 = result.merge(TO_EXTRACT[['index', 'raw']], how='left', left_on = 'indexno', right_on='index')

  df_final_result_3 = df_final_result_3.drop_duplicates(subset=['raw', 'org_detected', 'loc_detected','org_cleaned'])
  df = df.merge(df_final_result_3[['raw', 'org_cleaned', 'loc_detected']], how='left', left_on = 'raw_message', right_on='raw')
  df.rename(columns={'org_cleaned':'result_3'}, inplace=True)
  
  df.drop(['raw'],axis = 1,inplace = True)
  
  datetime = dt.now().strftime('%d%m_%H%M')

  df.to_csv(f'Results/Result_3_nlp_{datetime}.csv', index=False)
  
  return df

