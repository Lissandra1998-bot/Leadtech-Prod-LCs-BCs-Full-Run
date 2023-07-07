## Check for suffixes other than co

import pandas as pd
import numpy as np

import os
import csv
import re
import logging
import optparse

from datetime import datetime as dt

import time
import requests

from fuzzywuzzy import fuzz

from webscrape_google import *

global world
world_df = pd.read_csv('Data/world_cities_top.csv')
world_df.fillna('', inplace=True)
world = [x.lower() for x in list(set(list(world_df['city_ascii'])+list(world_df['country'])) - set(''))]

#store the last suffix in global variable to return it with it
global company_suff_pos
company_suff_pos = 0

def cut_suffix(name, list_search):
    global company_suff_pos
    strip_name = name.replace(' ', '') 

    
    for search_str in list_search:
          result_cut = -1
          actual_stopword = ''
    
          if (' ' in search_str) and (search_str in name):
              # result_cut = name.find(search_str)
               result_cut = name.find(search_str) + len(search_str)
    
    
          elif search_str in strip_name:
              
              # print(search_str)
            
              actual_stopword = ''
              checker_name = ''
              for idx,char in enumerate(name):
                if char == ' ':
                    actual_stopword = actual_stopword + ' '
                    continue
    
                if len(checker_name) == 0:
                    if char == search_str[0]:
                      checker_name = checker_name + char
                      result_cut = idx
                      actual_stopword = ''
                else:
                    check_temp  = checker_name + char      
    
                    if search_str[:len(check_temp)] == check_temp:
                        checker_name = check_temp
                        actual_stopword = actual_stopword + char
    
                    else:
                        checker_name = ''
                        actual_stopword = ''
                        result_cut = -1
    
                if checker_name == search_str and result_cut != 0:
                    if (search_str != 'http') and (search_str != 'pobox'):
                        result_cut = idx+1

                    
                    if result_cut < 3:
                          actual_stopword = ''
                          result_cut = -1
                          checker_name = ''
                          continue
                    break
          
          if result_cut != -1:
            break
        
    return result_cut


def get_before_str(name, list_search=[]):
      
  if len(list_search) == 0:               
    list_comp_names = [
        ' co ',
        ' as ',
        ' sa ',
        ' c o ',
        ' s a ',
        ' a s ',
        "Incorporated",
        "Limited",
        "LTD",
        "PSC",
        "PLC",
        'LLC',
        "PLLC",
        'FCZO',
        'FZE',
        'wll',
        'company',
        'sarl',
        'gmbh',
        'industirel',  
        'dmcc',
        'gmbh',
        ]

    list_comp_names = [x.lower() for x in list_comp_names]
    list_search = list_comp_names.copy()
    
  list_spaces = [x for x in list_search if ' ' in x]
    
  if type(name) is int:
    print(name)
      
  strip_name = name.replace(' ', '') 
    
  #count the number of suffixes in strip name to loop over
  no_occur = sum([x in strip_name for x in list_search]) + sum([x in name for x in list_spaces])

  result_str = name
  final_cut_result = -1
  for i in range(no_occur):
          
      final_cut_result = cut_suffix(result_str, list_search)
      result_str = result_str[:final_cut_result]
      result_str_strp = result_str.replace(' ', '')
      no_occur_temp = sum([x in result_str_strp for x in list_search]) + sum([x in result_str for x in list_spaces])
      if no_occur_temp <= 1:
          break
       
  return final_cut_result

def extract_pos_name(name, pos, type_=''):
  
  if pos != -1:
    if type_ == 'bank':
      return name[pos:]
    else: 
      return name[:pos] 
  else:
    return name


def run_scoring(str_orig, i):

  #keep only letters for comparison
  i = re.sub("[^A-Za-z ]", "", i)
  
  temp_score_set = fuzz.token_set_ratio(str_orig.lower(), i.lower())
  temp_score_sort = fuzz.partial_token_sort_ratio(str_orig.lower(), i.lower())
  temp_score = (temp_score_set + temp_score_sort)/2

  temp_score = temp_score/100

  return temp_score  
  
def run_nlp(name):
 
  global nlp
  doc=nlp(name)
  
  org_ = []
  gpe_ = []
  
  for ent in doc.ents:
    if ent.label_ == 'GPE':
      gpe_.append(ent.text)
    elif ent.label_ == 'ORG':
      org_.append(ent.text)
      
  return (org_, gpe_)

def withoutcc(name):
  global world
  
  name_split = name.split()
  
  cleaned = " ".join([x for x in name_split if x.lower() not in world])
  
  return cleaned


def clean_result(str_go_orig_dyn, str_go_list):
  ''''''
  #making sure 
  str_go_orig_dyn = str(str_go_orig_dyn)
  str_go_list = [str(x) for x in str_go_list]

  result_big = []
  scores_big = []
  
  #looping over the 3 google results for each search word
  for str_go in str_go_list:   
  
      #remove link from result
      str_go_pos = get_before_str(str_go, list_search = ['http'])
      str_go = extract_pos_name(str_go, str_go_pos)
      
      
      #remove countries and cities from result
      str_go = withoutcc(str_go)

      #find charachters to split for this result
      split_results = re.sub(r"[^*+-/:–;<=>_|]+", "", str_go)
      split_results = split_results.replace(".", '')
      split_results = ''.join(set(split_results))
      split_results = split_results.replace(" ", "")
      

      result = []
      scores = []
      list_has_geo = []

      #nothing to split by in result
      if  len(split_results) == 0:
          i = re.sub(r"[^A-Za-z&-. ]", "",str_go)
          i= i.strip()

          result_big.append(i)
          scores_big.append(run_scoring(str_go_orig_dyn.replace(" ",""), i.replace(" ","")))
      
      #splitting by whatever is in split_results list and scoring to get the best part
      else:
          #looping over the charachters to split by
          for char in split_results:
              if char == '-' or char == '–':
                char = char + ' '
              split_str = str_go.split(char)
              
              #looping over the splits after splitting and scoring each one againt the search string
              for i in split_str:
                  i = re.sub(r"[^A-Za-z&-. ]", "", i)
                  i = i.strip()
                  
                  #running scoring function
                  temp_score = run_scoring(str_go_orig_dyn.replace(" ",""), i.replace(" ",""))
                  
                  #removing the result if it is less than 3 charachters
                  if len(i) <=3:
                    temp_score = 0
                  
                  scores.append(temp_score)
                  result.append(i)
          
          #append best part of this particular result_no (eg. best part of google_result_number_1 is appended to big list before moving on to google_result_number_2 to get the best split reuslt from it)
          result_big.append(result[np.argmax(scores)])
          scores_big.append(np.max(scores))

  return [result_big, scores_big]

def get_max_score(list_names, list_scores):
  '''Getting the best score string based on a list of scores'''
  return [list_names[np.argmax(list_scores)], np.max(list_scores)]
    
#-----------------------------------------------------------------------------------------------------
def run_result_1_2(df_raw, ws_google=False):
  
  
  df_raw['cln_name_addre'] = df_raw['to_match'].str.lower().str.replace("[^A-Za-z0-9& ]", " ", regex=True)
  df_raw['cln_name_addre'] = df_raw['cln_name_addre'].str.replace("\d{3,}", "", regex=True)
  df_raw['cln_name_addre'] = df_raw['cln_name_addre'].str.replace(" +", " ", regex=True)
  df_raw['cln_name_addre'] = df_raw['cln_name_addre'].str.strip()
  

  df_raw['togoogle_pos'] = df_raw[['cln_name_addre']].apply(lambda x: get_before_str(*x, ['bank']), axis=1)
  df_raw['togoogle'] = df_raw[['cln_name_addre','togoogle_pos']].apply(lambda x: extract_pos_name(*x, 'bank'), axis=1)
  

  
  #---------------------------------------------------------------------------------------------
  #Results 1: dynamic splitting
  df_res_1 = df_raw.copy()

  df_res_1['result_1_pos_cut'] = df_res_1[['cln_name_addre']].apply(lambda x: get_before_str(*x), axis=1)

  df_pobox_cut = df_res_1[df_res_1['result_1_pos_cut'] != -1]
  df_other_cut = df_res_1[df_res_1['result_1_pos_cut'] == -1]

  df_other_cut['result_1_pos_cut'] =  df_other_cut[['cln_name_addre']].apply(lambda x: get_before_str(*x, list_search=['pobox']), axis=1)

  df_res_1 = df_pobox_cut.append(df_other_cut)

  df_res_1['result_1_dyn_split'] = df_res_1[['cln_name_addre', 'result_1_pos_cut']].apply(lambda x: extract_pos_name(*x), axis=1)

  datetime = dt.now().strftime('%d%m_%H%M')
  
  df_res_1.to_csv(f'Results/Result_1_splitting_{datetime}.csv', index=False)

  #------------------------------------------------------------------------------------------------
  #Result 2: google processing
  if ws_google:
    df_google = run_google_ws(df_res_1)
  else:
    base_path = 'Results/Google'
    list_files = os.listdir(base_path)
    paths = [os.path.join(base_path, file) for file in list_files]
    latest_file = max(paths, key=os.path.getctime)
    
    df_google = pd.read_csv(latest_file)
    df_google = df_google.rename(columns= {'name_add':'to_google_final'})
  
  df_google = df_google.drop_duplicates(subset = ['id', 'to_google_final', 'result_no'])

  #keep only top 3 results
  df_google = df_google[df_google['result_no'].isin([1, 2, 3])]

  df_google['titles'] = df_google['titles'].astype(str)

  #remove unwanted charachters
  df_google['go_res_cln'] = df_google['titles'].str.lower().str.replace(r"[^A-Za-z&.,/-:-*+;<=>_| ]", " ", regex=True)

  #removing duplicated instances of 
  df_google['go_res_cln'] = df_google['go_res_cln'].str.replace("([^A-Za-z])\\1+", "\\1", regex=True)
  #df_google[df_google['go_res_cln'].str.contains('([^A-Za-z])\\1+', regex=True)]

  df_google[['id', 'result_no']].duplicated().sum()
  try:
    df_google.drop('Unnamed: 0', axis=1, inplace=True)
  except:
    pass
  
  #create 3 columns for each result rather than 3 rows
  df_google['result_no'] = 'result_no_' + df_google['result_no'].astype(str)
  df_google = df_google.pivot(index=['id', 'to_google_final'], columns='result_no', values='go_res_cln')
  df_google = df_google.reset_index()

  raw_columns = [x for x in df_google.columns if 'result' in x]
  df_google['3_results_list'] = df_google[raw_columns].apply(list, axis = 1)

  df_google['title_split_cln'] =   df_google[['to_google_final', '3_results_list']].apply(lambda x: clean_result(*x), axis=1)                                                                          

  df_google[['title_split_cln', 'max_score']] = df_google['title_split_cln'].apply(pd.Series)

  df_google['result_2_google'] = df_google[['title_split_cln', 'max_score']].apply(lambda x: get_max_score(*x), axis=1)
  df_google[['result_2_google', 'final_max_score']] = df_google['result_2_google'].apply(pd.Series)
  
  datetime = dt.now().strftime('%d%m_%H%M')

  df_google.to_csv(f'Results/Result_2_google_{datetime}.csv', index=False)

  #---------------------------------------------------------------------------------
  df_final = df_res_1.merge(df_google [['id' , 'result_2_google']], how='left',on='id')
  
  return df_final