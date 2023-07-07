import pandas as pd
from fuzzywuzzy import fuzz
from datetime import datetime as dt
from collections import Counter
import numpy as np

def merchant_score(df, column_name, score_1, score_2):  
    
  df['First_3digits'] = df[column_name].str.replace(r"[^a-zA-Z0-9& ]+", " ").str.replace(" ","").astype(str).str[0:3].str.upper()
  df['First_3digits'] = df['First_3digits'].astype(str)

  df['cntry'] = df['cntry'].astype(str).str.upper()
  
  df_nan = df[df['cntry'] == 'NAN']
  print('shape is: ', df_nan.shape)
  
  countries = list(df['cntry'].unique())
  
  try:
    countries.remove('NAN')
  except ValueError:
    pass
  
  print('countries is: ', countries)

  df_temp = df[['id',column_name,'type','First_3digits','cntry']].copy()
  df_temp['final_mapped_name_no_space'] =  df_temp[column_name].str.upper()
  df_temp['shared_id'] = ''
  df_temp['temp_score'] = ''

  df_concat = pd.DataFrame() 
  id_count = 0

  for country in countries:
    merchants_temp = df_temp[df_temp['cntry'] == country]
    merchants_temp = merchants_temp[merchants_temp[column_name].notna()].reset_index(drop=True)
    for i in range(len(merchants_temp)):
      if merchants_temp.loc[i,'shared_id'] != '':
        continue
      temp_list = list(merchants_temp['final_mapped_name_no_space']) 
      temp_digits = merchants_temp['First_3digits'][i]
      merchants_list = [word for word in temp_list if word.replace(' ','').startswith(temp_digits)]
      if len(merchants_list) < 2: 
        merchants_temp.loc[i,['shared_id']] = 'id_single' 
        merchants_temp.loc[i,['temp_score']] = 100.0 
      else:  
        fuzz_sort = {}  
        fuzz_set = {}
        avg_dict = {}
        for j in range(len(merchants_list)):
            fuzz_sort[merchants_list[j]] = fuzz.token_sort_ratio(merchants_temp.loc[i,'final_mapped_name_no_space'], merchants_list[j])               
            fuzz_set[merchants_list[j]] = fuzz.token_set_ratio(merchants_temp.loc[i,'final_mapped_name_no_space'], merchants_list[j])               
            avg_dict[merchants_list[j]] = (fuzz_set[merchants_list[j]] + fuzz_sort[merchants_list[j]])/2

        avg_dict = {k: v for k, v in avg_dict.items() if v > score_1} 
        
        
        for temp_name in avg_dict.keys():
          if merchants_temp.loc[merchants_temp['final_mapped_name_no_space'] == temp_name,['shared_id']].values[0] != '':
            if avg_dict[temp_name] < merchants_temp.loc[merchants_temp['final_mapped_name_no_space'] == temp_name,['temp_score']].values[0]:              
              avg_dict[temp_name] = 0
        
        avg_dict =  {k: v for k, v in avg_dict.items() if v > 0}     
        if merchants_list.count(merchants_temp.loc[i,'final_mapped_name_no_space']) > 1:
          similar_merchants =  list(avg_dict.keys())
          scores_merchants = list(avg_dict.values())
          similar_merchants.extend([merchants_temp.loc[i,'final_mapped_name_no_space']]*(merchants_list.count(merchants_temp.loc[i,'final_mapped_name_no_space'])-1))
          scores_merchants.extend([100.0]*(merchants_list.count(merchants_temp.loc[i,'final_mapped_name_no_space'])-1))
        else:
          similar_merchants = list(avg_dict.keys())
          scores_merchants = list(avg_dict.values())
        if len(similar_merchants) == 1:
          merchants_temp.loc[i,['shared_id']] = 'id_single'
          merchants_temp.loc[i,['temp_score']] = 0.0
          continue
        merchants_temp.loc[merchants_temp['final_mapped_name_no_space'].isin(similar_merchants),['shared_id']] = 'id_' + str(id_count)
        for i in similar_merchants:
          merchants_temp.loc[merchants_temp['final_mapped_name_no_space'] == i,['temp_score']] = avg_dict[i]
        id_count = id_count + 1    
    df_concat = pd.concat([df_concat,merchants_temp],ignore_index=True)

  df = df.merge(df_concat[['id','shared_id']], on = 'id')  
  single_ids_list = [x for x in list(df['shared_id']) if list(df['shared_id']).count(x)==1]
  df.loc[df.shared_id.isin(single_ids_list),'shared_id'] = 'id_single'
  
  df['final_linked_name'] = ''

  df_similar = df[df['shared_id'] != 'id_single']

  id_list = list(set(df_similar['shared_id']))
  
  for i, id_ in enumerate(id_list):
    temp_list = list(df_similar.loc[df_similar['shared_id'] == id_,column_name])
    temp_list_ = list(df_similar.loc[df_similar['shared_id'] == id_,'volume_trxs'])
    duplicates_dict = {k: v for k, v in zip(temp_list, temp_list_)}
    if sum(1 for v in duplicates_dict.values() if v == max(list(duplicates_dict.values()))) > 1:
      temp_dict_ = {k for k, v in duplicates_dict.items() if v == max(list(duplicates_dict.values()))}
      df.loc[df['shared_id'] == id_,'final_linked_name'] = max(temp_dict_, key=lambda x: len(x))
    else: 
      df.loc[df['shared_id'] == id_,'final_linked_name'] = max(duplicates_dict, key=duplicates_dict.get)

  df.loc[df['shared_id'] == 'id_single','final_linked_name'] = df.loc[df['shared_id'] == 'id_single',column_name]


  df['First_3digits_linked'] = df['final_linked_name'].str.replace(r"[^a-zA-Z0-9& ]+", " ").str.replace(" ","").astype(str).str[0:3].str.upper()
  df['First_3digits_linked'] = df['First_3digits_linked'].astype(str)

  df_temp_ = df[['id','final_linked_name','type','First_3digits_linked','cntry']].copy()
  df_temp_['final_linked_name_no_space'] =  df_temp_['final_linked_name'].str.upper()
  df_temp_['shared_id_linked'] = ''
  df_temp_['temp_score'] = ''
  
  df_concat_ = pd.DataFrame() 
  id_count_ = 0
  
  for country in countries:
    merchants_temp_r = df_temp_[df_temp_['cntry'] == country]
    merchants_temp = merchants_temp_r[merchants_temp_r['final_linked_name'].notna()].reset_index(drop=True)
    
    for i in range(len(merchants_temp)):
      if merchants_temp.loc[i,'shared_id_linked'] != '':
        continue
      temp_list = list(merchants_temp['final_linked_name_no_space']) 
      temp_digits = merchants_temp['First_3digits_linked'][i]
      merchants_list = [word for word in temp_list if word.replace(' ','').startswith(temp_digits)]
      if len(merchants_list) < 2: 
        merchants_temp.loc[i,['shared_id_linked']] = 'correct'
        merchants_temp.loc[i,['temp_score']] = 100.0 
      else:
        fuzz_sort = {}  
        fuzz_set = {}
        avg_dict = {}
        
        for j in range(len(merchants_list)):
            fuzz_set[merchants_list[j]] = fuzz.token_set_ratio(merchants_temp.loc[i,'final_linked_name_no_space'], merchants_list[j])
            fuzz_sort[merchants_list[j]] = fuzz.token_sort_ratio(merchants_temp.loc[i,'final_linked_name_no_space'], merchants_list[j])               
            avg_dict[merchants_list[j]] = (fuzz_set[merchants_list[j]] + fuzz_sort[merchants_list[j]])/2
        
        avg_dict = {k: v for k, v in avg_dict.items() if v > score_2}

        if merchants_list.count(merchants_temp.loc[i,'final_linked_name_no_space']) > 1:
          similar_merchants =  list(avg_dict.keys())
          similar_merchants.extend([merchants_temp.loc[i,'final_linked_name_no_space']]*(merchants_list.count(merchants_temp.loc[i,'final_linked_name_no_space'])-1))
        else:
          similar_merchants = list(avg_dict.keys()) 
        if len(similar_merchants) == 1:
          merchants_temp.loc[i,['shared_id_linked']] = 'correct'
          continue
        merchants_temp.loc[merchants_temp['final_linked_name_no_space'].isin(similar_merchants),['shared_id_linked']] = 'id_' + str(id_count_)
        id_count_ = id_count_ + 1
    df_concat_ = pd.concat([df_concat_,merchants_temp],ignore_index=True)

    
  df = df.merge(df_concat_[['id','shared_id_linked']], on = 'id')  
  single_ids_list = [x for x in list(df['shared_id_linked']) if list(df['shared_id_linked']).count(x)==1]
  df.loc[df.shared_id_linked.isin(single_ids_list),'shared_id_linked'] = 'correct'  
  
  df['final_ending_name'] = ''

  df_similar = df[df['shared_id_linked'] != 'correct']

  id_list = list(set(df_similar['shared_id_linked']))

  for i, id_ in enumerate(id_list):
    temp_list = list(df_similar.loc[df_similar['shared_id_linked'] == id_,'final_linked_name'])
    temp_list_ = list(df_similar.loc[df_similar['shared_id_linked'] == id_,'volume_trxs'])
    duplicates_dict = {k: v for k, v in zip(temp_list, temp_list_)}
    if sum(1 for v in duplicates_dict.values() if v == max(list(duplicates_dict.values()))) > 1:
      temp_dict_ = {k for k, v in duplicates_dict.items() if v == max(list(duplicates_dict.values()))}
      df.loc[df['shared_id_linked'] == id_,'final_ending_name'] = max(temp_dict_, key=lambda x: len(x))
    else: 
      df.loc[df['shared_id_linked'] == id_,'final_ending_name'] = max(duplicates_dict, key=duplicates_dict.get)

  df.loc[df['shared_id_linked'] == 'correct','final_ending_name'] = df.loc[df['shared_id_linked'] == 'correct','final_linked_name']

  df.drop('final_linked_name', axis = 1,inplace=True)
  df = df.rename(columns = {'final_ending_name': 'final_linked_name'})

  
  df_final = pd.concat([df, df_nan], ignore_index=True)
  
  df_final['df_final'] = np.where(df_final['final_linked_name'].isna(), df_final['final_mapped_name'], df_final['final_linked_name'])
  
  return df_final
