import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz



def ab_client(df, df_ab_catalogue, thresh_digits, thresh_digits_val, thresh_country, thresh_country_val, transfer_run_only_flag):
   
#  df = df_before_ab_client.copy()
#  df_ab_catalogue = df_ab_cleint_dania.copy()  
#  thresh_digits, thresh_digits_val, thresh_country, thresh_country_val = 80, 75, 85, 80

  df['client_name_no_spaces'] = df['final_linked_name'].str.lower()
  df['ab_client_name'] = ''
  df['ab_client_flag'] = ''
  df['ab_client_flag_score'] = 0
  df['ab_client_customer_id'] = ''

  df_ab_catalogue['First_3digits'] = df_ab_catalogue['full_nam_eng'].str.replace(r"[^a-zA-Z0-9& ]+", " ").str.replace(" ","").astype(str).str[0:3]

  df['cntry'] = df['cntry'].astype(str).str.upper()
  
  countries = list(df['cntry'].unique())

  try:
    countries.remove('NAN')
  except ValueError:
    print('no nan countries found')
  
  def fuzz_check(new_merchants_str,merchants_list):
      fuzz_sort = {}    
      for j in range(len(merchants_list)):
        #average between token set and token sort
        fuzz_sort[merchants_list[j]] = (fuzz.token_sort_ratio(new_merchants_str, merchants_list[j].lower()) + fuzz.token_set_ratio(new_merchants_str, merchants_list[j].lower()))/2
      maximum = fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)]    
      return maximum, fuzz_sort
  
  for country in countries:
    new_merchants = list(set(df['client_name_no_spaces'][df['cntry'] == country]))
    if np.nan in new_merchants: new_merchants.remove(np.nan)    
    for i in range(len(new_merchants)):
      digits_flag = 1
      country_flag = 0
      merchants_list_digits = list(set(df_ab_catalogue['full_nam_eng'][(df_ab_catalogue['Country'] == country) & (df_ab_catalogue['First_3digits'] == new_merchants[i].lower().replace(' ', '')[0:3])]))
      if len(merchants_list_digits) == 0:
        digits_flag = 0
        country_flag = 1
        merchants_list_country = list(set(df_ab_catalogue['full_nam_eng'][df_ab_catalogue['Country'] == country]))        
        if len(merchants_list_country) == 0:
          df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag']] = 'not_found_country_catalogue'
          continue
                  
      if digits_flag == 1:
        ab_client_threshold = thresh_digits
        validation_threshold = thresh_digits_val
        maximum, fuzz_sort = fuzz_check(new_merchants[i],merchants_list_digits)
      
      if country_flag == 1:
        ab_client_threshold = thresh_country
        validation_threshold = thresh_country_val
        maximum, fuzz_sort = fuzz_check(new_merchants[i],merchants_list_country)
                
      if maximum > ab_client_threshold:
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_customer_id']] = df_ab_catalogue[(df_ab_catalogue['full_nam_eng'] ==  max(fuzz_sort, key=fuzz_sort.get)) & (df_ab_catalogue['Country'] == country)]['id_country'].to_list()
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_name']] = max(fuzz_sort, key=fuzz_sort.get)
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag_score']] = fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)]
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag']] = 'ab_client'
      elif maximum >= validation_threshold:
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_customer_id']] = df_ab_catalogue[(df_ab_catalogue['full_nam_eng'] ==  max(fuzz_sort, key=fuzz_sort.get)) & (df_ab_catalogue['Country'] == country)]['id_country'].to_list()
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_name']] = max(fuzz_sort, key=fuzz_sort.get)
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag_score']] = fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)]
        df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag']] = 'need_validation'
      else:
        if df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_swift_flag']].values[0] == 'AB Client SWIFT':
          df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag']] = 'ab_client_not_found'
        else:
          df.loc[(df['client_name_no_spaces'] == new_merchants[i]) & (df['cntry'] == country),['ab_client_flag']] = 'non_ab_client'
          
              
  return df


def ab_client_all_countries(df, df_ab_catalogue, ab_client_threshold, validation_threshold, transfer_run_only_flag):
      
  df = df[~(df['ab_client_name_if_stat'] == 'AB Client SWIFT') | ~(df['ab_client_flag'].isin(['ab_client','ab_client_not_found','need_validation']))].reset_index(drop = True)
 
  df['client_name_no_spaces'] = df['final_linked_name'].str.lower()
  
  df_ab_catalogue['First_3digits'] = df_ab_catalogue['full_nam_eng'].str.replace(r"[^a-zA-Z0-9& ]+", " ").str.replace(" ","").astype(str).str[0:3]

  new_merchants = list(set(df['client_name_no_spaces']))
  if np.nan in new_merchants: new_merchants.remove(np.nan)    
  dict_all_countries = {}
  
  for i in range(len(new_merchants)):
    merchants_list = list(set(df_ab_catalogue['full_nam_eng'][(df_ab_catalogue['First_3digits'] == new_merchants[i].lower().replace(' ', '')[0:3])]))
    if len(merchants_list) == 0:
      df.loc[(df['client_name_no_spaces'] == new_merchants[i]),['ab_client_flag_all_countries']] = 'non_ab_client'
      continue
    
    else:
      fuzz_sort = {}
      for j in range(len(merchants_list)):
        #average between token set and token sort
        fuzz_sort[merchants_list[j]] = (fuzz.token_sort_ratio(new_merchants[i], merchants_list[j].lower()) + fuzz.token_set_ratio(new_merchants[i], merchants_list[j].lower()))/2
      maximum = fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)]
      
      if maximum > ab_client_threshold:
        catalogue_basics_tuple = tuple(df_ab_catalogue[df_ab_catalogue['full_nam_eng'] ==  max(fuzz_sort, key=fuzz_sort.get)]['id_country'])
        if len(catalogue_basics_tuple) > 1:
          for j in range(len(catalogue_basics_tuple)):
            dict_all_countries[df.loc[i,'id'] + "_rep" + str(j)] = new_merchants[i],df.loc[i,'cntry'],catalogue_basics_tuple[j], max(fuzz_sort, key=fuzz_sort.get),fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)],'ab_client'
        else:
          dict_all_countries[df.loc[i,'id']] = new_merchants[i],df.loc[i,'cntry'],catalogue_basics_tuple, max(fuzz_sort, key=fuzz_sort.get),fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)],'ab_client'

      elif maximum >= validation_threshold:
        catalogue_basics_tuple = tuple(df_ab_catalogue[df_ab_catalogue['full_nam_eng'] ==  max(fuzz_sort, key=fuzz_sort.get)]['id_country'])
        if len(catalogue_basics_tuple) > 1:
          for j in range(len(catalogue_basics_tuple)):
            dict_all_countries[df.loc[i,'id'] + "_rep" + str(j)] = new_merchants[i],df.loc[i,'cntry'],catalogue_basics_tuple[j], max(fuzz_sort, key=fuzz_sort.get),fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)],'need_validation'
        else:
          dict_all_countries[df.loc[i,'id']] = new_merchants[i],df.loc[i,'cntry'],catalogue_basics_tuple, max(fuzz_sort, key=fuzz_sort.get),fuzz_sort[max(fuzz_sort, key=fuzz_sort.get)],'need_validation'

      else:        
        dict_all_countries[df.loc[i,'id']] = new_merchants[i],df.loc[i,'cntry'] ,(''),'',0,'non_ab_client'
        
  df_all_countries = pd.DataFrame(list(dict_all_countries.items()),columns=['referance','change'])
  columns = ['company','cntry','catalogue_basic','catalogue_name','catalogue_score','catalogue_flag']
  for i in range(len(df_all_countries)):
    if 'rep' in df_all_countries['referance'][i]:
      df_all_countries['referance'][i] = df_all_countries['referance'][i][:df_all_countries['referance'][i].find('_')]
    for col in range(6):        
      df_all_countries.loc[i,columns[col]] = df_all_countries['change'][i][col]

  df_all_countries.loc[:,'catalogue_country'] = df_all_countries.loc[:,'catalogue_basic'].str[-2:]      
  df_all_countries.drop('change', inplace=True,axis=1)
  
  
  
  return df_all_countries


