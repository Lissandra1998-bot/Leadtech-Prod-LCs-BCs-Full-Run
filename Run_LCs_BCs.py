import os, sys
os.chdir('Full Run/')

from config_lc_bc import *
from nlp_dataprocessing import *
from nlp_run import *
from merchant_scoring import *
from ab_client import *

import pandas as pd
import numpy as np
from datetime import datetime as dt

#trun off console warnings
import warnings

warnings.filterwarnings('ignore')

datetime = dt.now().strftime('%d%m_%H%M')

needed_cols_for_dash = ['id', 'validation_needed', 'final_linked_name', 'ab_client_name_if_stat', 'raw_message', 'ab_client_name',
                        'ab_client_flag', 'ab_client_customer_id','ab_client_flag_score','applicant_beneficiary','cntry','Bank Code', 
                        'Bank Name',"drawer_country_code","collection_type",'type','load_country', 'issue_date','ab_flag_final']

#*********************************************************************

#reading agregated raw dataset based on run type (manual or through db incremental run)

df_raw = get_data_bc_lc()

df_raw = df_raw.replace(np.nan, '', regex=True)

df_empty_raw_messages = df_raw[~df_raw['raw_message'].str.contains("[A-Za-z]", regex=True,na=False)]
df_empty_raw_messages['raw_message_flag'] = 'no company name'  
df_concat_cols = [x for x in df_empty_raw_messages.columns if x in needed_cols_for_dash]
df_empty_raw_messages = df_empty_raw_messages[df_concat_cols]
df_empty_raw_messages.reset_index(inplace=True, drop=True)

df_raw = df_raw[df_raw['raw_message'].str.contains("[A-Za-z]", regex=True,na=False)]
df_raw['raw_message_flag'] = 'company name to be processed'  
df_raw.reset_index(inplace=True, drop=True)

df_raw_without_cntry = df_raw[~df_raw['cntry'].str.contains("[A-Za-z]", regex=True,na=False)]

df_raw_with_cntry = df_raw[df_raw['cntry'].str.contains("[A-Za-z]", regex=True,na=False)]

df_raw_without_cntry = fill_country(df_raw_without_cntry)

df_raw_without_cntry['cntry'] = np.where(~df_raw_without_cntry['country_fuzz'].isna(), df_raw_without_cntry['country_fuzz_code'], df_raw_without_cntry['drawer_country_code'])

df_raw_without_cntry = df_raw_without_cntry[df_raw_without_cntry.columns]

df_raw = pd.concat([df_raw_with_cntry, df_raw_without_cntry])


#call code to run result 3
df_nlp_result = run_nlp_g(df_raw)
df_nlp_result = df_nlp_result.drop_duplicates(['id', 'raw_message', 'mergeon'])
df_nlp_result.reset_index(drop=True, inplace = True)
df_nlp_result.rename(columns={'result_3': 'final_mapped_name'}, inplace=True)
df_nlp_result = clean_final_name(df_nlp_result)

#---------------------------------------------

# group duplicates by column 'B' and count them
duplicates_count = df_nlp_result.groupby('final_mapped_name').size().reset_index(name='volume_trxs')

# merge the count dataframe with the original dataframe on column 'B'
df_merged = pd.merge(df_nlp_result, duplicates_count, on='final_mapped_name', how='left')
df_merged['id_to_merge_on'] = df_merged['final_mapped_name'] + "_" +df_merged['cntry']

# remove duplicates based on column 'B'
df_res_scrd_no_duplic = df_merged.drop_duplicates(subset=['id_to_merge_on'])


#run merchant scoring in order to identify similar company names out of the full 
column_name = 'final_mapped_name'
score_1 = 80
score_2 = 85

df_scoring_result = merchant_score(df_res_scrd_no_duplic, column_name, score_1, score_2)

#calculate similarity after run is done
df_scoring_result["token_set_ratio"] = df_scoring_result.apply(lambda row: fuzz.token_set_ratio(row["final_mapped_name"], row["final_linked_name"]), axis=1)
df_scoring_result["token_sort_ratio"] = df_scoring_result.apply(lambda row: fuzz.token_sort_ratio(row["final_mapped_name"], row["final_linked_name"]), axis=1)
df_scoring_result['score_diff'] = abs(df_scoring_result["token_set_ratio"] - df_scoring_result["token_sort_ratio"])
df_scoring_result['score_avg'] = (df_scoring_result["token_set_ratio"] + df_scoring_result["token_sort_ratio"])/2
df_scoring_result['validation_needed'] = np.where((df_scoring_result['score_avg'] < 70) | (df_scoring_result['score_diff'] > 40), 'name_needs_val', 'no_val_req')
                                                                                                                                        

cols_full = [x for x in df_scoring_result if x not in df_merged.columns]
df_result_full = df_merged[~df_merged.id_to_merge_on.isna()].merge(df_scoring_result[~df_scoring_result.id_to_merge_on.isna()][cols_full+['id_to_merge_on']], how='left', on ='id_to_merge_on')
df_result_full['id_to_merge_on_linked_name'] = df_result_full['final_linked_name'] + df_result_full['cntry']

#---------------------------------------------

#AB Catalogue
ab_catalogue = read_ab_catalogue()

ab_catalogue['Basic No'] = ab_catalogue['Basic No'].astype(str)
ab_catalogue['Country'] = ab_catalogue['Country'].astype(str)
ab_catalogue['id_country'] = ab_catalogue['Basic No'] + ab_catalogue['Country']
ab_catalogue = ab_catalogue.dropna(subset= ['Customer Name In English'])
ab_catalogue['full_nam_eng'] = ab_catalogue['Customer Name In English'].str.lower().str.replace("[^A-Za-z0-9& ]", " ", regex=True)
ab_catalogue['full_nam_eng'] = ab_catalogue['full_nam_eng'].str.replace(" +", " ", regex=True)
ab_catalogue['full_nam_eng'] = ab_catalogue['full_nam_eng'].str.strip()
ab_catalogue['full_nam_eng'] = ab_catalogue['full_nam_eng'].str.replace('[(x)\1{3,}]', '', regex=True)
ab_catalogue = ab_catalogue[ab_catalogue['full_nam_eng'].str.len() > 3]
ab_catalogue = ab_catalogue.sort_values(by = ['Active Customer'], ascending = False)
ab_catalogue = ab_catalogue.drop_duplicates(subset = ['full_nam_eng', 'Country'])
ab_catalogue['full_nam_eng_no_space'] = ab_catalogue['full_nam_eng'].str.lower().str.replace(' ', '')
ab_catalogue.reset_index(inplace = True)
ab_catalogue['full_nam_eng'] = ab_catalogue['full_nam_eng'].apply(remove_repeated_phrase)

#---------------------------------------------

df_result_full = agg_pre_results(df_result_full)

df_result_no_dup = df_result_full.drop_duplicates(subset= ['final_linked_name','cntry'])

#run ab_catalogue
df_ab_cleint = ab_client(df_result_no_dup, ab_catalogue, 80, 75, 85, 80, False)
df_all_countries = ab_client_all_countries(df_ab_cleint, ab_catalogue, 85, 80, False)

cols_full_after_ab = [x for x in df_ab_cleint if x not in df_full_before_ab_client.columns]
df_ab_cleint.loc[df_ab_cleint.ab_client_flag == '', 'ab_client_flag']  = 'not_found_country_catalogue'

df_full_after_ab = df_result_full.merge(df_ab_cleint[cols_full_after_ab+['id_to_merge_on_linked_name']], how='left', on ='id_to_merge_on_linked_name')
df_full_after_ab['final_linked_name']  =  df_full_after_ab['final_linked_name'].str.title()                    
df_full_after_ab['ab_client_flag_score'] = df_full_after_ab['ab_client_flag_score'].fillna(0)

df_full_after_ab = agg_results(df_full_after_ab)

df_insert = df_full_after_ab[needed_cols_for_dash]

df_all = pd.concat([df_insert, df_empty_raw_messages], ignore_index=True)

numeric_cols = list(df_all.select_dtypes(include=['number']).columns)
object_cols = list(df_all.select_dtypes(include=['object']).columns)

df_all[object_cols] = df_all[object_cols].fillna('')
df_all[numeric_cols] = df_all[numeric_cols].fillna(0.0)

df_all['run_time'] = dt.now()

df_all.ab_client_name = df_all.ab_client_name.str.capitalize()

df_all_countries.rename(columns = {'ref':'referance'}, inplace = True)


def columns_types(df) :
  for col in df.columns.tolist():
    print(col)
    if 'date' in col.lower():
      df[col] = pd.to_datetime(df[col])

  types = []
  for j in range(df.shape[1]):
    types.append(str(pd.DataFrame(df.dtypes).iloc[j,0]))
  return types



#
#if not manual_run:
#  
#  insert_data(df_all, "Leadtech_results", df_all.columns.to_list(), columns_types(df_all), [''])
#  insert_data(df_all_countries, "leadtech_non_ab_global", df_all_countries.columns.to_list(), columns_types(df_all_countries) , [''])
#
#  #save_results(df_insert)
