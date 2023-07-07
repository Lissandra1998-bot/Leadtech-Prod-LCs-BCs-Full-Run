# ADD HERE THE BANK THINGY
  
from fuzzywuzzy import fuzz
import numpy as np

def result_scoring(df):
#  df = df_res_1_2_3.copy()
  
  df['raw_message_cln'] = df['raw_message'].str.lower().str.replace("[^A-Za-z0-9]", "", regex=True)
  df['result_1_cln'] = df['result_1'].str.lower().str.replace("[^A-Za-z0-9]", "", regex=True)
  df['result_2_cln'] = df['result_2'].str.lower().str.replace("[^A-Za-z0-9]", "", regex=True)
  df['result_3_cln'] = df['result_3'].str.lower().str.replace("[^A-Za-z0-9]", "", regex=True)

  df['result_2_orig_sort'] = df.apply(lambda x: fuzz.token_sort_ratio(x['result_2_cln'], x['raw_message_cln']), axis=1)

  df['result_3_orig_sort'] = df.apply(lambda x: fuzz.token_sort_ratio(x['result_3_cln'].lower(), x['raw_message_cln']), axis=1)
  
  df['score2_score3_diff'] =  df['result_2_orig_sort'] - df['result_3_orig_sort']

  df['final_mapped_name'] = ''
  df['final_score'] = 0
  df['naming_method'] = ''

  
  df['naming_method'] = df[['result_2_orig_sort','result_3_orig_sort']].idxmax(axis = 1)  
  df.loc[df['result_2_orig_sort'] == df['result_3_orig_sort'],'naming_method'] = 'result_3_orig_sort'  
  
  df['naming_method'] = np.where((df['naming_method'] == 'result_2_orig_sort') & (((df['result_1_pos_cut'] != -1) & (df['score2_score3_diff'] > 15)) | (df['score2_score3_diff'] > 30)), 'result_2_orig_sort', 'result_3_orig_sort')
  
  df['naming_method'] = df['naming_method'].str[:-10]
  
  df['final_score'] = np.where(df['naming_method'] == 'result_2', df['result_2_orig_sort'],df['result_3_orig_sort'])

  df['final_mapped_name'] = np.where(df['naming_method'] == 'result_2', df['result_1'], df['result_3'])
 
  df['final_mapped_name'] = df['final_mapped_name'].str.title()

  return df

