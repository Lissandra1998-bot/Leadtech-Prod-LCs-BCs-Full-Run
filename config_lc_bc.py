import pandas as pd
import numpy as np
import re
from impala import dbapi
from impala.dbapi import connect
conn = dbapi.connect(host="cdhgtw.hqservices.arabbank.plc" , 
                 timeout=None,
                 port=21050,
                 use_ssl=True,
                 auth_mechanism='GSSAPI',
                 kerberos_service_name='impala',
                 database= 'data_science'
                 )

def get_data_bc_lc():  
  
    query_all = pd.read_sql("""select * From Leadtech_results_bk_11_06""",conn)
   
    query_all = pd.read_sql("""select * From leadtech_trade_v""",conn)
    query_all.rename(columns={"bank_name": "Bank Name" ,"bank_code": "Bank Code" },inplace=True)    

    #query_all.to_csv('Data/query_all.csv', index=False)
    
    return query_all
  
  
def read_ab_catalogue():  

    ab_catalogue = pd.read_sql("""SELECT * FROM leadtech_ab_catalogue""",conn)

    return ab_catalogue  
  

def remove_repeated_phrase(text):
  words = text.split()
  stride = int(len(words)/2)
  bigrams = [' '.join(words[i: i+stride]) for i in range(len(words)-1)]
  counts = Counter(bigrams)
  
  # find repeated bigrams
  repeated_phrases = [phrase for phrase, count in counts.items() if count > 1]
  
  # if a phrase is repeated, remove one intance of it
  if len(repeated_phrases) > 0:
    first_index = text.find(repeated_phrases[0]) #find the index of the first occurance
    second_index = text.find(repeated_phrases[0], first_index + 1) #find the index of the second occurance
    
    if second_index != -1:
      text = text[:second_index] + text[second_index:].replace(repeated_phrases[0], '', 1)
      
    text = text.strip() #remove any leading or traiiling white space
    
  return text  
  
  
def agg_pre_results(df):

  
  # Assuming df is your DataFrame and 'name' is the column with customer names

  flags_priority = ['AB Client SWIFT', 'EurAB Client SWIFT', 'Central Banks Client SWIFT', 'IIAB', 'Other Banks', 'Unknown']

  df['ab_client_swift_flag'] = 'Unknown' # Initialize the Unified_Flag column with 'Unknown'

  # List unique customer names
  customer_names = df['id_to_merge_on_linked_name'].unique()

  for customer in customer_names:
    customer_df = df[df['id_to_merge_on_linked_name'] == customer]

    for flag in flags_priority:
      if flag in customer_df['ab_client_name_if_stat'].values:
        df.loc[df['id_to_merge_on_linked_name'] == customer, 'ab_client_swift_flag'] = flag
        break  
              
  return df  

  
  
def agg_results(df):

  df['ab_flag_final'] = np.where(df['ab_client_flag'] == 'ab_client_not_found', 'AB Client-Not Found',
                        np.where(df['ab_client_swift_flag'] == 'AB Client SWIFT', 'AB Client',
                        np.where(df['ab_client_flag'] == 'ab_client', 'AB Client',
                        np.where(df['ab_client_flag'] == 'need_validation', 'AB Client needs validation',
                        np.where(df['ab_client_swift_flag'] == 'EurAB Client SWIFT', 'Europe AB Client',
                        np.where(df['ab_client_swift_flag'] == 'Central Banks Client SWIFT', 'Central Bank Client',         
                        np.where(df['ab_client_swift_flag'] == 'IIAB', 'IIAB Client',  
                        np.where(df['ab_client_flag'] == "non_ab_client", "Non AB Client",
                        np.where(df['ab_client_swift_flag'] == 'Other Banks', 'Non AB Client',         
                        'Unknown')))))))))
                                 
                                 
  return df


def fill_country(df):

  df_countries = pd.read_excel('Data/iso_2digit_alpha_country_codes_v2.xlsx', engine='openpyxl')
  df_countries.columns = ['country_code', 'country']
  list_countries = df_countries['country'].to_list()
  threshold = 0.9
  for idx, row in df.iterrows():
    
    to_search_in = row['raw_message'].lower().replace(' ', '')
    to_search_in = re.sub( "[^A-Za-z]", "", to_search_in)
    
    for j in range(len(list_countries)):
          if list_countries[j].lower().replace(' ', '') in to_search_in :
            df.at[idx,'country_fuzz'] = list_countries[j]
            df.at[idx,'country_fuzz_code'] = df_countries[df_countries['country'] == list_countries[j]].iloc[0,0]
            break
  return df


def save_results(results, table_name):
  from impala import dbapi

  conn = dbapi.connect(host = host_, timeout = None, port = port_,
                          use_ssl = use_ssl_, auth_mechanism = auth_mechanism_,
                          kerberos_service_name = kerberos_service_name_, 
                          database = results_db)     

  cursor = conn.cursor()

  cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
  
  if table_name == 'leadtech_table_1':
    cursor.execute(f'''create table if not exists sme_subsegments (customer_bsc string, segment string, last3m_avg_balance double, last3m_avg_balance_bucket string, approved_limit double, approved_limit_bucket string, avg_6m_credits double, avg_6m_credits_bucket string, created_at timestamp)''')
  elif table_name == 'leadtech_table_1':
    cursor.execute(f'''create table if not exists sme_subsegments (customer_bsc string, segment string, last3m_avg_balance double, last3m_avg_balance_bucket string, approved_limit double, approved_limit_bucket string, avg_6m_credits double, avg_6m_credits_bucket string, created_at timestamp)''')

  final_lists = []

  for i, content in enumerate(np.array(results)):

    final_lists.append((content[0],content[1], content[2], content[3],content[4], content[5],content[6],content[7], date.today()))

  database_inserts = [f"('%s', '%s', %.2f, '%s', %.2f, '%s', %.2f, '%s', '%s')" % final_list_copy for final_list_copy in final_lists]

  for i in range(0, len(database_inserts), 1000):

      insert_batch = database_inserts[i:i + 1000]
      cursor.execute(f'insert into sme_subsegments values {",".join(insert_batch)}')
def insert_data(df, table_name, cols, types, historical_tables):
  print('Inserting', table_name)
  types = ['timestamp' if 'datetime' in i else i for i in types]

  types = [i if i == 'timestamp' else 'string' if i == 'object' or i == 'nvarchar(10)' else 'int' if i =='int64' else 'float' for i in types]
  
  conn = connect(host="cdhgtw.hqservices.arabbank.plc" , 
                   timeout=None,
                   port=21050,
                   use_ssl=True,
                   auth_mechanism='GSSAPI',
                   kerberos_service_name='impala',
                   database= 'data_science'
                   )
  
  cols = [col.replace(' ', '_') for col in cols]
  
  cols_types = []
  for i in range(len(cols)):
    cols_types.append(f"{cols[i].replace('-','_')} {types[i]}")
    
  cols_string = ', '.join(cols_types)
  cursor = conn.cursor()
  
#  if table_name not in historical_tables:
#    cursor.execute(f"TRUNCATE TABLE IF EXISTS data_science.{table_name}")
  cursor.execute(f"CREATE TABLE IF NOT EXISTS data_science.{table_name} ({cols_string})")

  database_inserts = []

  database_inserts = [df.iloc[i].to_list() for i in range(df.shape[0])]
  
  subs = ''
  for i in range(len(types)):
    if types[i] == 'string' or types[i] == 'timestamp':
      subs = subs + '"%s",'
    
    elif types[i] == 'int':
      subs = subs + "%i,"
    else:
      subs = subs + "%0.2f,"

  subs = subs[:-1]
  database_inserts = [f"({subs})" % tuple(i) for i in database_inserts]
  
  for i in range(0, len(database_inserts), 1000):
      print(i)
      insert_batch = database_inserts[i:i + 1000]
      cursor.execute(f'insert into data_science.{table_name} values {",".join(insert_batch)}')
      

def clean_final_name(df):
  
  df['final_mapped_name'] = df['final_mapped_name'].str.replace("[^A-Za-z0-9& ]", " ", regex=True)
  df['final_mapped_name'] = df['final_mapped_name'].str.replace("\d{3,}", "", regex=True)
  df['final_mapped_name'] = df['final_mapped_name'].str.replace(" +", " ", regex=True)
  df['final_mapped_name'] = df['final_mapped_name'].str.strip()


  df['final_mapped_name'] = np.where(df['final_mapped_name'].str.lower().str.startswith('the '),
                                                    df['final_mapped_name'].str[4:],
                                                    df['final_mapped_name'])


  df['final_mapped_name'] = np.where(df['final_mapped_name'].str.lower().str.startswith('sarl '),
                                                    df['final_mapped_name'].str[5:],
                                                    df['final_mapped_name'])


  df['final_mapped_name'] = np.where(df['final_mapped_name'].str.lower().str.startswith('eurl '),
                                                    df['final_mapped_name'].str[5:],
                                                    df['final_mapped_name'])


  df['final_mapped_name'] = np.where(df['final_mapped_name'].str.lower().str.startswith('sas '),
                                                    df['final_mapped_name'].str[4:],
                                                    df['final_mapped_name'])
  
  df['final_mapped_name'] = np.where(df['raw_message'].str.lower().str.contains('^m/s', regex=True), df['final_mapped_name'].str.lower().str.replace('^ms', '', regex=True), df['final_mapped_name'])

  df['id_to_merge_on'] = df['final_mapped_name'] + "_" +df['cntry']

  return df