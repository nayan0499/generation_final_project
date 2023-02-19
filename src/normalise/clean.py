import pandas as pd

SENSITIVE_COLUMNS = ["customer_name", "card_number"]


def remove_user_info(data_frame, sensitive_columns):
    return data_frame.drop(columns=SENSITIVE_COLUMNS)

def add_transaction_col(df): 
    df['transaction_id'] =  df.index
    return df

def split_products(df): 
    df = df.assign(items=df['products'].str.split(',')).explode('items').reset_index(drop=True)
    df.rename(columns={"items":"name"}, inplace = True)
    df = df.drop(columns=['products'])
    df.index = df.index+1
    return df 

def add_quantity(df):
    df['quantity'] = df.groupby(['name', 'transaction_id'])['transaction_id'].transform('count')
    return df

def remove_duplicates(df): 
    df = df.drop_duplicates().reset_index(drop=True)
    df.index = df.index+1
    return df 

def add_price_col(df): 
    df['price'] = df['name'].str.extract(r'(\d+\.\d+)').astype(float)
    return df 
def remove_price(df): 
    df['name'] = df['name'].replace(to_replace=r'[^a-zA-Z ]+', value='', regex=True)
    return df 

def format_date_time(df):
    df['date_time'] = pd.to_datetime(dataframe['date_time'])
    return df 
    

def get_clean_df(df):
    df = add_transaction_col(df)
    df =split_products(df)
    df = add_quantity(df)
    df = remove_duplicates(df)
    df = add_price_col(df)
    df = remove_price(df)
    df = format_date_time(df)
    return df 

def get_product_df(df):
    df = df.drop(columns=['branch_name', 'date_time', 'payment_method','total_price'])
    return df 
def get_transaction_df(df):
    df = df.drop(columns=['name','price','transaction_id','quantity'])
    return df
    
