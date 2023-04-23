
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import config

def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    # download file from s3 to local storage
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name
    
def transform_and_load(key:str ,local_path: str) -> str:
   # read sales data 
    df = pd.read_csv(local_path + key,dtype={
                     'ORDER ID': str,
                     'Category':str,
                     'PRODUCT CODE': str,
                     'Price': float,
                     'quantity' : int,
                     'Discount':float,
                     'Total amt':float,
                 }, parse_dates=['Createddt'])

    # read products master dats
    product_df = pd.read_csv(config.products_master_path,dtype={
                     'PRODUCT ID': str,
                     'PRODUCT Description': str
                 })

    # validate product code with master data of products 
    df = df.merge(product_df, left_on = 'PRODUCT CODE', right_on = 'PRODUCT ID', how = 'left')

    invalid_prod_code = pd.DataFrame()
    invalid_prod_code['data'] = df[df['PRODUCT ID'].isnull()].apply(lambda x: ','.join(x.astype(str)),axis=1)
    invalid_prod_code['validation'] = 'Product code is not valid'
    df = df[~df['PRODUCT ID'].isnull()]

    #validate created date
    invalid_date = pd.DataFrame()
    invalid_date['data'] = df[ df['Createddt'] > datetime.now().date ].apply(lambda x: ','.join(x.astype(str)),axis=1)
    invalid_date['validation'] = 'Created date is invalid'
    df = df[ df['Createddt'] <= datetime.now().date ]
    
    # validate discount value
    invalid_discount = pd.DataFrame()
    invalid_discount['data'] = df[ df['Discount'] > 100.0 | df['Discount'] < 0.0 ].apply(lambda x: ','.join(x.astype(str)),axis=1)
    invalid_discount['validation'] = 'Discount value is invalid'
    df = df[ df['Discount'] < 100.0 & df['Discount'] > 0.0 ]

    # remove rows with null vales for required columns
    null_values = []
    for column in ['ORDER ID','Price','quantity','Createddt']:
        null_value = pd.DataFrame()
        null_value['data'] = df[df[column].isnull()].apply(lambda x: ','.join(x.astype(str)),axis=1)
        null_value['validation'] = column + ' value is null'
        null_values.append(null_value)
        df = df[~df[column].isnull()]

    # remove duplicate values for orderid as it should be unique
    duplicate_orderid = pd.DataFrame()
    duplicate_orderid['data'] = df[df.duplicated(subset=['ORDER ID'])].apply(lambda x: ','.join(x.astype(str)),axis=1)
    duplicate_orderid['validation'] = 'duplicate orderid'
    df.drop_duplicates(subset=['ORDER ID'], inplace=True)

    # rename and select only required columns
    df.rename(columns = { 'ORDER ID': 'orderid',
                     'Category': 'category',
                     'PRODUCT CODE': 'productcode',
                     'Price': 'price',
                     'quantity' : 'quantity',
                     'Discount': 'discount',
                     'Total amt': 'totalamt',
                     'Createddt' : 'createddt',
                     'PRODUCT Description': 'productdesc'}, inplace = True)
    
    df = df[['orderid', 'category', 'productcode', 'productdesc', 'price', 'quantity', 'discount','totalamt','createddt','PRODUCT Description']]

    # merge all invalid dfs into one df
    invalid_df = pd.concat(null_values.extend([invalid_date,invalid_prod_code,duplicate_orderid,invalid_discount]))
    
    # load to postgres table
    postgres_hook = PostgresHook(postgres_conn_id=config.POSTGRES_CONN)
    df.to_sql(config.TAGRET_TABLE, postgres_hook.get_sqlalchemy_engine(), schema = config.POSTGRES_SCHEMA, if_exists='append', chunksize=1000)
    invalid_df.to_sql(config.VALIDATION_TABLE, postgres_hook.get_sqlalchemy_engine(), schema = config.POSTGRES_SCHEMA, if_exists='append', chunksize=1000)


with DAG(dag_id="sales_csv_to_postgres",
     start_date=datetime.datetime(2023, 4, 24),
     schedule="@daily",) as dag:

    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': config.FILENAME,
            'bucket_name': config.BUCKET_NAME,
            'local_path': config.LOCAL_PATH,
        },
        email_on_failure=True
    )

    task_transform_and_load = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load,
        op_kwargs={
            'key': config.FILENAME,
            'local_path': config.LOCAL_PATH
        },
    )

    task_download_from_s3 >> task_transform_and_load

