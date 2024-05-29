'''
=================================================
Milestone 3

Nama  : Reski Hidayat
Batch : FTDS-015-HCK

Program ini dibuat untuk melakukan automatisasi transform dan load data dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset mengenai penjualan mobil di Indonesia selama tahun 2020.
=================================================
'''


from airflow.models import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.utils.task_group import TaskGroup
from datetime import datetime
from sqlalchemy import create_engine #koneksi ke postgres
import pandas as pd

from elasticsearch import Elasticsearch
# from elasticsearch.helpers import bulk

def load_csv_to_postgres():
    
    """
    Loads data from a CSV file into a PostgreSQL database table.

    This function connects to a PostgreSQL database using the provided credentials and loads data from a CSV file into a specified table. The CSV file must be located at '/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv'. The table in the database will be created or replaced with the data from the CSV file.

    """

    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    # engine= create_engine("postgresql+psycopg2://airflow:airflow@postgres/airflow")
    conn = engine.connect()

    df = pd.read_csv('/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv')
    #df.to_sql(nama_table_db, conn, index=False, if_exists='replace')
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  # M
    

def ambil_data():
    '''
    Retrieves data from a PostgreSQL database table and saves it to a CSV file.

    This function connects to a PostgreSQL database using the provided credentials, retrieves data from a specified table, and saves it to a CSV file located at '/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv'. The table name in the database should be 'table_m3'.

    '''
    database = "airflow_m3"
    username = "airflow_m3"
    password = "airflow_m3"
    host = "postgres"

    # Membuat URL koneksi PostgreSQL
    postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

    # Gunakan URL ini saat membuat koneksi SQLAlchemy
    engine = create_engine(postgres_url)
    conn = engine.connect()

    df = pd.read_sql_query("select * from table_m3", conn) #nama table sesuaikan sama nama table di postgres
    df.to_csv('/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv', sep=',', index=False)
    


def preprocessing(): 
    '''
    Preprocesses the data by cleaning and normalizing column names.

    This function reads data from the raw CSV file located at '/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv', cleans the data by normalizing column names, removes duplicate rows, and drops rows with missing values. The cleaned data is then saved to a new CSV file located at '/opt/airflow/dags/P2M3_Reski_Hidayat_data_clean.csv'.

    '''
    # Pengambilan data
    df = pd.read_csv("/opt/airflow/dags/P2M3_Reski_Hidayat_data_raw.csv")
  
    # Pembersihan data 
     # Normalisasi nama column
    data= df.copy()
    data.columns = data.columns.str.lower()  # Ubah menjadi lowercase
    data.columns = data.columns.str.replace('  ', ' ')  # Ganti spasi dengan underscore
    data.columns = data.columns.str.replace(' ', '_')  # Ganti spasi dengan underscore
    data.columns = data.columns.str.strip('|')  # Hapus simbol '|' di awal atau akhir nama column

    # Pembersihan data
    data = data.drop_duplicates()  # Hapus data yang duplikat
    data = data.dropna()  # Hapus baris yang memiliki nilai kosong (missing values)
    data.to_csv('/opt/airflow/dags/P2M3_Reski_Hidayat_data_clean.csv', index=False)
    
def upload_to_elasticsearch():
    '''
    Uploads data from a cleaned CSV file to Elasticsearch.

    This function establishes a connection to Elasticsearch running at 'http://elasticsearch:9200',
    reads data from the cleaned CSV file located at '/opt/airflow/dags/P2M3_Reski_Hidayat_data_clean.csv',
    iterates through each row of the DataFrame, converts it to a dictionary, and indexes it into Elasticsearch
    under the index 'table_m3'. The row number incremented by 1 is used as the document ID.

    '''
    es = Elasticsearch("http://elasticsearch:9200")
    df = pd.read_csv('/opt/airflow/dags/P2M3_Reski_Hidayat_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()  # Convert the row to a dictionary
        res = es.index(index="table_m3", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Reski', 
    'start_date': datetime(2023, 12, 24, 12, 00)
}

with DAG(
    "P2M3_Reski_Hidayat", #atur sesuai nama project kalian
    description='Milestone_3',
    schedule_interval='30 6 * * *', #atur schedule untuk menjalankan airflow pada 06:30.
    default_args=default_args, 
    catchup=False
) as dag:
    # Task : 1
    load_csv_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres) #sesuaikan dengan nama fungsi yang kalian buat
    
    #task: 2
    ambil_data_pg = PythonOperator(
        task_id='ambil_data_postgres',
        python_callable=ambil_data) #
    

    # Task: 3
    '''  Fungsi ini ditujukan untuk menjalankan pembersihan data.'''
    edit_data = PythonOperator(
        task_id='edit_data',
        python_callable=preprocessing)

    # Task: 4
    upload_data = PythonOperator(
        task_id='upload_data_elastic',
        python_callable=upload_to_elasticsearch)

    #proses untuk menjalankan di airflow
    load_csv_task >> ambil_data_pg >> edit_data >> upload_data