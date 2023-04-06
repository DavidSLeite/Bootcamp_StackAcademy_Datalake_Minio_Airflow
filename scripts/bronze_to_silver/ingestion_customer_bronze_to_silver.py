import datetime
from io import BytesIO
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from io import StringIO

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 1, 13),
}

dag = DAG('ingestion_customer_bronze_to_silver', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )
		
data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False
    )
	
def extract():
    #schema df_customers
    df_customers = pd.DataFrame(data=None, columns=["customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state"])

    #lista objetos do bucket
    objects = client.list_objects('bronze', prefix='olist/customers/',
                                  recursive=True)
    for obj in objects:
        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode('utf-8'))

        #iterar sobre cada item no bucket
        obj = client.get_object(
                obj.bucket_name,
                obj.object_name.encode('utf-8')
        )

        #retornando os dados em Bytes
        dadosBytes = obj.read()

        #converte os dados em Bytes para DataFrame Pandas
        data =str(dadosBytes,'utf-8')
        data = StringIO(data) 
        df_ = pd.read_csv(data, sep=',')

        df_customers = pd.concat([df_customers,df_])
    
    #persiste o dataset em área de Staging tmp.
    df_customers.to_csv("/tmp/customers.csv"
                    ,index=False
                )

def load():
    
    #ler os dados a partir da área de Staging tmp.
    df_ = pd.read_csv("/tmp/customers.csv")

    #converte os dados para o formato parquet.
    df_.to_parquet("/tmp/customers.parquet",index=False)

    #carrega os dados para o Data Lake.
    client.fput_object(
        "silver",
        "olist/customers/customers.parquet",
        "/tmp/customers.parquet"
        )
		
extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)


load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/customers.csv;rm -f /tmp/customers.json;rm -f /tmp/customers.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task