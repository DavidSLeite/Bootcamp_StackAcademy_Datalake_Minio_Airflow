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

dag = DAG(
		  dag_id = "ingestion_order_items_bronze_to_silver",
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
    df_customers = pd.DataFrame(data=None, columns=["order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"])

    #lista objetos do bucket
    objects = client.list_objects('bronze', prefix='olist/order_items/',
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
    df_customers.to_csv("/tmp/order_items.csv"
                    ,index=False
                )

def transform():
    #ler os dados a partir da área de Staging tmp.
    df_ = pd.read_csv("/tmp/order_items.csv")
    
    #Converte dado para data hora
    df_["shipping_limit_date"] = pd.to_datetime(df_["shipping_limit_date"])  
    
				
def load():
    
    #ler os dados a partir da área de Staging tmp.
    df_ = pd.read_csv("/tmp/order_items.csv")

    #converte os dados para o formato parquet.
    df_.to_parquet("/tmp/order_items.parquet",index=False)

    #carrega os dados para o Data Lake.
    client.fput_object(
        "silver",
        "olist/order_items/order_items.parquet",
        "/tmp/order_items.parquet"
        )
		
extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_file',
    provide_context=True,
    python_callable=transform,
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
    bash_command="rm -f /tmp/order_items.csv;rm -f /tmp/order_items.json;rm -f /tmp/order_items.parquet;",
    dag=dag
)

extract_task >> transform_task >> load_task >> clean_task