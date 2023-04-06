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

dag = DAG('ingestion_products_bronze_to_silver', 
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
    df_customers = pd.DataFrame(data=None, columns=["product_id","product_category_name","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm"])

    #lista objetos do bucket
    objects = client.list_objects('bronze', prefix='olist/products/',
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
    df_customers.to_csv("/tmp/products.csv"
                    ,index=False
                )

def load():
    
    #ler os dados a partir da área de Staging tmp.
    df_ = pd.read_csv("/tmp/products.csv")

    #converte os dados para o formato parquet.
    df_.to_parquet("/tmp/products.parquet",index=False)

    #carrega os dados para o Data Lake.
    client.fput_object(
        "silver",
        "olist/products/products.parquet",
        "/tmp/products.parquet"
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
    bash_command="rm -f /tmp/products.csv;rm -f /tmp/products.json;rm -f /tmp/products.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task