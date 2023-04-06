import datetime
from io import BytesIO
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from io import StringIO
from sqlalchemy import create_engine

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 1, 13),
}

dag = DAG('ingestion_vendas_silver_to_gold', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )
		
data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

mysql_server = Variable.get("mysql_server")
mysql_login = Variable.get("mysql_login")
mysql_password = Variable.get("mysql_password")

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False
    )
	
def extract_silver_to_gold():
    
	#caminho do arquivo temporario
	path_ = '/tmp/'
    
	#Produtos
	objects = client.list_objects('silver', prefix='olist/products/',recursive=True)
	for obj in objects:
		obj = client.fget_object(
				obj.bucket_name,
				obj.object_name,
				path_ + "products.parquet"        
		)  
		df_produtos = pd.read_parquet(path_ + "products.parquet") 

	df_produtos = df_produtos[['product_category_name','product_id']]
	
	#order
	objects = client.list_objects('silver', prefix='olist/orders/',recursive=True)
	for obj in objects:
		obj = client.fget_object(
				obj.bucket_name,
				obj.object_name,
				path_ + "orders.parquet"         
		)  
		df_orders = pd.read_parquet(path_ + "orders.parquet")    
	
	df_orders = df_orders[['order_purchase_timestamp','order_id','customer_id']]
	
	#order_items
	objects = client.list_objects('silver', prefix='olist/order_items/',recursive=True)
	for obj in objects:
		obj = client.fget_object(
				obj.bucket_name,
				obj.object_name,
				path_ + "order_items.parquet"
		)  
		df_order_items = pd.read_parquet(path_ + "order_items.parquet") 
		
	df_order_items = df_order_items[['price','freight_value','order_id','product_id']]
	
	#orders_payments
	objects = client.list_objects('silver', prefix='olist/order_payments/',recursive=True)
	for obj in objects:
		obj = client.fget_object(
				obj.bucket_name,
				obj.object_name,
				path_ + "order_payments.parquet"         
		)  
		df_order_payments = pd.read_parquet(path_ + "order_payments.parquet")    
	
	df_order_payments = df_order_payments[['payment_type','order_id', 'payment_value']]
	#feito pivot de colunas de forma de pagamento
	df_order_payments = df_order_payments.pivot_table(index='order_id', columns='payment_type', aggfunc='sum')
	df_order_payments = df_order_payments.payment_value
	
	#customers
	objects = client.list_objects('silver', prefix='olist/customers/',recursive=True)
	for obj in objects:
		obj = client.fget_object(
				obj.bucket_name,
				obj.object_name,
				path_ + "customers.parquet"         
		)  
		df_customers = pd.read_parquet(path_ + "customers.parquet")    
	
	df_customers = df_customers[['customer_city','customer_state','customer_id']]
	
	#DF consolidado
	df_vendas = df_order_items\
		.merge(df_orders, left_on='order_id', right_on='order_id', how='left')\
		.merge(df_produtos, left_on='product_id', right_on='product_id', how='left')\
		.merge(df_customers, left_on='customer_id', right_on='customer_id', how='left')\
		.merge(df_order_payments, left_on='order_id', right_on='order_id', how='left')
	
	#trata valores NaN para constante 0
	df_vendas['price'].fillna(0, inplace=True)
	df_vendas['freight_value'].fillna(0, inplace=True)
	df_vendas['boleto'].fillna(0, inplace=True)
	df_vendas['credit_card'].fillna(0, inplace=True)
	df_vendas['debit_card'].fillna(0, inplace=True)
	df_vendas['not_defined'].fillna(0, inplace=True)
	df_vendas['voucher'].fillna(0, inplace=True)
	
	#trata coluna order_purchase_timestamp de datetime para date
	df_vendas["order_purchase_timestamp"] = pd.to_datetime(df_vendas["order_purchase_timestamp"]).dt.date 
	
	#trata descrição para maiuscula
	df_vendas['product_category_name'] = df_vendas['product_category_name'].str.upper()
	df_vendas['customer_city'] = df_vendas['customer_city'].str.upper()
	
	#trata descrição de categoria do produto
	df_vendas['product_category_name'] = df_vendas['product_category_name'].str.replace("_", " ")
	
	#renomeia colunas
	df_vendas = df_vendas.rename(columns={
										'price': 'VALOR_VENDA'
										,'freight_value': 'FRETE'
										,'order_purchase_timestamp': 'DATA_VENDA'
										,'order_id': 'PEDIDO'
										,'product_id': 'ID_PRODUTO'
										,'customer_id': 'ID_CLIENTE'
										,'product_category_name': 'CATEGORIA_PRODUTO'
										,'customer_city': 'CIDADE_CLIENTE'
										,'customer_state': 'ESTADO_CLIENTE'
										,'boleto': 'VALOR_BOLETO'
										,'credit_card': 'VALOR_CREDITO'
										,'debit_card': 'VALOR_DEBITO'
										,'voucher': 'VALOR_VOUCHER'
										,'not_defined': 'VALOR_NAO_DEFINIDO'
										}
								)
	
	#gera arquivo parquet em temporaria
	df_vendas.to_parquet(
			path_ + "vendas.parquet"
			,index=False
		)
		
	#carrega arquivo de vendas na zona gold
	client.fput_object(
			"gold",
			"olist/vendas/vendas.parquet",
			path_ + "vendas.parquet"
		)

def venda_to_mysql():
	
	#caminho do arquivo temporario
	path_ = '/tmp/'
	
	#Dataframe de vendas
	df_vendas = pd.read_parquet(path_ + "vendas.parquet")
	
	#cria conexão com MYSQL
	engine = create_engine(
		"mysql+mysqldb://" + mysql_login +":" + mysql_password + "@" + mysql_server + "/BD_STACK"
	)
	
	conn = engine.connect()
	
	#insert no MYSQL
	df_vendas.to_sql(con=conn, name='TB_VENDAS', if_exists='replace', index=False)
	
	
silver_to_gold = PythonOperator(
    task_id='extract_silver_to_gold',
    provide_context=True,
    python_callable=extract_silver_to_gold,
    dag=dag
)

venda_to_bd = PythonOperator(
    task_id='venda_to_mysql',
    provide_context=True,
    python_callable=venda_to_mysql,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

silver_to_gold >> venda_to_bd >> clean_task