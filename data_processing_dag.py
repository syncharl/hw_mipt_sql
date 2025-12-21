from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator as EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import pandas as pd
import os
import psycopg2
from contextlib import closing

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def success_callback(context):
    print("DAG выполнен УСПЕШНО!")

def failure_callback(context):
    print("DAG выполнен с ОШИБКОЙ!")

dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Ежедневная обработка данных из файлов',
    schedule_interval='@daily',
    catchup=False,
    on_success_callback=success_callback,
    on_failure_callback=failure_callback,
)

BASE_DIR = '/opt/airflow/dags/data/'
CUSTOMER_FILE = os.path.join(BASE_DIR, 'customer.csv')
PRODUCT_FILE = os.path.join(BASE_DIR, 'product.csv')
ORDERS_FILE = os.path.join(BASE_DIR, 'orders.csv')
ORDER_ITEMS_FILE = os.path.join(BASE_DIR, 'order_items.csv')
OUTPUT_DIR = '/opt/airflow/dags/out'

def load_customer_data():
    """Загрузка данных о клиентах"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_csv(CUSTOMER_FILE, sep=';')

        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS mipt;
            CREATE TABLE IF NOT EXISTS mipt.customer (
                customer_id int4 NOT NULL,
                first_name varchar(50) NULL,
                last_name varchar(50) NULL,
                gender varchar(50) NULL,
                dob varchar(50) NULL,
                job_title varchar(50) NULL,
                job_industry_category varchar(50) NULL,
                wealth_segment varchar(50) NULL,
                deceased_indicator varchar(50) NULL,
                owns_car varchar(50) NULL,
                address varchar(50) NULL,
                postcode int4 NULL,
                state varchar(50) NULL,
                country varchar(50) NULL,
                property_valuation int4 NULL,
                CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
            )
        """)
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO mipt.customer VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (customer_id) DO UPDATE SET
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    gender = EXCLUDED.gender,
                    dob = EXCLUDED.dob,
                    job_title = EXCLUDED.job_title,
                    job_industry_category = EXCLUDED.job_industry_category,
                    wealth_segment = EXCLUDED.wealth_segment,
                    deceased_indicator = EXCLUDED.deceased_indicator,
                    owns_car = EXCLUDED.owns_car,
                    address = EXCLUDED.address,
                    postcode = EXCLUDED.postcode,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    property_valuation = EXCLUDED.property_valuation
            """, tuple(row))
                        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Загружено {len(df)} записей в таблицу mipt.customer")

    except Exception as e:
        print(f"Ошибка при загрузке customer: {str(e)}")
        raise

def load_product_data():
    """Загрузка данных о продуктах"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_csv(PRODUCT_FILE)
        
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS mipt;
            CREATE TABLE IF NOT EXISTS mipt.product (
                product_id serial4 NOT NULL,
                brand varchar(50) NULL,
                product_line varchar(50) NULL,
                product_class varchar(50) NULL,
                product_size varchar(50) NULL,
                list_price numeric(10, 2) NULL,
                standard_cost numeric(10, 2) NULL,
                CONSTRAINT product_pkey PRIMARY KEY (product_id)
            )
        """)
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO mipt.product VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    brand = EXCLUDED.brand,
                    product_line = EXCLUDED.product_line,
                    product_class = EXCLUDED.product_class,
                    product_size = EXCLUDED.product_size,
                    list_price = EXCLUDED.list_price,
                    standard_cost = EXCLUDED.standard_cost
            """, tuple(row))
                
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Загружено {len(df)} записей в таблицу mipt.product")
    except Exception as e:
        print(f"Ошибка при загрузке product: {str(e)}")
        raise

def load_orders_data():
    """Загрузка данных о заказах"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_csv(ORDERS_FILE)

        df['online_order'] = df['online_order'].apply(
            lambda x: True if x == 'True' 
            else False if x == 'False' 
            else None if pd.isna(x) or str(x).lower() == 'nan' 
            else bool(x)
        )

        df['order_date'] = df['order_date'].astype(str).replace('nan', None)
        
        df['order_status'] = df['order_status'].astype(str).replace('nan', None)
        
        df['order_id'] = pd.to_numeric(df['order_id'], errors='coerce')
        df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')
        
        df = df.dropna(subset=['order_id'])

        conn = hook.get_conn()
        cursor = conn.cursor()    
       
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS mipt;
            CREATE TABLE IF NOT EXISTS mipt.orders (
                order_id int4 NOT NULL,
                customer_id int4 NULL,
                order_date date NULL,
                online_order bool NULL,
                order_status varchar(50) NULL,
                CONSTRAINT orders_pkey PRIMARY KEY (order_id)
            )
        """)
        
        for _, row in df.iterrows():
            values = (
                int(row['order_id']) if not pd.isna(row['order_id']) else None,
                int(row['customer_id']) if not pd.isna(row['customer_id']) else None,
                row['order_date'] if row['order_date'] != 'None' else None,
                row['online_order'],
                row['order_status'] if row['order_status'] != 'None' else None
            )

            cursor.execute("""
                INSERT INTO mipt.orders (order_id, customer_id, order_date, online_order, order_status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_date = EXCLUDED.order_date,
                    online_order = EXCLUDED.online_order,
                    order_status = EXCLUDED.order_status
            """, values)
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Загружено {len(df)} записей в таблицу mipt.orders")
    except Exception as e:
        print(f"Ошибка при загрузке orders: {str(e)}")
        raise

def load_order_items_data():
    """Загрузка данных о позициях заказов"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        df = pd.read_csv(ORDER_ITEMS_FILE)

        conn = hook.get_conn()
        cursor = conn.cursor()  
        
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS mipt;
            CREATE TABLE IF NOT EXISTS mipt.order_items (
                order_item_id INT PRIMARY KEY,
                order_id INT,
                product_id INT,
                quantity INT,
                item_list_price_at_sale DECIMAL(10,2),
                item_standard_cost_at_sale DECIMAL(10,2),
                FOREIGN KEY (order_id) REFERENCES mipt.orders(order_id),
                FOREIGN KEY (product_id) REFERENCES mipt.product(product_id)
            )
        """)
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO mipt.order_items VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (order_item_id) DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    product_id = EXCLUDED.product_id,
                    quantity = EXCLUDED.quantity,
                    item_list_price_at_sale = EXCLUDED.item_list_price_at_sale,
                    item_standard_cost_at_sale = EXCLUDED.item_standard_cost_at_sale
            """, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Загружено {len(df)} записей в таблицу mipt.order_items")
    except Exception as e:
        print(f"Ошибка при загрузке order_items: {str(e)}")
        raise

def execute_query_1():
    """Запрос 1: ТОП-3 минимальная и ТОП-3 максимальная сумма транзакций"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()

        query = """
        WITH customer_transactions AS (
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) as total_spent
            FROM mipt.customer c
            LEFT JOIN mipt.orders o ON c.customer_id = o.customer_id
            LEFT JOIN mipt.order_items oi ON o.order_id = oi.order_id
            GROUP BY c.customer_id, c.first_name, c.last_name
        ),
        min_top3 AS (
            SELECT first_name, last_name, total_spent, 'min' as type
            FROM customer_transactions
            ORDER BY total_spent ASC
            LIMIT 3
        ),
        max_top3 AS (
            SELECT first_name, last_name, total_spent, 'max' as type
            FROM customer_transactions
            ORDER BY total_spent DESC
            LIMIT 3
        )
        SELECT * FROM min_top3
        UNION ALL
        SELECT * FROM max_top3
        ORDER BY type, total_spent;
        """
        
        df = pd.read_sql_query(query, conn)

        conn.close()
        
        output_file = os.path.join(OUTPUT_DIR, 'query1_result.csv')
        df.to_csv(output_file, index=False)
        
        print(f"Результат запроса 1 сохранен в {output_file}")
        print(f"Количество строк: {len(df)}")
        
        return len(df)
            
    except Exception as e:
        print(f"Ошибка в запросе 1: {str(e)}")
        raise

def execute_query_2():
    """Запрос 2: ТОП-5 клиентов по доходу в каждом сегменте благосостояния"""
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()

        query = """
        WITH customer_income AS (
            SELECT 
                c.customer_id,
                c.first_name,
                c.last_name,
                c.wealth_segment,
                COALESCE(SUM(oi.quantity * oi.item_list_price_at_sale), 0) as total_income
            FROM mipt.customer c
            LEFT JOIN mipt.orders o ON c.customer_id = o.customer_id
            LEFT JOIN mipt.order_items oi ON o.order_id = oi.order_id
            GROUP BY c.customer_id, c.first_name, c.last_name, c.wealth_segment
        ),
        ranked_customers AS (
            SELECT 
                first_name,
                last_name,
                wealth_segment,
                total_income,
                ROW_NUMBER() OVER (PARTITION BY wealth_segment ORDER BY total_income DESC) as rank
            FROM customer_income
        )
        SELECT first_name, last_name, wealth_segment, total_income
        FROM ranked_customers
        WHERE rank <= 5
        ORDER BY wealth_segment, rank;
        """
        
        df = pd.read_sql_query(query, conn)

        conn.close()
        
        output_file = os.path.join(OUTPUT_DIR, 'query2_result.csv')
        df.to_csv(output_file, index=False)
        
        print(f"Результат запроса 2 сохранен в {output_file}")
        print(f"Количество строк: {len(df)}")
        
        return len(df)
            
    except Exception as e:
        print(f"Ошибка в запросе 2: {str(e)}")
        raise

def check_query_results_1(**context):
    """Проверка результатов запроса 1"""
    try:
        ti = context['ti']
        row_count = ti.xcom_pull(task_ids='execute_query_1')
        
        if row_count == 0:
            raise AirflowException("ОШИБКА: Запрос 1 вернул 0 строк")
        else:
            print(f"УСПЕШНО: Запрос 1 вернул {row_count} строк")
            
    except Exception as e:
        print(f"Ошибка при проверке запроса 1: {str(e)}")
        raise

def check_query_results_2(**context):
    """Проверка результатов запроса 2"""
    try:
        ti = context['ti']
        row_count = ti.xcom_pull(task_ids='execute_query_2')
        
        if row_count == 0:
            raise AirflowException("ОШИБКА: Запрос 2 вернул 0 строк")
        else:
            print(f"УСПЕШНО: Запрос 2 вернул {row_count} строк")
            
    except Exception as e:
        print(f"Ошибка при проверке запроса 2: {str(e)}")
        raise

start_task = EmptyOperator(
    task_id='start',
    dag=dag,
)

load_customer_task = PythonOperator(
    task_id='load_customer_data',
    python_callable=load_customer_data,
    dag=dag,
)

load_product_task = PythonOperator(
    task_id='load_product_data',
    python_callable=load_product_data,
    dag=dag,
)

load_orders_task = PythonOperator(
    task_id='load_orders_data',
    python_callable=load_orders_data,
    dag=dag,
)

load_order_items_task = PythonOperator(
    task_id='load_order_items_data',
    python_callable=load_order_items_data,
    dag=dag,
)

execute_query_1_task = PythonOperator(
    task_id='execute_query_1',
    python_callable=execute_query_1,
    dag=dag,
)

execute_query_2_task = PythonOperator(
    task_id='execute_query_2',
    python_callable=execute_query_2,
    dag=dag,
)

check_query_1_task = PythonOperator(
    task_id='check_query_1_results',
    python_callable=check_query_results_1,
    provide_context=True,
    dag=dag,
)

check_query_2_task = PythonOperator(
    task_id='check_query_2_results',
    python_callable=check_query_results_2,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end',
    dag=dag,
)

start_task >> [load_customer_task, load_product_task, load_orders_task, load_order_items_task]

load_customer_task >> [execute_query_1_task, execute_query_2_task]
load_product_task >> [execute_query_1_task, execute_query_2_task]
load_orders_task >> [execute_query_1_task, execute_query_2_task]
load_order_items_task >> [execute_query_1_task, execute_query_2_task]

execute_query_1_task >> check_query_1_task
execute_query_2_task >> check_query_2_task

check_query_1_task >> end_task
check_query_2_task >> end_task