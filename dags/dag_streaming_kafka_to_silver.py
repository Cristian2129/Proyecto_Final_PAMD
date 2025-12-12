"""
DAG para procesamiento continuo de Kafka a capa Silver
Ejecuta cada 5 minutos para verificar nuevos mensajes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def check_kafka_messages(**context):
    """
    Sensor: Verifica si hay mensajes pendientes en Kafka
    Retorna True si hay mensajes, False si no
    """
    from kafka import KafkaConsumer
    import json
    
    try:
        consumer = KafkaConsumer(
            'contratos-publicos',
            bootstrap_servers=['kafka:29092'],
            auto_offset_reset='latest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        # Verificar lag de mensajes
        partitions = consumer.partitions_for_topic('contratos-publicos')
        if not partitions:
            print("❌ No hay particiones disponibles")
            return False
        
        # Calcular mensajes pendientes
        pending_messages = 0
        for partition in partitions:
            tp = consumer._client.cluster.leader_for_partition(partition)
            if tp:
                end_offset = consumer.end_offsets([tp])[tp]
                current_offset = consumer.position(tp)
                pending_messages += (end_offset - current_offset)
        
        consumer.close()
        
        print(f"📊 Mensajes pendientes: {pending_messages}")
        
        # Si hay más de 100 mensajes pendientes, procesar
        if pending_messages > 100:
            context['ti'].xcom_push(key='pending_count', value=pending_messages)
            return True
        
        return False
        
    except Exception as e:
        print(f"⚠️ Error verificando Kafka: {str(e)}")
        return False


def process_kafka_to_silver(**context):
    """
    Ejecuta el script de PySpark para procesar Kafka → Silver
    """
    pending_count = context['ti'].xcom_pull(key='pending_count', task_ids='check_kafka_sensor')
    
    print(f"🚀 Procesando {pending_count} mensajes de Kafka a Silver...")
    
    # Ejecutar script de PySpark
    cmd = [
        "spark-submit",
        "--master", "spark://stack-completo:7077",
        "--deploy-mode", "client",
        "--driver-memory", "2g",
        "--executor-memory", "2g",
        "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.0.0",
        "/opt/airflow/dags/scripts/streaming_kafka_to_silver.py"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error en procesamiento:\n{result.stderr}")
        raise Exception(f"Spark job falló: {result.stderr}")
    
    print(f"✅ Procesamiento exitoso:\n{result.stdout}")
    return result.stdout


def update_silver_stats(**context):
    """
    Actualiza estadísticas de la tabla Silver para monitoreo
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("UpdateSilverStats") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    SILVER_PATH = "/app/notebooks/delta_lake/silver_contracts"
    
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    total_records = df_silver.count()
    latest_date = df_silver.agg({"fecha_firma": "max"}).collect()[0][0]
    
    print(f"📊 Estadísticas Silver:")
    print(f"   Total registros: {total_records:,}")
    print(f"   Última fecha: {latest_date}")
    
    # Guardar en XCom para siguiente tarea
    context['ti'].xcom_push(key='silver_total', value=total_records)
    context['ti'].xcom_push(key='silver_latest_date', value=str(latest_date))
    
    spark.stop()


# Definir DAG
with DAG(
    'streaming_kafka_to_silver',
    default_args=default_args,
    description='Procesa mensajes de Kafka a capa Silver continuamente',
    schedule_interval='*/5 * * * *',  # Cada 5 minutos
    catchup=False,
    tags=['streaming', 'kafka', 'silver', 'bronze-to-silver'],
) as dag:
    
    # Sensor: Verificar si hay mensajes en Kafka
    check_kafka = PythonSensor(
        task_id='check_kafka_sensor',
        python_callable=check_kafka_messages,
        mode='poke',
        timeout=300,
        poke_interval=30,
    )
    
    # Procesar Kafka → Silver
    process_silver = PythonOperator(
        task_id='process_kafka_to_silver',
        python_callable=process_kafka_to_silver,
    )
    
    # Actualizar estadísticas
    update_stats = PythonOperator(
        task_id='update_silver_stats',
        python_callable=update_silver_stats,
    )
    
    # Flujo
    check_kafka >> process_silver >> update_stats