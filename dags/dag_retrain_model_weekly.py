"""
DAG para re-entrenamiento semanal del modelo predictivo
Se ejecuta los domingos a las 2 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import subprocess
import mlflow

default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def check_silver_data_quality(**context):
    """
    Verifica que hay suficientes datos nuevos en Silver para re-entrenar
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, max as spark_max, min as spark_min, count
    
    spark = SparkSession.builder \
        .appName("CheckSilverQuality") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    SILVER_PATH = "/app/notebooks/delta_lake/silver_contracts"
    
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    # Verificar calidad
    total = df_silver.count()
    stats = df_silver.select(
        spark_min("fecha_firma").alias("min_date"),
        spark_max("fecha_firma").alias("max_date"),
        count("valor_contrato").alias("valid_contracts")
    ).collect()[0]
    
    print(f"📊 Calidad de datos Silver:")
    print(f"   Total registros: {total:,}")
    print(f"   Rango fechas: {stats['min_date']} → {stats['max_date']}")
    print(f"   Contratos válidos: {stats['valid_contracts']:,}")
    
    # Validaciones
    if total < 10000:
        raise ValueError(f"❌ Datos insuficientes: {total} registros (mínimo 10,000)")
    
    if stats['valid_contracts'] < total * 0.95:
        raise ValueError(f"❌ Calidad insuficiente: solo {stats['valid_contracts']:,}/{total:,} válidos")
    
    print("✅ Datos Silver válidos para re-entrenamiento")
    
    context['ti'].xcom_push(key='silver_total', value=total)
    context['ti'].xcom_push(key='date_range', value=f"{stats['min_date']} → {stats['max_date']}")
    
    spark.stop()


def run_feature_engineering(**context):
    """
    Ejecuta pipeline de Feature Engineering (Fase 3)
    """
    print("🔧 Ejecutando Feature Engineering...")
    
    cmd = [
        "spark-submit",
        "--master", "spark://stack-completo:7077",
        "--deploy-mode", "client",
        "--driver-memory", "4g",
        "--executor-memory", "4g",
        "--packages", "io.delta:delta-spark_2.12:3.0.0",
        "/opt/airflow/dags/scripts/feature_engineering.py"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"❌ Feature Engineering falló:\n{result.stderr}")
    
    print(f"✅ Feature Engineering exitoso")
    return result.stdout


def train_new_model(**context):
    """
    Entrena nuevo modelo y lo registra en MLflow
    """
    print("🤖 Entrenando nuevo modelo...")
    
    cmd = [
        "spark-submit",
        "--master", "spark://stack-completo:7077",
        "--deploy-mode", "client",
        "--driver-memory", "4g",
        "--executor-memory", "4g",
        "--packages", "io.delta:delta-spark_2.12:3.0.0",
        "/opt/airflow/dags/scripts/train_model.py"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"❌ Entrenamiento falló:\n{result.stderr}")
    
    print(f"✅ Modelo entrenado y registrado en MLflow")
    
    # Extraer Run ID de los logs
    import re
    run_id_match = re.search(r'Run ID: ([a-f0-9]+)', result.stdout)
    if run_id_match:
        run_id = run_id_match.group(1)
        context['ti'].xcom_push(key='new_model_run_id', value=run_id)
        print(f"📊 Run ID: {run_id}")
    
    return result.stdout


def evaluate_and_promote_model(**context):
    """
    Compara el nuevo modelo con el actual y lo promueve si es mejor
    """
    mlflow.set_tracking_uri("http://172.17.0.1:5000")
    
    new_run_id = context['ti'].xcom_pull(key='new_model_run_id', task_ids='train_new_model')
    
    print(f"📊 Evaluando modelo: {new_run_id}")
    
    # Obtener métricas del nuevo modelo
    client = mlflow.tracking.MlflowClient()
    new_run = client.get_run(new_run_id)
    new_r2 = new_run.data.metrics.get('test_r2_log', 0)
    new_rmse = new_run.data.metrics.get('test_rmse_log', 999)
    
    print(f"   Nuevo R²: {new_r2:.4f}")
    print(f"   Nuevo RMSE: {new_rmse:.4f}")
    
    # Obtener modelo actual en producción
    try:
        prod_versions = client.get_latest_versions(
            "contract_value_predictor_rf_log_v1",
            stages=["Production"]
        )
        
        if prod_versions:
            prod_run_id = prod_versions[0].run_id
            prod_run = client.get_run(prod_run_id)
            prod_r2 = prod_run.data.metrics.get('test_r2_log', 0)
            prod_rmse = prod_run.data.metrics.get('test_rmse_log', 999)
            
            print(f"\n📊 Modelo en producción:")
            print(f"   R²: {prod_r2:.4f}")
            print(f"   RMSE: {prod_rmse:.4f}")
            
            # Comparar: promover si R² es mayor Y RMSE es menor
            if new_r2 > prod_r2 and new_rmse < prod_rmse:
                print("\n✅ Nuevo modelo es MEJOR → Promoviendo a Producción")
                
                # Archivar modelo anterior
                client.transition_model_version_stage(
                    name="contract_value_predictor_rf_log_v1",
                    version=prod_versions[0].version,
                    stage="Archived"
                )
                
                # Promover nuevo modelo
                new_version = client.search_model_versions(
                    f"run_id='{new_run_id}'"
                )[0].version
                
                client.transition_model_version_stage(
                    name="contract_value_predictor_rf_log_v1",
                    version=new_version,
                    stage="Production"
                )
                
                return "PROMOTED"
            else:
                print("\n⚠️ Nuevo modelo NO es mejor → Manteniendo modelo actual")
                return "NOT_PROMOTED"
        else:
            print("\n📊 No hay modelo en producción → Promoviendo nuevo modelo")
            
            new_version = client.search_model_versions(
                f"run_id='{new_run_id}'"
            )[0].version
            
            client.transition_model_version_stage(
                name="contract_value_predictor_rf_log_v1",
                version=new_version,
                stage="Production"
            )
            
            return "PROMOTED_FIRST"
            
    except Exception as e:
        print(f"⚠️ Error en evaluación: {str(e)}")
        raise


def send_training_report(**context):
    """
    Genera reporte del re-entrenamiento
    """
    silver_total = context['ti'].xcom_pull(key='silver_total', task_ids='check_silver_quality')
    date_range = context['ti'].xcom_pull(key='date_range', task_ids='check_silver_quality')
    promotion_status = context['ti'].xcom_pull(task_ids='evaluate_and_promote_model')
    
    report = f"""
    ════════════════════════════════════════════════════════
    📊 REPORTE DE RE-ENTRENAMIENTO SEMANAL
    ════════════════════════════════════════════════════════
    
    🗓️ Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    📊 Datos procesados:
       - Total registros: {silver_total:,}
       - Rango temporal: {date_range}
    
    🤖 Estado del modelo:
       - Promoción: {promotion_status}
    
    ✅ Re-entrenamiento completado exitosamente
    ════════════════════════════════════════════════════════
    """
    
    print(report)
    
    # Aquí podrías enviar el reporte por email o Slack
    return report


# Definir DAG
with DAG(
    'retrain_model_weekly',
    default_args=default_args,
    description='Re-entrena el modelo de predicción semanalmente',
    schedule_interval='0 2 * * 0',  # Domingos a las 2 AM
    catchup=False,
    tags=['training', 'mlflow', 'weekly', 'model-update'],
) as dag:
    
    # 1. Verificar calidad de datos
    check_quality = PythonOperator(
        task_id='check_silver_quality',
        python_callable=check_silver_data_quality,
    )
    
    # 2. Feature Engineering
    feature_eng = PythonOperator(
        task_id='run_feature_engineering',
        python_callable=run_feature_engineering,
    )
    
    # 3. Entrenar modelo
    train_model = PythonOperator(
        task_id='train_new_model',
        python_callable=train_new_model,
    )
    
    # 4. Evaluar y promover
    evaluate = PythonOperator(
        task_id='evaluate_and_promote_model',
        python_callable=evaluate_and_promote_model,
    )
    
    # 5. Enviar reporte
    report = PythonOperator(
        task_id='send_training_report',
        python_callable=send_training_report,
    )
    
    # Flujo
    check_quality >> feature_eng >> train_model >> evaluate >> report