"""
DAG para detección diaria de anomalías en contratos
Se ejecuta todos los días a las 6 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def check_new_contracts_in_silver(**context):
    """
    Verifica si hay contratos nuevos desde la última detección
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, max as spark_max
    from datetime import datetime, timedelta
    
    spark = SparkSession.builder \
        .appName("CheckNewContracts") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    SILVER_PATH = "/app/notebooks/delta_lake/silver_contracts"
    
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    # Obtener última fecha procesada (usar XCom o Variable de Airflow)
    from airflow.models import Variable
    
    try:
        last_processed_date = Variable.get("last_anomaly_detection_date")
        last_processed = datetime.strptime(last_processed_date, "%Y-%m-%d").date()
    except:
        # Primera ejecución: usar fecha de hace 1 día
        last_processed = (datetime.now() - timedelta(days=1)).date()
        print(f"⚠️ Primera ejecución, usando fecha: {last_processed}")
    
    # Contar contratos nuevos
    new_contracts = df_silver.filter(col("fecha_firma") > str(last_processed))
    new_count = new_contracts.count()
    
    print(f"\n📊 Verificación de contratos nuevos:")
    print(f"   Última detección: {last_processed}")
    print(f"   Contratos nuevos: {new_count:,}")
    
    if new_count == 0:
        print("⚠️ No hay contratos nuevos para analizar")
        spark.stop()
        return "skip_detection"
    
    # Guardar info para siguiente tarea
    context['ti'].xcom_push(key='new_contracts_count', value=new_count)
    context['ti'].xcom_push(key='last_processed_date', value=str(last_processed))
    
    spark.stop()
    return "run_detection"


def load_production_model(**context):
    """
    Carga el modelo en producción desde MLflow
    """
    import mlflow
    
    mlflow.set_tracking_uri("http://172.17.0.1:5000")
    
    print("📦 Cargando modelo en producción desde MLflow...")
    
    client = mlflow.tracking.MlflowClient()
    
    try:
        # Obtener modelo en producción
        prod_versions = client.get_latest_versions(
            "contract_value_predictor_rf_log_v1",
            stages=["Production"]
        )
        
        if not prod_versions:
            raise ValueError("❌ No hay modelo en producción")
        
        model_version = prod_versions[0].version
        run_id = prod_versions[0].run_id
        
        print(f"✅ Modelo encontrado:")
        print(f"   Versión: {model_version}")
        print(f"   Run ID: {run_id}")
        
        # Obtener sigma del modelo
        run = client.get_run(run_id)
        sigma_log = run.data.metrics.get('sigma_log_train')
        threshold_log = run.data.metrics.get('anomaly_threshold_log')
        
        if not sigma_log or not threshold_log:
            raise ValueError("❌ Modelo no tiene métricas de detección de anomalías")
        
        print(f"   σ (log): {sigma_log:.4f}")
        print(f"   Threshold 2.8σ: {threshold_log:.4f}")
        
        # Guardar en XCom
        context['ti'].xcom_push(key='model_version', value=model_version)
        context['ti'].xcom_push(key='model_run_id', value=run_id)
        context['ti'].xcom_push(key='sigma_log', value=sigma_log)
        context['ti'].xcom_push(key='threshold_log', value=threshold_log)
        
        return {
            'version': model_version,
            'run_id': run_id,
            'sigma_log': sigma_log,
            'threshold_log': threshold_log
        }
        
    except Exception as e:
        print(f"❌ Error cargando modelo: {str(e)}")
        raise


def run_anomaly_detection(**context):
    """
    Ejecuta detección de anomalías en contratos nuevos
    """
    # Obtener parámetros
    new_count = context['ti'].xcom_pull(key='new_contracts_count', task_ids='check_new_contracts')
    model_info = context['ti'].xcom_pull(task_ids='load_production_model')
    
    print(f"\n🔍 Detectando anomalías en {new_count:,} contratos...")
    print(f"   Modelo: v{model_info['version']}")
    
    # Ejecutar script de detección
    cmd = [
        "spark-submit",
        "--master", "spark://stack-completo:7077",
        "--deploy-mode", "client",
        "--driver-memory", "3g",
        "--executor-memory", "3g",
        "--packages", "io.delta:delta-spark_2.12:3.0.0",
        "--conf", f"spark.mlflow.model_run_id={model_info['run_id']}",
        "--conf", f"spark.anomaly.sigma_log={model_info['sigma_log']}",
        "--conf", f"spark.anomaly.threshold_log={model_info['threshold_log']}",
        "/opt/airflow/dags/scripts/detect_anomalies.py"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"❌ Detección falló:\n{result.stderr}")
    
    print("✅ Detección completada")
    
    # Extraer métricas del resultado
    import re
    anomalies_match = re.search(r'Anomalías detectadas: (\d+)', result.stdout)
    if anomalies_match:
        anomalies_count = int(anomalies_match.group(1))
        context['ti'].xcom_push(key='anomalies_detected', value=anomalies_count)
        print(f"   Anomalías encontradas: {anomalies_count}")
    
    return result.stdout


def analyze_anomalies(**context):
    """
    Analiza las anomalías detectadas y genera alertas
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, desc
    
    spark = SparkSession.builder \
        .appName("AnalyzeAnomalies") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    GOLD_PATH = "/tmp/gold_anomalies"
    
    df_anomalies = spark.read.format("delta").load(GOLD_PATH)
    
    # Filtrar solo anomalías
    df_atipicos = df_anomalies.filter(col("anomaly_flag") == "ATIPICO")
    
    total_anomalies = df_atipicos.count()
    
    if total_anomalies == 0:
        print("✅ No se detectaron anomalías")
        spark.stop()
        return
    
    print(f"\n⚠️ ANÁLISIS DE ANOMALÍAS:")
    print(f"   Total anomalías: {total_anomalies}")
    
    # Top 10 anomalías más severas
    print("\n🔴 Top 10 anomalías más severas:")
    df_atipicos.select(
        "valor_contrato",
        "valor_predicho_log",
        "z_score",
        "severity"
    ).orderBy(desc("z_score")).show(10, truncate=False)
    
    # Distribución por severidad
    print("\n📊 Distribución por severidad:")
    df_atipicos.groupBy("severity").count().orderBy(desc("count")).show()
    
    # Guardar alertas de alta severidad
    high_severity = df_atipicos.filter(col("severity").isin(["SEVERO", "MODERADO"]))
    high_count = high_severity.count()
    
    if high_count > 0:
        print(f"\n🚨 ALERTA: {high_count} anomalías de alta severidad detectadas")
        
        # Aquí podrías enviar alertas por email, Slack, etc.
        context['ti'].xcom_push(key='high_severity_count', value=high_count)
    
    spark.stop()
    
    return {
        'total_anomalies': total_anomalies,
        'high_severity': high_count
    }


def update_detection_date(**context):
    """
    Actualiza la fecha de última detección en Variables de Airflow
    """
    from airflow.models import Variable
    
    current_date = datetime.now().strftime("%Y-%m-%d")
    Variable.set("last_anomaly_detection_date", current_date)
    
    print(f"✅ Fecha de última detección actualizada: {current_date}")


def send_anomaly_report(**context):
    """
    Genera y envía reporte de anomalías
    """
    new_count = context['ti'].xcom_pull(key='new_contracts_count', task_ids='check_new_contracts')
    anomalies = context['ti'].xcom_pull(key='anomalies_detected', task_ids='run_anomaly_detection')
    analysis = context['ti'].xcom_pull(task_ids='analyze_anomalies')
    
    if not analysis:
        analysis = {'total_anomalies': 0, 'high_severity': 0}
    
    report = f"""
    ════════════════════════════════════════════════════════
    🔍 REPORTE DIARIO DE DETECCIÓN DE ANOMALÍAS
    ════════════════════════════════════════════════════════
    
    🗓️ Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    📊 Contratos analizados: {new_count:,}
    
    ⚠️ Anomalías detectadas: {anomalies or 0}
    
    🔴 Alta severidad: {analysis.get('high_severity', 0)}
    
    {'🚨 SE REQUIERE REVISIÓN URGENTE' if analysis.get('high_severity', 0) > 0 else '✅ No hay alertas críticas'}
    
    ════════════════════════════════════════════════════════
    """
    
    print(report)
    
    # Aquí podrías enviar por email/Slack
    return report


# Definir DAG
with DAG(
    'detect_anomalies_daily',
    default_args=default_args,
    description='Detecta anomalías en contratos diariamente',
    schedule_interval='0 6 * * *',  # Todos los días a las 6 AM
    catchup=False,
    tags=['anomaly-detection', 'daily', 'fraud-detection', 'gold-layer'],
) as dag:
    
    # Rama de decisión
    check_contracts = BranchPythonOperator(
        task_id='check_new_contracts',
        python_callable=check_new_contracts_in_silver,
    )
    
    # Si no hay contratos nuevos
    skip = DummyOperator(task_id='skip_detection')
    
    # Si hay contratos nuevos
    load_model = PythonOperator(
        task_id='load_production_model',
        python_callable=load_production_model,
    )
    
    detect = PythonOperator(
        task_id='run_anomaly_detection',
        python_callable=run_anomaly_detection,
    )
    
    analyze = PythonOperator(
        task_id='analyze_anomalies',
        python_callable=analyze_anomalies,
    )
    
    update_date = PythonOperator(
        task_id='update_detection_date',
        python_callable=update_detection_date,
        trigger_rule='none_failed',
    )
    
    report = PythonOperator(
        task_id='send_anomaly_report',
        python_callable=send_anomaly_report,
        trigger_rule='none_failed',
    )
    
    # Flujo
    check_contracts >> [skip, load_model]
    load_model >> detect >> analyze >> update_date >> report
    skip >> update_date