"""
Script PySpark MEJORADO: Procesa Kafka con manejo de offsets y deduplicación
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# ============================================================
# 1. INICIALIZAR SPARK
# ============================================================
spark = (
    SparkSession.builder
    .appName("Kafka_to_Silver_Incremental")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 2. GESTIÓN DE OFFSETS (para no reprocesar)
# ============================================================
OFFSET_PATH = "/app/notebooks/kafka_offsets.json"
DELTA_PATH = "/app/notebooks/delta_lake/silver_contracts"

def load_last_offset():
    """Carga el último offset procesado"""
    import json
    import os
    
    if os.path.exists(OFFSET_PATH):
        with open(OFFSET_PATH, 'r') as f:
            state = json.load(f)
            return state.get('last_offset', 'earliest')
    return 'earliest'

def save_last_offset(end_offsets):
    """Guarda el último offset procesado"""
    import json
    
    state = {
        'last_offset': end_offsets,
        'timestamp': str(datetime.now())
    }
    
    with open(OFFSET_PATH, 'w') as f:
        json.dump(state, f, indent=2)
    
    print(f"✅ Offset guardado: {end_offsets}")

# ============================================================
# 3. SCHEMA DE CONTRATOS
# ============================================================
contract_schema = StructType([
    StructField("id_contrato", StringType()),
    StructField("objeto_contrato", StringType()),
    StructField("entidad", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("region", StringType()),
    StructField("codigo_unspsc", StringType()),
    StructField("descripcion_categoria", StringType()),
    StructField("valor_contrato", DoubleType()),
    StructField("duracion_dias", IntegerType()),
    StructField("fecha_firma", StringType()),
    StructField("tipo_contrato", StringType()),
    StructField("estado_contrato", StringType()),
    StructField("modalidad", StringType()),
    StructField("anno", IntegerType()),
    StructField("id_interno_sistema", StringType()),
    StructField("campo_vacio", StringType()),
    StructField("constante_1", StringType()),
    StructField("constante_2", IntegerType()),
    StructField("duplicate_id", StringType()),
    StructField("timestamp_carga", StringType())
])

# ============================================================
# 4. LEER DE KAFKA (solo mensajes NUEVOS)
# ============================================================
print("📊 Leyendo mensajes NUEVOS de Kafka...")

last_offset = load_last_offset()
print(f"   Último offset procesado: {last_offset}")

# CAMBIO CRÍTICO: Usar latest en lugar de earliest después de primera ejecución
if last_offset == 'earliest':
    print("   ⚠️ Primera ejecución - procesando todos los mensajes")
    starting_offset = "earliest"
else:
    print("   ✅ Procesando solo mensajes nuevos")
    starting_offset = "latest"

df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "contratos-publicos") \
    .option("startingOffsets", starting_offset) \
    .load()

# Parsear JSON
df_bronze = df_kafka.select(
    from_json(col("value").cast("string"), contract_schema).alias("data"),
    col("offset"),
    col("partition"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "offset", "partition", "kafka_timestamp")

total_kafka = df_bronze.count()

if total_kafka == 0:
    print("✅ No hay mensajes nuevos")
    spark.stop()
    exit(0)

print(f"✅ Mensajes nuevos: {total_kafka:,}")

# ============================================================
# 5. LIMPIEZA Y PREPARACIÓN
# ============================================================
print("\n🧹 Limpiando datos...")

# Eliminar columnas redundantes
redundant_columns = [
    "id_interno_sistema",
    "campo_vacio",
    "constante_1",
    "constante_2",
    "duplicate_id",
    "timestamp_carga"
]

df_cleaned = df_bronze.drop(*redundant_columns)

# Convertir fecha
df_cleaned = (
    df_cleaned
    .withColumn("fecha_firma_temp", to_timestamp(col("fecha_firma")))
    .withColumn("fecha_firma", to_date(col("fecha_firma_temp")))
    .drop("fecha_firma_temp")
)

# Aplicar filtros de calidad
df_silver_new = df_cleaned \
    .filter(col("id_contrato").isNotNull()) \
    .filter(col("objeto_contrato").isNotNull()) \
    .filter(col("valor_contrato").isNotNull()) \
    .filter(col("valor_contrato") > 0) \
    .filter(col("fecha_firma").isNotNull())

total_valid = df_silver_new.count()
print(f"✅ Registros válidos: {total_valid:,}")

if total_valid == 0:
    print("⚠️ No hay registros válidos después de limpieza")
    spark.stop()
    exit(0)

# ============================================================
# 6. GUARDAR EN DELTA CON DEDUPLICACIÓN (MERGE)
# ============================================================
print("\n💾 Guardando en Delta Lake con deduplicación...")

# Verificar si la tabla ya existe
try:
    delta_table = DeltaTable.forPath(spark, DELTA_PATH)
    table_exists = True
    print("   Tabla Silver existe - usando MERGE")
except:
    table_exists = False
    print("   Primera carga - creando tabla")

if table_exists:
    # MERGE: Actualizar existentes e insertar nuevos
    delta_table.alias("target").merge(
        df_silver_new.alias("source"),
        "target.id_contrato = source.id_contrato"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    
    print("✅ MERGE completado (deduplicación aplicada)")
else:
    # Primera carga: crear tabla
    df_silver_new.write \
        .format("delta") \
        .mode("overwrite") \
        .save(DELTA_PATH)
    
    print("✅ Tabla Delta creada")

# ============================================================
# 7. GUARDAR OFFSET (para próxima ejecución)
# ============================================================
max_offset = df_bronze.agg(max("offset")).collect()[0][0]
save_last_offset(str(max_offset))

# ============================================================
# 8. ESTADÍSTICAS FINALES
# ============================================================
print("\n" + "="*70)
print("📊 RESUMEN DE PROCESAMIENTO")
print("="*70)
print(f"Mensajes Kafka procesados: {total_kafka:,}")
print(f"Registros válidos insertados/actualizados: {total_valid:,}")
print(f"Último offset procesado: {max_offset}")

# Estadísticas de la tabla completa
df_silver_total = spark.read.format("delta").load(DELTA_PATH)
total_records = df_silver_total.count()
latest_date = df_silver_total.agg({"fecha_firma": "max"}).collect()[0][0]

print(f"\n📊 Tabla Silver (total):")
print(f"   Total registros: {total_records:,}")
print(f"   Última fecha: {latest_date}")
print("="*70)

spark.stop()