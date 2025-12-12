"""
Script PySpark: Entrena modelo de predicción y lo registra en MLflow
Combina Fase 3 (Feature Engineering) + Fase 4 (Modelado)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.spark

# ============================================================
# 1. INICIALIZAR SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("TrainModel") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 2. CARGAR DATOS DESDE SILVER
# ============================================================
print("📊 Cargando datos desde Silver...")
SILVER_PATH = "/app/notebooks/delta_lake/silver_contracts"
df_silver = spark.read.format("delta").load(SILVER_PATH)

print(f"✅ Registros cargados: {df_silver.count():,}")

# ============================================================
# 3. FEATURE ENGINEERING (FASE 3)
# ============================================================
print("\n🔧 FEATURE ENGINEERING...")

# 3.1 Limpieza de texto
print("   Limpiando texto...")
src_chars = "áéíóúüñ"
dst_chars = "aeiouun"

df_prepared = df_silver.withColumn(
    "objeto_limpio",
    trim(
        regexp_replace(
            regexp_replace(
                translate(lower(col("objeto_contrato")), src_chars, dst_chars),
                "[^a-z0-9\\s]", " "
            ),
            "\\s+", " "
        )
    )
).filter(length(col("objeto_limpio")) >= 10)

# 3.2 Tokenización
print("   Tokenizando...")
tokenizer = Tokenizer(inputCol="objeto_limpio", outputCol="palabras")
df_tokenized = tokenizer.transform(df_prepared)

stopwords_es = [
    "el", "la", "de", "que", "y", "a", "en", "un", "ser", "se", "no",
    "por", "con", "su", "para", "como", "estar", "contrato", "servicio"
]

remover = StopWordsRemover(
    inputCol="palabras",
    outputCol="palabras_filtradas",
    stopWords=stopwords_es
)
df_filtered = remover.transform(df_tokenized)

# 3.3 Word2Vec Embeddings
print("   Generando embeddings Word2Vec...")
word2vec = Word2Vec(
    vectorSize=100,
    minCount=2,
    maxIter=10,
    seed=42,
    inputCol="palabras_filtradas",
    outputCol="embedding_raw"
)
word2vec_model = word2vec.fit(df_filtered)
df_embeddings = word2vec_model.transform(df_filtered)

# 3.4 Variables categóricas
print("   Codificando variables categóricas...")
low_card_cols = ["tipo_contrato", "estado_contrato", "modalidad"]

for col_name in low_card_cols:
    indexer = StringIndexer(
        inputCol=col_name,
        outputCol=f"{col_name}_idx",
        handleInvalid="keep"
    )
    df_embeddings = indexer.fit(df_embeddings).transform(df_embeddings)
    
    encoder = OneHotEncoder(
        inputCol=f"{col_name}_idx",
        outputCol=f"{col_name}_ohe",
        dropLast=True
    )
    df_embeddings = encoder.fit(df_embeddings).transform(df_embeddings)

# 3.5 Frequency Encoding para entidad
print("   Frequency encoding para entidad...")
entidad_freq = df_embeddings.groupBy("entidad").count()
total_count = df_embeddings.count()
entidad_freq = entidad_freq.withColumn(
    "entidad_freq",
    col("count") / total_count
).select("entidad", "entidad_freq")

df_embeddings = df_embeddings.join(entidad_freq, "entidad", "left")

# ============================================================
# 4. TRANSFORMACIÓN LOGARÍTMICA DEL TARGET
# ============================================================
print("\n📊 Transformando target a escala logarítmica...")
df_embeddings = df_embeddings.withColumn(
    "log_valor_contrato",
    log1p(col("valor_contrato"))
)

# ============================================================
# 5. DIVISIÓN TEMPORAL (80/20)
# ============================================================
print("\n✂️ División temporal train/test...")
from datetime import datetime

df_temp = df_embeddings.withColumn(
    "fecha_num",
    col("fecha_firma").cast("timestamp").cast("long")
)

q = df_temp.approxQuantile("fecha_num", [0.8], 0.01)
split_ts = q[0]
split_date = datetime.utcfromtimestamp(split_ts)

print(f"   Fecha de corte: {split_date}")

train_data = df_embeddings.filter(col("fecha_firma") <= split_date)
test_data = df_embeddings.filter(col("fecha_firma") > split_date)

print(f"   Train: {train_data.count():,}")
print(f"   Test: {test_data.count():,}")

# ============================================================
# 6. TARGET ENCODING (en escala log)
# ============================================================
print("\n🎯 Target encoding para codigo_unspsc...")

global_mean_log = train_data.agg(mean("log_valor_contrato")).first()[0]
m = 50

stats = train_data.groupBy("codigo_unspsc").agg(
    mean("log_valor_contrato").alias("cat_mean_log"),
    count("log_valor_contrato").alias("cat_count")
)

stats = stats.withColumn(
    "codigo_unspsc_te_log",
    (col("cat_count") * col("cat_mean_log") + m * global_mean_log) / (col("cat_count") + m)
).select("codigo_unspsc", "codigo_unspsc_te_log")

train_data = train_data.join(stats, "codigo_unspsc", "left")
test_data = test_data.join(stats, "codigo_unspsc", "left")
test_data = test_data.fillna({"codigo_unspsc_te_log": global_mean_log})

# ============================================================
# 7. ENSAMBLAR FEATURES
# ============================================================
print("\n🔧 Ensamblando features...")

train_data = train_data.fillna({"duracion_dias": 0})
test_data = test_data.fillna({"duracion_dias": 0})

feature_cols = [
    "embedding_raw",
    "tipo_contrato_ohe",
    "estado_contrato_ohe",
    "modalidad_ohe",
    "entidad_freq",
    "codigo_unspsc_te_log",
    "duracion_dias"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features_raw",
    handleInvalid="skip"
)

train_features = assembler.transform(train_data)
test_features = assembler.transform(test_data)

# ============================================================
# 8. NORMALIZACIÓN
# ============================================================
print("\n📏 Normalizando features...")

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

scaler_model = scaler.fit(train_features)
train_final = scaler_model.transform(train_features).select(
    col("log_valor_contrato").alias("label"),
    "features",
    "valor_contrato"
).cache()

test_final = scaler_model.transform(test_features).select(
    col("log_valor_contrato").alias("label"),
    "features",
    "valor_contrato"
).cache()

print(f"   Dimensiones: {len(train_final.select('features').first()[0])}")

# ============================================================
# 9. ENTRENAMIENTO DEL MODELO
# ============================================================
print("\n🤖 Entrenando Random Forest...")

rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="label",
    numTrees=30,
    maxDepth=8,
    maxBins=32,
    subsamplingRate=0.8,
    seed=42
)

rf_model = rf.fit(train_final)
print("   ✅ Modelo entrenado")

# ============================================================
# 10. CALCULAR SIGMA EN TRAIN (ESCALA LOG)
# ============================================================
print("\n📊 Calculando σ en train set...")

train_predictions = rf_model.transform(train_final)
train_with_residuals = train_predictions.withColumn(
    "residual_log",
    col("label") - col("prediction")
)

sigma_log = train_with_residuals.agg(
    stddev("residual_log").alias("sigma")
).first()["sigma"]

threshold_log = 2.8 * sigma_log

print(f"   σ (log): {sigma_log:.4f}")
print(f"   Threshold 2.8σ: {threshold_log:.4f}")

# ============================================================
# 11. PREDICCIONES EN TEST
# ============================================================
print("\n🔮 Generando predicciones en test...")

test_predictions = rf_model.transform(test_final)

# ============================================================
# 12. MÉTRICAS
# ============================================================
print("\n📊 Calculando métricas...")

evaluator_r2 = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="r2"
)
evaluator_rmse = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)
evaluator_mae = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="mae"
)

r2 = evaluator_r2.evaluate(test_predictions)
rmse = evaluator_rmse.evaluate(test_predictions)
mae = evaluator_mae.evaluate(test_predictions)

print(f"\n📊 MÉTRICAS DEL MODELO:")
print(f"   R²: {r2:.4f}")
print(f"   RMSE: {rmse:.4f}")
print(f"   MAE: {mae:.4f}")

# ============================================================
# 13. REGISTRO EN MLFLOW
# ============================================================
print("\n📦 Registrando en MLflow...")

mlflow.set_tracking_uri("http://172.17.0.1:5000")
mlflow.set_experiment("contract_value_regression_log")

with mlflow.start_run(run_name=f"rf_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
    # Parámetros
    mlflow.log_param("model_type", "RandomForestRegressor")
    mlflow.log_param("target_transform", "log1p")
    mlflow.log_param("numTrees", 30)
    mlflow.log_param("maxDepth", 8)
    mlflow.log_param("train_size", train_final.count())
    mlflow.log_param("test_size", test_final.count())
    
    # Métricas
    mlflow.log_metric("test_r2_log", r2)
    mlflow.log_metric("test_rmse_log", rmse)
    mlflow.log_metric("test_mae_log", mae)
    mlflow.log_metric("sigma_log_train", sigma_log)
    mlflow.log_metric("anomaly_threshold_log", threshold_log)
    
    # Modelo
    mlflow.spark.log_model(
        spark_model=rf_model,
        artifact_path="model_log",
        registered_model_name="contract_value_predictor_rf_log_v1"
    )
    
    # Tags
    mlflow.set_tag("framework", "PySpark")
    mlflow.set_tag("algorithm", "RandomForest")
    mlflow.set_tag("anomaly_detection_method", "sigma_log_train")
    
    run_id = mlflow.active_run().info.run_id
    print(f"\n✅ Modelo registrado - Run ID: {run_id}")

print("\n✅ Entrenamiento completado exitosamente")

spark.stop()