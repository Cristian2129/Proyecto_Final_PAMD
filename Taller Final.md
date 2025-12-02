# **Detección de Anomalías en Contratación Pública (Pipeline MLOps)**

**Objetivo:** Construir una arquitectura de datos *End-to-End* que ingeste datos de contratos en tiempo real, los procese, entrene un modelo de predicción de precios basado en la descripción del contrato (Embeddings) y detecte sobrecostos atípicos (anomalías) según la regla de negocio definida.

Duración estimada: 8 \- 12 horas.

Herramientas: Apache Spark (PySpark), Delta Lake, Apache Kafka, Apache Airflow, Jupyter Lab.

---

## **Fase 0: Arquitectura y Despliegue**

El estudiante debe levantar un entorno utilizando docker-compose que contenga:

1. **Zookeeper & Kafka:** Para la ingesta de mensajes.  
2. **Spark Master & Worker:** Con soporte para Delta Lake y librerías de ML.  
3. **Jupyter Lab:** Conectado al clúster de Spark.  
4. **Airflow Webserver & Scheduler:** Para orquestar el re-entrenamiento y procesamiento batch.

---

## **Fase 1: Ingesta de Datos (Kafka y Simulación)**

Según el tablero, existen 83 variables posibles, pero nos enfocaremos en las críticas. El estudiante debe crear un script en Python (contract\_producer.py) que simule la llegada de contratos al tópico contratos-publicos.

**Requerimiento del Tablero:**

* **Dataset Base:**   
  * Regiones Base  
  * 

| Región Turística  Macrorregión | Departamentos Incluidos (32 en total) |
| :---- | :---- |
| **Eje Cafetero** | Antioquia (suroeste), Caldas, Quindío, Risaralda, Tolima (noroccidente), Valle del Cauca (norte/oriente) |
| **Gran Caribe** | Atlántico, Bolívar, Cesar, Córdoba, La Guajira, Magdalena, Sucre, Archipiélago de San Andrés, Providencia y Santa Catalina |
| **Pacífico** | Cauca, Chocó, Nariño, Valle del Cauca (excepto la zona norte/oriente) |
| **Orinoquía** | Arauca, Casanare, Meta, Vichada |
| **Amazonía** | Amazonas, Caquetá, Guainía, Guaviare, Putumayo, Vaupés |
| **Centro-Oriente** | Boyacá, Cundinamarca, Huila, Norte de Santander, Santander |

  *   
* **Datos en Streaming (Kafka):** El script debe generar JSONs con:  
  * id\_contrato  
  * objeto\_contrato (Texto largo, ej: "Suministro de papelería para la sede norte...")  
  * entidad (Nombre de la entidad pública)  
  * codigo\_unspsc (Categoría)  
  * duracion\_dias (Numérico)  
  * valor\_contrato (Target \- Numérico)  
  * fecha\_firma  
  * *Variables ruidosas adicionales* (para cumplir con el requisito de "Sacar variables redundantes").

---

## **Fase 2: Procesamiento y Limpieza (Spark \+ Delta Lake)**

El estudiante debe usar **Jupyter Lab** para desarrollar un Job de Spark Streaming que lea de Kafka.

**Tareas (Capa Bronze a Silver):**

1. **Ingesta:** Leer el stream de Kafka.  
2. **Explosión de Metadatos:** Extraer metadatos de la metadata de Kafka si es necesario (timestamp, offset).  
3. **Limpieza (Según tablero):**  
   * **Eliminación de Redundantes:** Identificar y eliminar columnas que no aporten información (ej: columnas con varianza 0 o IDs internos del sistema fuente).  
   * **Cruce con Regiones:** Unir el stream con el archivo regiones.csv entregado por el profesor (Broadcasting Join).  
4. **Persistencia:** Guardar los datos limpios en formato **Delta Lake** (Tabla: silver\_contracts).

---

## **Fase 3: Feature Engineering y Embeddings (La lógica del tablero)**

Esta es la sección analítica crítica. Se debe crear un pipeline de transformación en Spark ML.

**Requerimientos Específicos:**

1. **Texto a Vector (Embeddings):**  
   * Tomar la columna objeto\_contrato.  
   * Aplicar limpieza de texto (Stopwords, tokenización).  
   * Generar Embeddings. *Opción A (Básica):* Word2Vec de Spark ML. *Opción B (Avanzada):* Usar un modelo Transformer (BERT) mediante Spark NLP o UDFs de PyTorch, reduciendo el texto a un vector denso.  
2. **Variables Categóricas:** Indexar y codificar entidad y codigo\_unspsc (StringIndexer \+ OneHotEncoder).  
3. **Selección de Variables:**  
   * Analizar correlaciones (Pearson/Spearman) frente a valor\_contrato.  
   * Filtrar variables con baja correlación.  
4. **Reducción de Dimensionalidad:**  
   * Como indica el tablero, aplicar **PCA** (Principal Component Analysis) sobre los vectores resultantes de los embeddings y las variables numéricas para reducir la complejidad del modelo.

---

## **Fase 4: Modelado Predictivo (Regresión)**

El objetivo es predecir cuánto *debería* costar el contrato según sus características.

1. **Split:** Separar datos en Train/Test.  
2. **Modelo:** Entrenar un regresor (ej: RandomForestRegressor o GBTRegressor) para predecir valor\_contrato.  
   * *Features:* Vector resultante del PCA (Embeddings \+ Numéricas \+ Categóricas).  
   * *Target:* valor\_contrato.  
3. **Evaluación:** Calcular RMSE y R2.  
4. **Persistencia del Modelo:** Guardar el pipeline entrenado en MLflow o en disco para su uso posterior.

---

## **Fase 5: Detección de Atípicos (Regla de Negocio)**

Implementar la lógica final del tablero para detectar corrupción o sobrecostos.

Lógica a implementar:

$$Desviación \= ValorReal \- ValorPredicho$$  
**Regla del Tablero:**

"Contratos con una desviación por encima del predicho \+ 2.8"

El estudiante debe calcular la desviación estándar ($\\sigma$) de los residuos (errores) del modelo en el set de entrenamiento. La regla de detección será:

* Si $ValorReal \> (ValorPredicho \+ 2.8 \\times \\sigma)$: **Marcar como ATÍPICO (Posible sobrecosto).**  
* Si no: **LIBRE (Normal).**

El resultado final debe escribirse en una tabla Delta llamada gold\_anomalies.

---

## **Fase 6: Orquestación (Airflow)**

Todo lo anterior no debe ser manual. El estudiante debe crear un DAG en Airflow:

1. **Sensor:** Verificar que existan nuevos datos en la tabla Silver.  
2. **Task 1 (Retraining \- Semanal):** Ejecutar el script de Spark que re-entrena el modelo y recalcula el PCA y los Embeddings.  
3. **Task 2 (Inferencia \- Diaria):** Ejecutar el script que toma los nuevos contratos, aplica el modelo guardado, calcula la regla del "2.8" y guarda las alertas en la tabla Gold.

---

## **Entregables del Estudiante**

1. **Repositorio Git:** Con los DAGs de Airflow y scripts de PySpark.  
2. **Notebook de Análisis:** Mostrando la exploración de datos, la correlación y la justificación de la reducción de dimensionalidad.  
3. **Video/Demo:** Mostrando:  
   * El script productor enviando datos a Kafka.  
   * Airflow disparando el proceso.  
   * Una consulta SQL final sobre la tabla Delta gold\_anomalies mostrando un contrato detectado como "Atípico" por exceso de valor.

### 

### 

### 

### 

### **Rúbrica de Evaluación Sugerida**

| Criterio | Peso | Descripción |
| :---- | :---- | :---- |
| **Infraestructura** | 20% | Docker-compose funcional con Kafka, Spark, Airflow. |
| **Ingeniería de Datos** | 30% | Uso correcto de Delta Lake, Kafka Streaming y limpieza de datos. |
| **Machine Learning** | 30% | Correcta implementación de **Embeddings** para el texto y **PCA** (según tablero). Modelo predictivo funcional. |
| **Lógica de Negocio** | 20% | Aplicación correcta de la fórmula de detección (Predicción \+ 2.8 desviaciones). |

