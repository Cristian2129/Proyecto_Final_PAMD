# 1. Usar la imagen base de Python 3.11
FROM python:3.11-slim

# Variables de entorno para las versiones y rutas
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV JUPYTER_TOKEN="hola-mundo"

# 2. Instalar dependencias del sistema: Java y herramientas
# Se añadieron procps (para 'ps'), rsync (necesario para Spark) y dos2unix (para arreglar start.sh)
RUN apt-get update && apt-get install -y \
    openjdk-21-jre-headless \
    curl \
    tini \
    procps \
    rsync \
    dos2unix \
    && rm -rf /var/lib/apt/lists/*

# 3. Descargar y configurar Spark
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    --output spark.tgz \
    && mkdir -p ${SPARK_HOME} \
    && tar -xzf spark.tgz -C ${SPARK_HOME} --strip-components=1 \
    && rm spark.tgz

# 4. Instalar las librerías de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Preparar el directorio de trabajo y el script de inicio
WORKDIR /app
COPY start.sh .
# 🚨 SOLUCIÓN LÍNEAS CRLF: Convertir el script a formato Unix (LF) antes de hacerlo ejecutable.
# Esto previene el error "$'\r': command not found".
RUN dos2unix start.sh
RUN chmod +x start.sh

# 6. Exponer los puertos de las UIs
EXPOSE 8888 8080 7077 5000

# Usar Tini como entrypoint para gestionar procesos correctamente
ENTRYPOINT ["/usr/bin/tini", "--"]

# Comando por defecto para iniciar los servicios
CMD ["/app/start.sh"]
