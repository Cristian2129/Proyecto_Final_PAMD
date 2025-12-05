#!/bin/bash

# Iniciar Spark Master en segundo plano
echo "Iniciando Spark Master..."
${SPARK_HOME}/sbin/start-master.sh --host 0.0.0.0

# Iniciar Spark Worker en segundo plano y conectarlo al Master
echo "Iniciando Spark Worker..."
${SPARK_HOME}/sbin/start-worker.sh spark://0.0.0.0:7077

# Iniciar MLflow Server en segundo plano
echo "Iniciando MLflow Server..."
mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri postgresql://admin_user:admin_pass@postgres-db:5432/mlflow_db \
    --default-artifact-root file:///app/mlflow_artifacts &

# Iniciar JupyterLab como el proceso principal (en primer plano)
echo "Iniciando JupyterLab..."
exec jupyter lab \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    --allow-root \
    --NotebookApp.token=${JUPYTER_TOKEN} \
    --notebook-dir=/app/notebooks