-- Crear base de datos para MLflow
CREATE DATABASE mlflow_db;

-- Crear base de datos para Airflow
CREATE DATABASE airflow_db;

-- Otorgar permisos al usuario admin_user en ambas bases de datos
GRANT ALL PRIVILEGES ON DATABASE mlflow_db TO admin_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO admin_user;