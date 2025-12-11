"""
Script productor de contratos públicos SECOP II para Kafka
Descarga datos reales de un departamento específico y los carga incrementalmente día por día
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import time
from sodapy import Socrata
import sys
import re

DEPARTAMENTOS_DISPONIBLES = [
    "Bogotá D.C.",
    "Antioquia",
    "Valle del Cauca",
    "Cundinamarca",
    "Santander",
    "Atlántico",
    "Bolívar",
]

DEPARTAMENTO_REGION = {
    "Distrito Capital de Bogotá": "Centro-Oriente",
    "Antioquia": "Eje Cafetero",
    "Valle del Cauca": "Pacífico",
    "Cundinamarca": "Centro-Oriente",
    "Santander": "Centro-Oriente",
    "Atlántico": "Gran Caribe",
    "Bolívar": "Gran Caribe",
    "Caldas": "Eje Cafetero",
    "Risaralda": "Eje Cafetero",
    "Quindío": "Eje Cafetero",
    "Tolima": "Eje Cafetero",
    "Boyacá": "Centro-Oriente",
    "Huila": "Centro-Oriente",
    "Norte de Santander": "Centro-Oriente",
    "Cesar": "Gran Caribe",
    "Córdoba": "Gran Caribe",
    "La Guajira": "Gran Caribe",
    "Magdalena": "Gran Caribe",
    "Sucre": "Gran Caribe",
    "Cauca": "Pacífico",
    "Chocó": "Pacífico",
    "Nariño": "Pacífico",
}

def parse_duration(value):
        """Función auxiliar para parsear duración"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return int(value)

        value = str(value).strip()

        # Si viene vacío, texto, "No Definido", "No D", etc.
        if value == "" or value.lower() in ["no d", "no definido", "nd", "sin definir"]:
            return None

        # Extraer cualquier número dentro del texto
        match = re.search(r'\d+', value)
        if match:
            return int(match.group(0))

        # Si no hay ningún número → return None
        return None
def parse_year(value, default=2024):
    """Función auxiliar para parsear año"""
    if value is None:
        return default
    
    if isinstance(value, (int, float)):
        return int(value)
    
    value = str(value).strip()
    
    # Si viene vacío o "No D"
    if value == "" or value.lower() in ["no d", "no definido", "nd"]:
        return default
    
    # Extraer año (4 dígitos)
    match = re.search(r'\d{4}', value)
    if match:
        return int(match.group(0))
    
    # Cualquier número
    match = re.search(r'\d+', value)
    if match:
        year = int(match.group(0))
        # Validar que sea un año razonable
        if 2000 <= year <= 2030:
            return year
    
    return default


def parse_duration(value):
    """Función auxiliar para parsear duración"""
    if value is None:
        return None
    
    if isinstance(value, (int, float)):
        return int(value)

    value = str(value).strip()

    if value == "" or value.lower() in ["no d", "no definido", "nd", "sin definir"]:
        return None

    match = re.search(r'\d+', value)
    if match:
        return int(match.group(0))

    return None


def parse_year(value, default=2024):
    """Función auxiliar para parsear año"""
    if value is None:
        return default
    
    if isinstance(value, (int, float)):
        return int(value)
    
    value = str(value).strip()

    if value == "" or value.lower() in ["no d", "no definido", "nd"]:
        return default

    match = re.search(r'\d{4}', value)
    if match:
        return int(match.group(0))

    match = re.search(r'\d+', value)
    if match:
        year = int(match.group(0))
        if 2000 <= year <= 2030:
            return year

    return default


class SECOPContractProducer:
    def __init__(self, bootstrap_servers='kafka:29092', topic='contratos-publicos'):
        """Inicializa el productor de Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            max_request_size=10485760,
            api_version=(0, 10, 1),
            connections_max_idle_ms=540000,
            request_timeout_ms=30000,
            metadata_max_age_ms=300000,
        )
        self.topic = topic
        self.contracts_df = None
        self.departamento = None

    def check_department_data(self, departamento):
        print("\n======================================================================")
        print("VERIFICANDO DISPONIBILIDAD DE DATOS")
        print("======================================================================")

        try:
            client = Socrata("www.datos.gov.co", None)
            results = client.get(
                "jbjy-vk9h",
                where=(
                    f"departamento='{departamento}' AND "
                    f"fecha_de_firma >= '2024-01-01T00:00:00' AND "
                    f"fecha_de_firma < '2025-01-01T00:00:00'"
                ),
                select="COUNT(*) as total"
            )

            if results:
                total = int(results[0]["total"])
                print(f"Contratos encontrados: {total:,}")
                return total

            return 0

        except Exception as e:
            print(f"No se pudo verificar el volumen: {str(e)}")
            return -1

    def download_secop_data(self, departamento, year=2024):
        self.departamento = departamento

        print("\n======================================================================")
        print("DESCARGANDO DATOS DE SECOP II")
        print("======================================================================")

        total_available = self.check_department_data(departamento)

        try:
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"

            where_clause = (
                f"departamento='{departamento}' AND "
                f"fecha_de_firma >= '{year}-01-01T00:00:00' AND "
                f"fecha_de_firma < '{year+1}-01-01T00:00:00'"
            )

            offset = 0
            batch_size = 10000
            all_results = []

            while True:
                batch_results = client.get(
                    dataset_id,
                    where=where_clause,
                    limit=batch_size,
                    offset=offset,
                    order="fecha_de_firma ASC"
                )

                if not batch_results:
                    break

                all_results.extend(batch_results)
                offset += batch_size
                time.sleep(2)

            self.contracts_df = pd.DataFrame.from_records(all_results)

            if len(self.contracts_df) == 0:
                print("No se encontraron contratos.")
                return False

            self.contracts_df["fecha_de_firma"] = pd.to_datetime(
                self.contracts_df['fecha_de_firma']
            ).dt.date

            return True

        except Exception as e:
            print(f"Error descargando datos: {str(e)}")
            return False

<<<<<<< HEAD

    def prepare_contract_message(self, row):
            """
            Prepara el mensaje del contrato para enviar a Kafka
            """
            try:
                contract = {
                    "id_contrato": str(row.get('referencia_del_contrato', row.get('uid', f"CT-{row.name}"))),
                    "objeto_contrato": str(row.get('descripcion_del_procedimiento', row.get('objeto_del_contrato', ''))),
                    "entidad": str(row.get('nombre_entidad', '')),
                    "departamento": self.departamento,
                    "region": DEPARTAMENTO_REGION.get(self.departamento, 'Desconocida'),
                    "codigo_unspsc": str(row.get('codigo_de_categoria_principal', '')),
                    "descripcion_categoria": str(row.get('descripcion_del_proceso', '')),
                    "valor_contrato": float(row.get('valor_del_contrato', 0)) if pd.notna(row.get('valor_del_contrato')) else 0.0,
                    "duracion_dias": parse_duration(row.get('duraci_n_del_contrato')),  # SIN self
                    "fecha_firma": str(row.get('fecha_de_inicio_del_contrato', '')),
                    "tipo_contrato": str(row.get('tipo_de_contrato', '')),
                    "estado_contrato": str(row.get('estado_contrato', '')),
                    "modalidad": str(row.get('modalidad_de_contratacion', '')),
                    "anno": parse_year(row.get('anno_bpin'), 2024),  # SIN self - CAMBIO AQUÍ
                    "id_interno_sistema": f"SYS-{row.name}",
                    "campo_vacio": None,
                    "constante_1": "VALOR_FIJO",
                    "constante_2": 100,
                    "duplicate_id": str(row.get('referencia_del_contrato', '')),
                    "timestamp_carga": datetime.now().isoformat()
                }
                
                return contract
                
            except Exception as e:
                print(f"Error preparando contrato en fila {row.name}: {str(e)}")
                return None


=======
    def prepare_contract_message(self, row):
        try:
            contract = {
                "id_contrato": str(row.get('referencia_del_contrato', row.get('uid', f"CT-{row.name}"))),
                "objeto_contrato": str(row.get('descripcion_del_procedimiento', row.get('objeto_del_contrato', ''))),
                "entidad": str(row.get('nombre_entidad', '')),
                "departamento": self.departamento,
                "region": DEPARTAMENTO_REGION.get(self.departamento, 'Desconocida'),
                "codigo_unspsc": str(row.get('codigo_de_categoria_principal', '')),
                "descripcion_categoria": str(row.get('descripcion_del_proceso', '')),
                "valor_contrato": float(row.get('valor_del_contrato', 0)) if pd.notna(row.get('valor_del_contrato')) else 0.0,
                "duracion_dias": parse_duration(row.get('duraci_n_del_contrato')),
                "fecha_firma": str(row.get('fecha_de_inicio_del_contrato', '')),
                "tipo_contrato": str(row.get('tipo_de_contrato', '')),
                "estado_contrato": str(row.get('estado_contrato', '')),
                "modalidad": str(row.get('modalidad_de_contratacion', '')),
                "anno": parse_year(row.get('anno_bpin'), 2024),
                "id_interno_sistema": f"SYS-{row.name}",
                "campo_vacio": None,
                "constante_1": "VALOR_FIJO",
                "constante_2": 100,
                "duplicate_id": str(row.get('referencia_del_contrato', '')),
                "timestamp_carga": datetime.now().isoformat()
            }
            return contract

        except Exception as e:
            print(f"Error preparando contrato en fila {row.name}: {str(e)}")
            return None
>>>>>>> 01ba0aa (Actualización de Dockerfile, notebooks y scripts de contrato)

    def send_contracts_for_date(self, target_date):
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos")

        date_contracts = self.contracts_df[
            self.contracts_df['fecha_de_firma'] == target_date
        ]

        if len(date_contracts) == 0:
            print(f"No hay contratos para {target_date}")
            return 0

        sent_count = 0
        error_count = 0

        for idx, row in date_contracts.iterrows():
            try:
                contract = self.prepare_contract_message(row)
                self.producer.send(self.topic, value=contract)
                sent_count += 1

            except Exception as e:
                error_count += 1
                continue

        self.producer.flush()

        print(f"[OK] {sent_count:,} contratos enviados para {target_date}")
        if error_count:
            print(f"[WARNING] {error_count} errores")

        return sent_count

    def simulate_incremental_load(self, start_date=None, end_date=None, max_days=None):
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos")

        all_dates = sorted(self.contracts_df["fecha_de_firma"].unique())

        if start_date is None:
            start_date = all_dates[0]
        if end_date is None:
            end_date = all_dates[-1]

        dates = [d for d in all_dates if start_date <= d <= end_date]

        if max_days:
            dates = dates[:max_days]

        total_sent = 0

        for date in dates:
            total_sent += self.send_contracts_for_date(date)
            time.sleep(0.5)

        print(f"Total enviados: {total_sent:,}")

    def close(self):
        self.producer.close()


def main():
    KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
    KAFKA_TOPIC = 'contratos-publicos'
    DEPARTAMENTO = "Cundinamarca"
    YEAR = 2024

    try:
        producer = SECOPContractProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC
        )

        success = producer.download_secop_data(
            departamento=DEPARTAMENTO,
            year=YEAR
        )

        if not success:
            print("No se pudieron descargar datos.")
            return

        resp = input("¿Deseas cargar incrementamente? (s/n): ")
        if resp.lower() != "s":
            print("Cancelado.")
            return

        producer.simulate_incremental_load()
        producer.close()

        print("[OK] Proceso finalizado.")

    except Exception as e:
        print(f"Error fatal: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
