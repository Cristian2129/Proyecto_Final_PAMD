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


class SECOPContractProducer:
    def __init__(self, bootstrap_servers='kafka:29092', topic='contratos-publicos'):
        """
        Inicializa el productor de Kafka
        
        Args:
            bootstrap_servers: Dirección del servidor Kafka (usar kafka:29092 para comunicación interna)
            topic: Nombre del tópico donde se publicarán los mensajes
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            max_request_size=10485760,
            # Configuraciones adicionales para estabilidad
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
        print(f"Departamento: {departamento}")
        print("======================================================================\n")

        try:
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"

            print("Consultando volumen de contratos en SECOP II...")

            where_clause = (
                f"departamento='{departamento}' AND "
                f"fecha_de_firma >= '2024-01-01T00:00:00' AND "
                f"fecha_de_firma < '2025-01-01T00:00:00'"
            )

            results = client.get(dataset_id, where=where_clause, select="COUNT(*) as total")

            if results:
                total = int(results[0]['total'])
                print(f"\nContratos encontrados en 2024: {total:,}")
                return total

            return 0

        except Exception as e:
            print(f"No se pudo verificar el volumen: {str(e)}")
            print("Continuando sin verificación...")
            return -1

    def download_secop_data(self, departamento, year=2024):
        """
        Descarga datos de SECOP II con reintentos y manejo de errores
        """
        self.departamento = departamento

        print("\n======================================================================")
        print("DESCARGANDO DATOS DE SECOP II")
        print("======================================================================")
        print(f"Departamento: {departamento}")
        print(f"Región: {DEPARTAMENTO_REGION.get(departamento, 'Desconocida')}")
        print(f"Año: {year}")
        print("======================================================================\n")

        total_available = self.check_department_data(departamento)

        try:
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"

            print("\nDescargando datos completos de la API (sin límite)...")
            print("Esto puede tardar varios minutos según el volumen\n")

            where_clause = (
                f"departamento='{departamento}' AND "
                f"fecha_de_firma >= '{year}-01-01T00:00:00' AND "
                f"fecha_de_firma < '{year+1}-01-01T00:00:00'"
            )

            offset = 0
            batch_size = 10000  # Reducido para evitar timeouts
            all_results = []
            max_retries = 3

            while True:
                print(f"Descargando lote: {offset:,} - {offset + batch_size:,}")
                
                retry_count = 0
                batch_results = None
                
                # Implementar reintentos con backoff exponencial
                while retry_count < max_retries:
                    try:
                        batch_results = client.get(
                            dataset_id,
                            where=where_clause,
                            limit=batch_size,
                            offset=offset,
                            order="fecha_de_firma ASC"
                        )
                        break  # Si funciona, salir del loop de reintentos
                        
                    except Exception as e:
                        retry_count += 1
                        if retry_count < max_retries:
                            wait_time = 2 ** retry_count  # 2, 4, 8 segundos
                            print(f"⚠️ Error en descarga (intento {retry_count}/{max_retries}): {str(e)}")
                            print(f"Reintentando en {wait_time} segundos...")
                            time.sleep(wait_time)
                        else:
                            print(f"❌ Error persistente después de {max_retries} intentos")
                            raise

                if not batch_results:
                    print("No hay más datos disponibles. Descarga finalizada.")
                    break

                all_results.extend(batch_results)
                offset += batch_size

                print(f"Total acumulado: {len(all_results):,}")

                time.sleep(2)  # Delay entre requests para evitar rate limiting

            self.contracts_df = pd.DataFrame.from_records(all_results)

            if len(self.contracts_df) == 0:
                print("No se encontraron contratos para los filtros especificados.")
                return False

            print(f"\nTotal descargado: {len(self.contracts_df):,} contratos\n")

            self.contracts_df['fecha_de_firma'] = pd.to_datetime(
                self.contracts_df['fecha_de_firma']
            ).dt.date

            return True

        except Exception as e:
            print(f"Error descargando datos: {str(e)}")
            raise

    def prepare_contract_message(self, row):
        contract = {
            "id_contrato": str(row.get('referencia_del_contrato', row.get('uid', f"CT-{row.name}"))),
            "objeto_contrato": str(row.get('descripcion_del_procedimiento', row.get('objeto_del_contrato', ''))),
            "entidad": str(row.get('nombre_entidad', '')),
            "departamento": self.departamento,
            "municipio": str(row.get('municipio', '')),
            "region": DEPARTAMENTO_REGION.get(self.departamento, 'Desconocida'),
            "codigo_unspsc": str(row.get('codigo_principal_de_categoria', '')),
            "descripcion_categoria": str(row.get('descripcion_del_codigo_principal', '')),
            "valor_contrato": float(row.get('valor_del_contrato', 0)) if pd.notna(row.get('valor_del_contrato')) else 0.0,
            "duracion_dias": int(float(row.get('plazo_de_ejec_del_contrato', 0))) if pd.notna(row.get('plazo_de_ejec_del_contrato')) else None,
            "fecha_firma": str(row.get('fecha_de_firma', '')),
            "tipo_contrato": str(row.get('tipo_de_contrato', '')),
            "estado_contrato": str(row.get('estado_contrato', '')),
            "modalidad": str(row.get('modalidad_de_contratacion', '')),
            "anno": int(row.get('anno_cargue_secop', 2024)) if pd.notna(row.get('anno_cargue_secop')) else 2024,
            "id_interno_sistema": f"SYS-{row.name}",
            "campo_vacio": None,
            "constante_1": "VALOR_FIJO",
            "constante_2": 100,
            "duplicate_id": str(row.get('referencia_del_contrato', '')),
            "timestamp_carga": datetime.now().isoformat()
        }

        return contract

    def send_contracts_for_date(self, target_date):
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos con download_secop_data()")

        date_contracts = self.contracts_df[
            self.contracts_df['fecha_de_firma'] == target_date
        ]

        if len(date_contracts) == 0:
            print(f"No hay contratos para la fecha: {target_date}")
            return 0

        print("\n======================================================================")
        print(f"Enviando contratos del {target_date}")
        print("======================================================================")
        print(f"Total a enviar: {len(date_contracts):,}")
        print("======================================================================\n")

        sent_count = 0
        error_count = 0

        for idx, row in date_contracts.iterrows():
            try:
                contract = self.prepare_contract_message(row)
                self.producer.send(self.topic, value=contract)
                sent_count += 1

                if sent_count % 50 == 0:
                    print(f"Enviados {sent_count:,}/{len(date_contracts):,} contratos...")

                if sent_count % 100 == 0:
                    time.sleep(0.1)

            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"Error enviando contrato {idx}: {str(e)}")
                continue

        self.producer.flush()
        print(f"\n✅ {sent_count:,} contratos enviados correctamente para {target_date}")

        if error_count > 0:
            print(f"⚠️ {error_count} contratos con errores\n")

        return sent_count

    def simulate_incremental_load(self, start_date=None, end_date=None, max_days=None):
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos con download_secop_data()")

        all_dates = sorted(self.contracts_df['fecha_de_firma'].unique())

        if start_date is None:
            start_date = all_dates[0]
        if end_date is None:
            end_date = all_dates[-1]

        dates_with_data = [d for d in all_dates if start_date <= d <= end_date]

        if max_days:
            dates_with_data = dates_with_data[:max_days]

        print("\n======================================================================")
        print("INICIANDO CARGA INCREMENTAL A KAFKA")
        print("======================================================================")
        print(f"Departamento: {self.departamento}")
        print(f"Fecha inicial: {start_date}")
        print(f"Fecha final: {dates_with_data[-1]}")
        print(f"Total días a procesar: {len(dates_with_data)}")
        print(f"Tópico Kafka: {self.topic}")
        print("======================================================================\n")

        total_sent = 0

        for i, date in enumerate(dates_with_data, 1):
            print(f"\n📅 Día {i}/{len(dates_with_data)}")
            sent = self.send_contracts_for_date(date)
            total_sent += sent
            time.sleep(0.5)

        print("\n======================================================================")
        print("✅ CARGA INCREMENTAL COMPLETADA")
        print("======================================================================")
        print(f"Total de contratos enviados: {total_sent:,}")
        print(f"Días procesados: {len(dates_with_data)}")
        print(f"Promedio por día: {total_sent / len(dates_with_data):.1f}")
        print("======================================================================\n")

    def close(self):
        self.producer.close()


def main():
    # Configuración para comunicación interna de Docker
    KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'  # Puerto interno de Kafka
    KAFKA_TOPIC = 'contratos-publicos'
    DEPARTAMENTO = "Cundinamarca"
    YEAR = 2024

    print("\n======================================================================")
    print("PRODUCTOR DE CONTRATOS SECOP II PARA KAFKA")
    print("======================================================================\n")

    print("Departamentos recomendados (alto volumen):")
    for dept in DEPARTAMENTOS_DISPONIBLES[:5]:
        print(f" - {dept}")
    print()

    try:
        producer = SECOPContractProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC
        )

        print(f"Descargando datos para: {DEPARTAMENTO}")
        success = producer.download_secop_data(
            departamento=DEPARTAMENTO,
            year=YEAR
        )

        if not success:
            print("No se pudieron descargar datos. Abortando.")
            return

        print("\n======================================================================")
        response = input("¿Deseas proceder con la carga incremental a Kafka? (s/n): ")
        if response.lower() != 's':
            print("Carga cancelada por el usuario")
            return

        producer.simulate_incremental_load()
        producer.close()
        print("\n✅ Proceso finalizado correctamente\n")

    except KeyboardInterrupt:
        print("\n⚠️ Proceso interrumpido por el usuario")
        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()