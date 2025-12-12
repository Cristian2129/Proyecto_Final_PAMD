"""
Productor INCREMENTAL de contratos SECOP II
Solo carga contratos NUEVOS desde la última ejecución
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
from sodapy import Socrata
import time
import os
import re

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


class IncrementalContractProducer:
    def __init__(self, bootstrap_servers='kafka:29092', topic='contratos-publicos'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            max_request_size=10485760,
        )
        self.topic = topic
        self.state_file = "/app/notebooks/producer_state.json"
        
    def load_state(self):
        """Carga el estado de la última ejecución"""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                last_date = datetime.strptime(state['last_processed_date'], '%Y-%m-%d').date()
                print(f"📊 Última fecha procesada: {last_date}")
                return last_date
        else:
            default_date = (datetime.now() - timedelta(days=30)).date()
            print(f"⚠️ Primera ejecución. Iniciando desde: {default_date}")
            return default_date
    
    def save_state(self, last_date):
        """Guarda el estado de la última ejecución"""
        state = {
            'last_processed_date': last_date.strftime('%Y-%m-%d'),
            'timestamp': datetime.now().isoformat(),
            'total_loaded': getattr(self, 'total_sent', 0)
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        print(f"✅ Estado guardado: {last_date}")
    
    def download_new_contracts(self, departamento, start_date, end_date=None):
        """Descarga solo contratos NUEVOS desde start_date"""
        if end_date is None:
            end_date = datetime.now().date()
        
        print("\n" + "="*70)
        print("📊 CARGA INCREMENTAL DE CONTRATOS")
        print("="*70)
        print(f"Departamento: {departamento}")
        print(f"Desde: {start_date}")
        print(f"Hasta: {end_date}")
        print("="*70 + "\n")
        
        try:
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"
            
            where_clause = (
                f"departamento='{departamento}' AND "
                f"fecha_de_firma >= '{start_date}T00:00:00' AND "
                f"fecha_de_firma <= '{end_date}T23:59:59'"
            )
            
            print("🔍 Consultando contratos nuevos...")
            
            offset = 0
            batch_size = 5000
            all_results = []
            
            while True:
                batch = client.get(
                    dataset_id,
                    where=where_clause,
                    limit=batch_size,
                    offset=offset,
                    order="fecha_de_firma ASC"
                )
                
                if not batch:
                    break
                
                all_results.extend(batch)
                offset += batch_size
                print(f"   Descargados: {len(all_results):,} contratos...")
                
                time.sleep(1)
            
            if len(all_results) == 0:
                print("⚠️ No hay contratos nuevos en este rango")
                return None
            
            self.contracts_df = pd.DataFrame.from_records(all_results)
            self.contracts_df['fecha_de_firma'] = pd.to_datetime(
                self.contracts_df['fecha_de_firma']
            ).dt.date
            
            print(f"\n✅ Total descargado: {len(self.contracts_df):,} contratos nuevos")
            return self.contracts_df
            
        except Exception as e:
            print(f"❌ Error descargando: {str(e)}")
            raise
    
    def send_contracts(self):
        """Envía todos los contratos descargados a Kafka"""
        if self.contracts_df is None or len(self.contracts_df) == 0:
            print("⚠️ No hay contratos para enviar")
            return 0
        
        print("\n📤 Enviando contratos a Kafka...")
        print(f"Total a enviar: {len(self.contracts_df):,}")
        
        sent_count = 0
        error_count = 0
        
        for idx, row in self.contracts_df.iterrows():
            try:
                contract = self.prepare_contract_message(row)
                self.producer.send(self.topic, value=contract)
                sent_count += 1
                
                if sent_count % 100 == 0:
                    print(f"   Enviados: {sent_count:,}/{len(self.contracts_df):,}")
                    time.sleep(0.1)
                    
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"⚠️ Error en contrato {idx}: {str(e)}")
        
        self.producer.flush()
        
        print(f"\n✅ Enviados exitosamente: {sent_count:,} contratos")
        if error_count > 0:
            print(f"⚠️ Errores: {error_count}")
        
        self.total_sent = sent_count
        return sent_count
    
    def prepare_contract_message(self, row):
        """Prepara el mensaje del contrato"""
        departamento = str(row.get('departamento', ''))
        
        return {
            "id_contrato": str(row.get('referencia_del_contrato', f"CT-{row.name}")),
            "objeto_contrato": str(row.get('descripcion_del_procedimiento', '')),
            "entidad": str(row.get('nombre_entidad', '')),
            "departamento": departamento,
            "municipio": str(row.get('municipio', '')),
            "region": DEPARTAMENTO_REGION.get(departamento, 'Desconocida'),
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
    
    def run_incremental_load(self, departamento):
        """Ejecuta la carga incremental completa"""
        print("\n" + "="*70)
        print("🚀 INICIANDO CARGA INCREMENTAL")
        print("="*70)
        
        last_date = self.load_state()
        today = datetime.now().date()
        
        if last_date >= today:
            print(f"✅ Ya estamos al día. Última fecha: {last_date}")
            return
        
        self.download_new_contracts(
            departamento=departamento,
            start_date=last_date + timedelta(days=1),
            end_date=today
        )
        
        if self.contracts_df is None or len(self.contracts_df) == 0:
            print("✅ No hay contratos nuevos")
            return
        
        sent = self.send_contracts()
        
        if sent > 0:
            latest_date = self.contracts_df['fecha_de_firma'].max()
            self.save_state(latest_date)
        
        print("\n" + "="*70)
        print("✅ CARGA INCREMENTAL COMPLETADA")
        print("="*70)
        print(f"Contratos procesados: {sent:,}")
        print(f"Nueva fecha de corte: {latest_date}")
        print("="*70)
    
    def close(self):
        self.producer.close()


def main():
    """Script principal para carga incremental"""
    KAFKA_BOOTSTRAP = 'kafka:29092'
    TOPIC = 'contratos-publicos'
    DEPARTAMENTO = "Cundinamarca"
    
    print("\n🔄 PRODUCTOR INCREMENTAL - SECOP II")
    print(f"Departamento: {DEPARTAMENTO}\n")
    
    try:
        producer = IncrementalContractProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic=TOPIC
        )
        
        producer.run_incremental_load(departamento=DEPARTAMENTO)
        producer.close()
        
        print("\n✅ Proceso completado exitosamente\n")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()