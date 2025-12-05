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

# Departamentos con mayor volumen de contratación
DEPARTAMENTOS_DISPONIBLES = [
    "Bogotá D.C.",      # Mayor volumen
    "Antioquia",        # Alto volumen
    "Valle del Cauca",  # Alto volumen
    "Cundinamarca",     # Alto volumen
    "Santander",        # Alto volumen
    "Atlántico",        # Moderado-Alto
    "Bolívar",          # Moderado-Alto
]

# Mapeo de regiones por departamento
DEPARTAMENTO_REGION = {
    "Bogotá D.C.": "Centro-Oriente",
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
    def __init__(self, bootstrap_servers='localhost:9092', topic='contratos-publicos'):
        """Inicializa el productor de Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            max_request_size=10485760  # 10MB
        )
        self.topic = topic
        self.contracts_df = None
        self.departamento = None
        
    def check_department_data(self, departamento):
        """
        Verifica cuántos contratos tiene un departamento antes de descargar
        """
        print(f"\n{'='*70}")
        print(f"🔍 VERIFICANDO DISPONIBILIDAD DE DATOS")
        print(f"{'='*70}")
        print(f"Departamento: {departamento}")
        print(f"{'='*70}\n")
        
        try:
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"
            
            print("📊 Consultando volumen de contratos en SECOP II...")
            
            # Consultar solo el conteo
            where_clause = f"departamento='{departamento}' AND fecha_de_firma >= '2024-01-01T00:00:00' AND fecha_de_firma < '2025-01-01T00:00:00'"
            
            # Obtener conteo aproximado
            results = client.get(
                dataset_id,
                where=where_clause,
                select="COUNT(*) as total",
            )
            
            if results:
                total = int(results[0]['total'])
                print(f"\n✅ Contratos encontrados en 2024: {total:,}")
                
                if total < 100000:
                    print(f"\n⚠️  ADVERTENCIA: Este departamento tiene menos de 100,000 contratos")
                    print(f"   Se recomienda usar: Bogotá D.C., Antioquia o Valle del Cauca")
                else:
                    print(f"✅ Este departamento cumple con el mínimo de 100,000 contratos")
                
                return total
            
            return 0
            
        except Exception as e:
            print(f"⚠️  No se pudo verificar el volumen: {str(e)}")
            print("   Continuaremos con la descarga de todos modos...")
            return -1
    
    def download_secop_data(self, departamento, year=2024, limit=150000):
        """
        Descarga datos de SECOP II desde datos.gov.co para un departamento específico
        
        Args:
            departamento: Nombre del departamento (ej: "Bogotá D.C.")
            year: Año a filtrar (default: 2024)
            limit: Límite de registros a descargar (mínimo 100,000)
        """
        
        self.departamento = departamento
        
        print(f"\n{'='*70}")
        print(f"DESCARGANDO DATOS DE SECOP II")
        print(f"{'='*70}")
        print(f"Departamento: {departamento}")
        print(f"Región: {DEPARTAMENTO_REGION.get(departamento, 'Desconocida')}")
        print(f"Año: {year}")
        print(f"Límite de registros: {limit:,}")
        print(f"{'='*70}\n")
        
        # Verificar disponibilidad primero
        total_available = self.check_department_data(departamento)
        
        if total_available > 0 and total_available < 100000:
            print(f"\n⚠️  IMPORTANTE: Solo hay {total_available:,} contratos disponibles")
            response = input("¿Deseas continuar de todos modos? (s/n): ")
            if response.lower() != 's':
                return False
        
        try:
            # Conectar a la API de datos.gov.co
            client = Socrata("www.datos.gov.co", None)
            dataset_id = "jbjy-vk9h"
            
            print("\n📥 Descargando datos completos de la API...")
            print(f"   Target: {limit:,} contratos")
            print("   (Esto puede tomar 10-20 minutos dependiendo del volumen)\n")
            
            # Construir filtro WHERE
            where_clause = f"departamento='{departamento}' AND fecha_de_firma >= '{year}-01-01T00:00:00' AND fecha_de_firma < '{year+1}-01-01T00:00:00'"
            
            # Descargar datos en lotes
            offset = 0
            batch_size = 50000
            all_results = []
            
            while offset < limit:
                print(f"   Descargando lote: {offset:,} - {offset+batch_size:,}")
                
                batch_results = client.get(
                    dataset_id,
                    where=where_clause,
                    limit=batch_size,
                    offset=offset,
                    order="fecha_de_firma ASC"
                )
                
                if not batch_results:
                    print("   No hay más datos disponibles")
                    break
                
                all_results.extend(batch_results)
                offset += batch_size
                
                print(f"   ✓ Total acumulado: {len(all_results):,}")
                
                # Si ya alcanzamos el límite, salir
                if len(all_results) >= limit:
                    break
                
                time.sleep(1)  # Pausa para no saturar la API
            
            # Convertir a DataFrame
            self.contracts_df = pd.DataFrame.from_records(all_results)
            
            if len(self.contracts_df) == 0:
                print("\n⚠️  No se encontraron contratos para los filtros especificados.")
                return False
            
            print(f"\n✅ Total descargado: {len(self.contracts_df):,} contratos\n")
            
            # Validar mínimo de 100,000
            if len(self.contracts_df) < 100000:
                print(f"⚠️  ADVERTENCIA: Se descargaron {len(self.contracts_df):,} contratos")
                print(f"   El objetivo era mínimo 100,000 contratos")
                print(f"\n   Recomendación: Usa 'Bogotá D.C.' o 'Antioquia' para mayor volumen")
            else:
                print(f"✅ Se cumple el mínimo de 100,000 contratos")
            
            # Información básica
            print(f"\n📊 Resumen de datos:")
            print(f"   - Registros: {len(self.contracts_df):,}")
            print(f"   - Columnas: {len(self.contracts_df.columns)}")
            print(f"   - Departamento: {departamento}")
            print(f"   - Región: {DEPARTAMENTO_REGION.get(departamento, 'Desconocida')}")
            
            # Procesar fechas
            self.contracts_df['fecha_de_firma'] = pd.to_datetime(
                self.contracts_df['fecha_de_firma']
            ).dt.date
            
            print(f"\n📅 Distribución temporal:")
            date_counts = self.contracts_df['fecha_de_firma'].value_counts().sort_index()
            print(f"   - Primer contrato: {date_counts.index[0]} ({date_counts.iloc[0]} contratos)")
            print(f"   - Último contrato: {date_counts.index[-1]} ({date_counts.iloc[-1]} contratos)")
            print(f"   - Total días con contratos: {len(date_counts)}")
            print(f"   - Promedio por día: {len(self.contracts_df) / len(date_counts):.1f}")
            
            # Mostrar columnas disponibles
            print(f"\n📋 Columnas principales disponibles:")
            key_columns = [
                'referencia_del_contrato', 'descripcion_del_procedimiento',
                'nombre_entidad', 'departamento', 'municipio',
                'valor_del_contrato', 'fecha_de_firma', 'codigo_principal_de_categoria'
            ]
            available = [col for col in key_columns if col in self.contracts_df.columns]
            for col in available[:10]:
                print(f"   ✓ {col}")
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error descargando datos: {str(e)}")
            print("\nPosibles soluciones:")
            print("1. Verifica tu conexión a internet")
            print("2. Instala la librería: pip install sodapy")
            print("3. La API puede estar temporalmente no disponible")
            print("4. Intenta con un límite menor")
            raise
    
    def prepare_contract_message(self, row):
        """Prepara un contrato en formato JSON para Kafka"""
        
        # Extraer campos principales con manejo de valores nulos
        contract = {
            # Campos principales
            "id_contrato": str(row.get('referencia_del_contrato', row.get('uid', f"CT-{row.name}"))),
            "objeto_contrato": str(row.get('descripcion_del_procedimiento', row.get('objeto_del_contrato', ''))),
            "entidad": str(row.get('nombre_entidad', '')),
            "departamento": self.departamento,
            "municipio": str(row.get('municipio', '')),
            "region": DEPARTAMENTO_REGION.get(self.departamento, 'Desconocida'),
            
            # Categorización
            "codigo_unspsc": str(row.get('codigo_principal_de_categoria', '')),
            "descripcion_categoria": str(row.get('descripcion_del_codigo_principal', '')),
            
            # Valores numéricos
            "valor_contrato": float(row.get('valor_del_contrato', 0)) if pd.notna(row.get('valor_del_contrato')) else 0.0,
            "duracion_dias": int(float(row.get('plazo_de_ejec_del_contrato', 0))) if pd.notna(row.get('plazo_de_ejec_del_contrato')) else None,
            
            # Fechas
            "fecha_firma": str(row.get('fecha_de_firma', '')),
            
            # Información adicional
            "tipo_contrato": str(row.get('tipo_de_contrato', '')),
            "estado_contrato": str(row.get('estado_contrato', '')),
            "modalidad": str(row.get('modalidad_de_contratacion', '')),
            
            # Metadata para análisis
            "anno": int(row.get('anno_cargue_secop', 2024)) if pd.notna(row.get('anno_cargue_secop')) else 2024,
            
            # Variables ruidosas/redundantes (para fase de limpieza)
            "id_interno_sistema": f"SYS-{row.name}",
            "campo_vacio": None,
            "constante_1": "VALOR_FIJO",
            "constante_2": 100,
            "duplicate_id": str(row.get('referencia_del_contrato', '')),
            "timestamp_carga": datetime.now().isoformat()
        }
        
        return contract
    
    def send_contracts_for_date(self, target_date):
        """Envía todos los contratos de una fecha específica a Kafka"""
        
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos con download_secop_data()")
        
        # Filtrar contratos de la fecha
        date_contracts = self.contracts_df[
            self.contracts_df['fecha_de_firma'] == target_date
        ]
        
        if len(date_contracts) == 0:
            print(f"⚠️  No hay contratos para la fecha: {target_date}")
            return 0
        
        print(f"\n{'='*70}")
        print(f"📤 Enviando contratos del {target_date}")
        print(f"{'='*70}")
        print(f"Total a enviar: {len(date_contracts):,}")
        print(f"{'='*70}\n")
        
        sent_count = 0
        error_count = 0
        
        for idx, row in date_contracts.iterrows():
            try:
                # Preparar mensaje
                contract = self.prepare_contract_message(row)
                
                # Enviar a Kafka
                self.producer.send(self.topic, value=contract)
                sent_count += 1
                
                # Log cada 50 contratos
                if sent_count % 50 == 0:
                    print(f"   ✓ Enviados {sent_count:,}/{len(date_contracts):,} contratos...")
                
                # Pequeña pausa cada 100 contratos
                if sent_count % 100 == 0:
                    time.sleep(0.1)
                
            except Exception as e:
                error_count += 1
                if error_count <= 5:  # Solo mostrar los primeros 5 errores
                    print(f"   ⚠️  Error enviando contrato {idx}: {str(e)}")
                continue
        
        self.producer.flush()
        print(f"\n✅ {sent_count:,} contratos enviados exitosamente para {target_date}")
        if error_count > 0:
            print(f"⚠️  {error_count} contratos con errores\n")
        
        return sent_count
    
    def simulate_incremental_load(self, start_date=None, end_date=None, max_days=None):
        """
        Simula carga incremental día por día de los contratos descargados
        
        Args:
            start_date: Fecha inicial (None = primera fecha en los datos)
            end_date: Fecha final (None = última fecha en los datos)
            max_days: Máximo número de días a procesar (None = todos)
        """
        
        if self.contracts_df is None:
            raise ValueError("Primero debes descargar los datos con download_secop_data()")
        
        # Obtener rango de fechas
        all_dates = sorted(self.contracts_df['fecha_de_firma'].unique())
        
        if start_date is None:
            start_date = all_dates[0]
        if end_date is None:
            end_date = all_dates[-1]
        
        dates_with_data = [d for d in all_dates if start_date <= d <= end_date]
        
        if max_days:
            dates_with_data = dates_with_data[:max_days]
        
        print(f"\n{'='*70}")
        print(f"🚀 INICIANDO CARGA INCREMENTAL A KAFKA")
        print(f"{'='*70}")
        print(f"Departamento: {self.departamento}")
        print(f"Fecha inicial: {start_date}")
        print(f"Fecha final: {dates_with_data[-1]}")
        print(f"Total días a procesar: {len(dates_with_data)}")
        print(f"Tópico Kafka: {self.topic}")
        print(f"{'='*70}\n")
        
        total_sent = 0
        
        for i, date in enumerate(dates_with_data, 1):
            print(f"\n📆 Día {i}/{len(dates_with_data)}")
            sent = self.send_contracts_for_date(date)
            total_sent += sent
            
            # Pausa entre días
            time.sleep(0.5)
        
        print(f"\n{'='*70}")
        print(f"✅ CARGA INCREMENTAL COMPLETADA")
        print(f"{'='*70}")
        print(f"Total de contratos enviados: {total_sent:,}")
        print(f"Días procesados: {len(dates_with_data)}")
        print(f"Promedio por día: {total_sent / len(dates_with_data):.1f}")
        print(f"{'='*70}\n")
    
    def close(self):
        """Cierra el productor"""
        self.producer.close()


def main():
    """Función principal"""
    
    # ==================== CONFIGURACIÓN ====================
    
    KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
    KAFKA_TOPIC = 'contratos-publicos'
    
    # Selecciona un departamento con alto volumen
    # Recomendados para +100k contratos: "Bogotá D.C.", "Antioquia", "Valle del Cauca"
    DEPARTAMENTO = "Cundinamarca"
    
    YEAR = 2024
    LIMIT = 150000  # Descargar 150k para asegurar +100k válidos
    
    # =======================================================
    
    print("\n" + "="*70)
    print("🚀 PRODUCTOR DE CONTRATOS SECOP II PARA KAFKA")
    print("   Enfocado en un departamento con +100,000 contratos")
    print("="*70 + "\n")
    
    print(f"📍 Departamentos recomendados (alto volumen):")
    for dept in DEPARTAMENTOS_DISPONIBLES[:5]:
        print(f"   • {dept}")
    print()
    
    try:
        # Crear productor
        producer = SECOPContractProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC
        )
        
        # Descargar datos de SECOP II
        print(f"Descargando datos para: {DEPARTAMENTO}")
        success = producer.download_secop_data(
            departamento=DEPARTAMENTO,
            year=YEAR,
            limit=LIMIT
        )
        
        if not success:
            print("\n⚠️  No se pudieron descargar datos. Abortando.")
            return
        
        # Preguntar al usuario si continuar
        print("\n" + "="*70)
        response = input("¿Deseas proceder con la carga incremental a Kafka? (s/n): ")
        if response.lower() != 's':
            print("\n❌ Carga cancelada por el usuario")
            return
        
        # Realizar carga incremental día por día
        producer.simulate_incremental_load()
        
        # Cerrar productor
        producer.close()
        print("\n✅ Proceso finalizado exitosamente\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error fatal: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()