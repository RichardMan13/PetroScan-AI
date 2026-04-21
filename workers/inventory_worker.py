import os
import json
import logging
import pandas as pd
import psycopg2
from typing import Dict, Any
from tempfile import NamedTemporaryFile
import boto3

# Importar o BaseWorker
from base_worker import BaseWorker

logger = logging.getLogger("InventoryWorker")

class InventoryWorker(BaseWorker):
    """
    Worker especialista em processamento de dados estruturados (ETL).
    Lê planilhas de inventário de ativos (XLSX/CSV) e sincroniza no banco de dados.
    """

    def __init__(self):
        # A fila que este worker escuta (Fase 4 do README)
        super().__init__(queue_name="task.inventory")
        
        # Conexão com S3 (MinIO)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

        # Conexão com Postgres
        self.db_params = {
            'host': os.getenv("POSTGRES_HOST", "127.0.0.1"),
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Executa o ETL de uma planilha de inventário.
        Esperado no corpo: { 's3_key': 'inventory_2025.xlsx', 'bucket': 'petroscan-inventory' }
        """
        s3_key = body.get('s3_key')
        bucket = body.get('bucket', 'petroscan-inventory')

        if not s3_key:
            logger.error("Mensagem malformada: s3_key ausente.")
            return False

        try:
            # 1. Download do arquivo do MinIO
            tmp = NamedTemporaryFile(delete=False, suffix=os.path.splitext(s3_key)[1])
            temp_path = tmp.name
            tmp.close()  # Fecha o handle para evitar WinError 32 no Windows

            logger.info(f"Baixando inventário: {s3_key}...")
            self.s3_client.download_file(bucket, s3_key, temp_path)

            # 2. Leitura com Pandas (Suporte a CSV e Excel)
            if s3_key.endswith('.csv'):
                df = pd.read_csv(temp_path)
            else:
                df = pd.read_excel(temp_path)

            # 3. Sanitização e Normalização (Data Cleaning)
            # - Padronizar nomes de colunas
            # - Limpar espaços em branco
            # - Normalizar TAGs para uppercase
            df.columns = [c.lower().strip() for c in df.columns]
            
            if 'tag' in df.columns:
                df['tag'] = df['tag'].astype(str).str.upper().str.strip()
            else:
                logger.error("A planilha não possui uma coluna 'tag' obrigatória.")
                return False

            # Lidar com datas nulas (Evitar erro no PostgreSQL DATE)
            if 'installation_date' in df.columns:
                df['installation_date'] = pd.to_datetime(df['installation_date'], errors='coerce')

            # 4. Ingestão no PostgreSQL (Bulk Upsert)
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            logger.info(f"Inserindo {len(df)} registros de ativos no banco...")
            
            for index, row in df.iterrows():
                query = """
                    INSERT INTO inventory (tag, description, location, installation_date, maintenance_status, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (tag) DO UPDATE SET
                        description = EXCLUDED.description,
                        location = EXCLUDED.location,
                        maintenance_status = EXCLUDED.maintenance_status,
                        metadata = EXCLUDED.metadata
                """
                
                # Transformar o restante das colunas em JSONB 'metadata'
                metadata = row.to_dict()
                
                cur.execute(query, (
                    row['tag'],
                    row.get('description', 'N/A'),
                    row.get('location', 'Plataforma'),
                    row['installation_date'] if pd.notnull(row.get('installation_date')) else None,
                    row.get('maintenance_status', 'Active'),
                    json.dumps(metadata, default=str)
                ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Limpeza
            os.unlink(temp_path)
            logger.info("ETL de inventário concluído com sucesso.")
            return True

        except Exception as e:
            logger.error(f"Erro no pipeline ETL: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = InventoryWorker()
    worker.run()
