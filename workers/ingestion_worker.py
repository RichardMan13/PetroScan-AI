import os
import json
import logging
import psycopg2
import boto3
from typing import Dict, Any
from tempfile import NamedTemporaryFile
from docling.document_converter import DocumentConverter

# Importar o BaseWorker que criamos anteriormente
from base_worker import BaseWorker

logger = logging.getLogger("IngestionWorker")

class IngestionWorker(BaseWorker):
    """
    Worker especialista em ingestão de documentos.
    Utiliza Docling para extrair texto e tabelas de PDFs técnicos.
    """

    def __init__(self):
        # A fila que este worker escuta (Fase 2 do README)
        super().__init__(queue_name="task.ingestion")
        
        # Conexão com S3 (MinIO)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        # Conexão com Postgres
        self.db_params = {
            'host': 'localhost', # Ou o nome do service no docker-compose se rodando dentro de outro container
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }
        
        # Inicializa o Conversor do Docling
        self.converter = DocumentConverter()

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Executa a extração do PDF via Docling e salva no PostgreSQL.
        Esperado no corpo: { 's3_key': 'path/to/doc.pdf', 'bucket': 'petroscan-docs', 'document_id': '...' }
        """
        s3_key = body.get('s3_key')
        bucket = body.get('bucket', 'petroscan-docs')
        document_id = body.get('document_id')

        if not s3_key or not document_id:
            logger.error("Mensagem malformada: s3_key ou document_id ausentes.")
            return False

        try:
            # 1. Download do arquivo do MinIO para um arquivo temporário
            with NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                logger.info(f"Fazendo download de {s3_key} do bucket {bucket}...")
                self.s3_client.download_file(bucket, s3_key, tmp_file.name)
                temp_path = tmp_file.name

            # 2. Conversão via Docling
            logger.info(f"Iniciando extração via Docling para {s3_key}...")
            result = self.converter.convert(temp_path)
            extracted_text = result.document.export_to_markdown()
            
            # Novo step: Upload do Markdown para o bucket de 'parsed docs' (Trabalho temporário)
            parsed_bucket = os.getenv("MINIO_BUCKET_PARSED", "petroscan-parsed")
            parsed_key = f"{document_id}.md"
            
            logger.info(f"Fazendo upload do conteúdo extraído para {parsed_key}...")
            self.s3_client.put_object(
                Bucket=parsed_bucket,
                Key=parsed_key,
                Body=extracted_text,
                ContentType='text/markdown'
            )

            metadata = {
                "source_s3": s3_key,
                "parsed_s3": parsed_key, # Chave para o EmbeddingWorker buscar
                "pages": len(result.document.pages) if hasattr(result.document, 'pages') else 0,
                "status": "extracted"
            }

            # 3. Persistência de METADADOS no PostgreSQL
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            # Nota: Coluna 'content' removida conforme refatoração do init.sql
            query = """
                INSERT INTO documents (id, title, metadata, category)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    metadata = EXCLUDED.metadata,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            cur.execute(query, (
                document_id,
                os.path.basename(s3_key),
                json.dumps(metadata),
                "technical_norm"
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Limpeza
            os.unlink(temp_path)
            logger.info(f"Processamento de {document_id} concluído. Texto disponível no S3.")
            return True

        except Exception as e:
            logger.error(f"Erro ao processar documento {document_id}: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = IngestionWorker()
    worker.run()
