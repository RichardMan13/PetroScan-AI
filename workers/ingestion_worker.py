import os
import json
import logging
import psycopg2
import boto3
from typing import Dict, Any
from tempfile import NamedTemporaryFile, mkdtemp
from docling.document_converter import DocumentConverter
from pdf2image import convert_from_path
import io

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
            # 1. Criar caminho para arquivo temporário
            tmp = NamedTemporaryFile(delete=False, suffix=".pdf")
            temp_path = tmp.name
            tmp.close() # Fecha o handle no Windows para permitir que o boto3 escreva nele

            logger.info(f"Fazendo download de {s3_key} do bucket {bucket}...")
            self.s3_client.download_file(bucket, s3_key, temp_path)

            # 2. Conversão via Docling
            # O Docling suporta PDF, PNG, JPG, etc.
            logger.info(f"Iniciando extração via Docling para {s3_key}...")
            result = self.converter.convert(temp_path)
            extracted_text = result.document.export_to_markdown()
            
            # Novo step: Garantir que o bucket de 'parsed docs' existe (Resiliência no Windows)
            parsed_bucket = os.getenv("MINIO_BUCKET_PARSED", "petroscan-parsed")
            try:
                self.s3_client.create_bucket(Bucket=parsed_bucket)
            except Exception:
                pass # Bucket já existe
                
            parsed_key = f"{document_id}.md"
            
            logger.info(f"Fazendo upload do conteúdo extraído para {parsed_key} no bucket {parsed_bucket}...")
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
            host = self.db_params['host']
            db = self.db_params['database']
            user = self.db_params['user']
            pwd = self.db_params['password']
            port = self.db_params['port']
            
            logger.info(f"Conectando ao banco {db} em {host}:{port} como {user}...")
            conn_str = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"
            conn = psycopg2.connect(conn_str)
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

            # --- NOVO: Extração de Páginas para Layout ---
            self.trigger_layout_tasks(document_id, s3_key, bucket)

            # --- NOVO: Disparo de Embedding (Busca Semântica) ---
            self.trigger_embedding_tasks(document_id)

            return True

        except Exception as e:
            logger.exception(f"FALHA CRÍTICA NO PROCESSAMENTO {document_id}")
            # Grave em um arquivo para o agente ler com certeza
            with open("tmp/ingestion_error.log", "a") as f:
                f.write(f"\n--- ERROR {document_id} ---\n")
                import traceback
                f.write(traceback.format_exc())
                f.write(f"REPR: {repr(e)}\n")
            return False

    def trigger_layout_tasks(self, document_id: str, original_s3_key: str, bucket: str):
        """
        Gera imagens de cada página do PDF e dispara o Layout Worker.
        """
        tmp_dir = mkdtemp()
        temp_pdf = os.path.join(tmp_dir, "doc.pdf")
        
        try:
            # 1. Download do PDF novamente (ou poderíamos ter mantido o anterior)
            self.s3_client.download_file(bucket, original_s3_key, temp_pdf)
            
            # 2. Converter PDF para Imagens
            logger.info(f"Convertendo {original_s3_key} para imagens (páginas)...")
            images = convert_from_path(temp_pdf)
            
            pages_bucket = os.getenv("MINIO_BUCKET_PAGES", "petroscan-pages")
            try:
                self.s3_client.create_bucket(Bucket=pages_bucket)
            except:
                pass

            conn_str = f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}"
            conn = psycopg2.connect(conn_str)
            cur = conn.cursor()

            for i, image in enumerate(images):
                page_num = i + 1
                page_key = f"{document_id}/page_{page_num}.png"
                
                # Salvar imagem em buffer
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format='PNG')
                img_byte_arr = img_byte_arr.getvalue()
                
                # Upload para S3
                self.s3_client.put_object(
                    Bucket=pages_bucket,
                    Key=page_key,
                    Body=img_byte_arr,
                    ContentType='image/png'
                )
                
                # 3. Registrar no Banco
                query = """
                    INSERT INTO document_pages (document_id, page_number, s3_key, status)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (document_id, page_number) DO UPDATE SET
                        s3_key = EXCLUDED.s3_key,
                        status = 'pending'
                """
                cur.execute(query, (document_id, page_num, page_key, 'pending'))
                
                # 4. Enviar para Fila Redis
                layout_task = {
                    "document_id": str(document_id),
                    "page_number": page_num,
                    "s3_key": page_key,
                    "bucket": pages_bucket
                }
                self.redis_client.lpush("task.layout", json.dumps(layout_task))
                
                logger.info(f"Página {page_num} enviada para LAYOUT (Doc: {document_id})")

            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Erro ao extrair imagens para layout do doc {document_id}: {str(e)}")
        finally:
            # Limpeza do diretório temporário
            if os.path.exists(temp_pdf):
                os.remove(temp_pdf)
            os.rmdir(tmp_dir)

    def trigger_embedding_tasks(self, document_id: str):
        """
        Dispara a tarefa de vetorização para o documento.
        """
        try:
            embedding_task = {
                "document_id": str(document_id)
            }
            self.redis_client.lpush("task.embedding", json.dumps(embedding_task))
            logger.info(f"Tarefa de EMBEDDING enviada para o documento {document_id}")
        except Exception as e:
            logger.error(f"Erro ao disparar embedding para o doc {document_id}: {str(e)}")

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = IngestionWorker()
    worker.run()
