import os
import json
import logging
import psycopg2
from typing import Dict, Any
from sentence_transformers import SentenceTransformer

# Importar o BaseWorker
from base_worker import BaseWorker

logger = logging.getLogger("EmbeddingWorker")

class EmbeddingWorker(BaseWorker):
    """
    Worker especialista em vetorização de conteúdo (Embeddings).
    Utiliza Sentence-Transformers para gerar vetores para busca semântica.
    """

    def __init__(self, model_name: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"):
        # A fila que este worker escuta
        super().__init__(queue_name="task.embedding")
        
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
            'host': 'localhost',
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }
        
        # Inicializa o modelo de embedding (Multilíngue para Português/Inglês)
        logger.info(f"Carregando modelo de embedding: {model_name}...")
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        logger.info(f"Modelo carregado. Dimensão: {self.embedding_dim}")

    def _chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> list:
        """Divide o texto em pedaços (chunks) com sobreposição para contexto."""
        chunks = []
        if not text:
            return chunks
        
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunks.append(text[start:end])
            start += chunk_size - overlap
        return chunks

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Gera vetores para múltiplos chunks baixados do S3.
        """
        document_id = body.get('document_id')

        if not document_id:
            logger.error("Mensagem malformada: document_id ausente.")
            return False

        try:
            # 1. Buscar metadados para obter o link do arquivo parseado no S3
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            cur.execute("SELECT metadata FROM documents WHERE id = %s", (document_id,))
            row = cur.fetchone()
            
            if not row or not row[0]:
                logger.error(f"Metadados de {document_id} não encontrados.")
                return False
            
            metadata = row[0]
            parsed_s3_key = metadata.get('parsed_s3')
            parsed_bucket = os.getenv("MINIO_BUCKET_PARSED", "petroscan-parsed")

            if not parsed_s3_key:
                logger.error(f"Atenção: parsed_s3_key ausente nos metadados de {document_id}.")
                return False

            # 2. Download do conteúdo Markdown do MinIO
            logger.info(f"Baixando conteúdo extraído do S3 ({parsed_s3_key})...")
            response = self.s3_client.get_object(Bucket=parsed_bucket, Key=parsed_s3_key)
            full_content = response['Body'].read().decode('utf-8')

            # 3. Fragmentar o texto (Chunking) e Gerar Embeddings
            chunks = self._chunk_text(full_content)
            logger.info(f"Vetorizando {len(chunks)} chunks para o documento {document_id}...")

            # Limpar chunks antigos se estiver re-processando
            cur.execute("DELETE FROM document_chunks WHERE document_id = %s", (document_id,))

            for i, chunk_content in enumerate(chunks):
                embedding = self.model.encode(chunk_content).tolist()
                
                cur.execute(
                    """
                    INSERT INTO document_chunks (document_id, content, chunk_index, embedding)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (document_id, chunk_content, i, embedding)
                )
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.info(f"Processamento de {len(chunks)} embeddings concluído para {document_id}.")
            return True

        except Exception as e:
            logger.error(f"Erro no processamento de embeddings: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = EmbeddingWorker()
    worker.run()
