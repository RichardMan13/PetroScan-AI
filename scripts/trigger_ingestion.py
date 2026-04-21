import os
import redis
import json
import boto3
import uuid
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TriggerIngestion")

def trigger_ingestion(local_file_path: str):
    """
    Simula o upload de um documento e dispara a tarefa no Redis.
    """
    if not os.path.exists(local_file_path):
        logger.error(f"Arquivo não encontrado: {local_file_path}")
        return

    # 1. Configurar MinIO e Redis
    bucket_name = "petroscan-docs"
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

    # 2. Garantir que o bucket existe
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        # Também criamos o bucket de 'parsed docs' necessário para a refatoração que fizemos
        s3_client.create_bucket(Bucket="petroscan-parsed")
    except Exception:
        pass # Bucket já existe

    # 3. Gerar ID Único e fazer Upload
    document_id = str(uuid.uuid4())
    s3_key = os.path.basename(local_file_path)
    
    logger.info(f"Fazendo upload de {local_file_path} para o MinIO...")
    s3_client.upload_file(local_file_path, bucket_name, s3_key)

    # 4. Enviar mensagem para o Redis
    redis_host = os.getenv("REDIS_HOST", "127.0.0.1")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    message = {
        "document_id": document_id,
        "bucket": bucket_name,
        "s3_key": s3_key
    }

    # Publicar na rota da Ingestão
    redis_client.lpush("task.ingestion", json.dumps(message))

    logger.info(f"Tarefa de ingestão disparada para {s3_key} (ID: {document_id})")
    redis_client.close()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
    else:
        # Padrão caso nenhum argumento seja passado
        test_file = r"d:\repositorios_pessoais\PetroScan-AI\data\raw\Resolução 916 2023 da ANP BR.pdf"
    
    trigger_ingestion(test_file)
