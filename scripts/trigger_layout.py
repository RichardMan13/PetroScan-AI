import os
import redis
import json
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TriggerLayout")

def trigger_layout(document_id: str, s3_key: str, bucket: str = "petroscan-docs"):
    """
    Dispara a tarefa de análise de Layout para uma imagem/página específica no S3.
    """
    # 1. Conectar ao Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    message = {
        "document_id": document_id,
        "s3_key": s3_key,
        "bucket": bucket
    }

    # 2. Publicar na fila task.layout
    redis_client.lpush("task.layout", json.dumps(message))

    logger.info(f"Tarefa de LAYOUT disparada para a página {s3_key} (Doc: {document_id})")
    redis_client.close()

if __name__ == "__main__":
    print("--- Trigger Layout Worker ---")
    doc_id = input("Digite o UUID do documento: ")
    s3_key = input("Digite a chave (S3 Key) da imagem/página: ")
    
    if doc_id and s3_key:
        trigger_layout(doc_id, s3_key)
