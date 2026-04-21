import os
import redis
import json
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TriggerEmbedding")

def trigger_embedding(document_id: str):
    """
    Dispara a tarefa de vetorização (Embedding) para um documento já ingerido.
    """
    # 1. Conectar ao Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    message = {
        "document_id": document_id
    }

    # 2. Publicar na fila task.embedding
    redis_client.lpush("task.embedding", json.dumps(message))

    logger.info(f"Tarefa de EMBEDDING disparada para o documento ID: {document_id}")
    redis_client.close()

if __name__ == "__main__":
    # Substitua pelo ID que você viu no banco de dados ou no log do Ingestion
    doc_id = input("Digite o UUID do documento para vetorizar: ")
    if doc_id:
        trigger_embedding(doc_id)
