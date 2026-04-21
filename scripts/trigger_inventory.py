import os
import redis
import json
import boto3
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TriggerInventory")

def trigger_inventory(local_file_path: str):
    """
    Simula o upload de uma planilha de inventário e dispara a tarefa de ETL.
    """
    if not os.path.exists(local_file_path):
        logger.error(f"Arquivo de inventário não encontrado: {local_file_path}")
        return

    # 1. Configurar MinIO e Redis
    bucket_name = "petroscan-inventory"
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

    # Garantir bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except Exception:
        pass

    # 2. Upload
    s3_key = os.path.basename(local_file_path)
    logger.info(f"Fazendo upload do inventário {s3_key} para o MinIO...")
    s3_client.upload_file(local_file_path, bucket_name, s3_key)

    # 3. Disparar no Redis
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_client = redis.Redis(host=redis_host, port=redis_port)

    message = {
        "s3_key": s3_key,
        "bucket": bucket_name
    }

    redis_client.lpush("task.inventory", json.dumps(message))

    logger.info(f"Tarefa de INVENTÁRIO disparada para {s3_key}")
    redis_client.close()

if __name__ == "__main__":
    # Ajuste o caminho se tiver um CSV/XLSX de teste
    test_inventory = r"d:\repositorios_pessoais\PetroScan-AI\data\inventory\test_inventory.csv"
    if os.path.exists(test_inventory):
        trigger_inventory(test_inventory)
    else:
        print(f"Caminho padrão não encontrado: {test_inventory}")
        path = input("Digite o caminho completo do arquivo de inventário (CSV/XLSX): ")
        if path:
            trigger_inventory(path)
