import os
import torch
import logging
import psycopg2
from PIL import Image
from typing import Dict, Any
from transformers import CLIPProcessor, CLIPModel
import boto3
from tempfile import NamedTemporaryFile

# Importar o BaseWorker
from base_worker import BaseWorker

logger = logging.getLogger("CLIPWorker")

class CLIPWorker(BaseWorker):
    """
    Worker especialista em Embeddings Multimodais (CLIP).
    Utilizado para indexação visual de símbolos em P&IDs e busca por similaridade de imagem.
    """

    def __init__(self, model_name: str = "openai/clip-vit-base-patch32"):
        # A fila que este worker escuta (Fase 3 do README)
        super().__init__(queue_name="task.clip")
        
        # Conexão com Postgres
        self.db_params = {
            'host': 'localhost',
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }

        # Conexão com S3 (MinIO)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

        # Configuração de dispositivo (GPU/CPU)
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Carregando CLIP no dispositivo: {self.device}...")
        
        # Inicializa o Processor e Modelo
        self.processor = CLIPProcessor.from_pretrained(model_name)
        self.model = CLIPModel.from_pretrained(model_name).to(self.device)
        logger.info("Modelo CLIP carregado com sucesso.")

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Gera o vetor visual (embedding) para um snippet de imagem.
        Esperado no corpo: { 's3_key': 'snippet_valve.png', 'entity_id': '...' }
        """
        s3_key = body.get('s3_key')
        entity_id = body.get('entity_id')

        if not s3_key or not entity_id:
            logger.error("Mensagem malformada: s3_key ou entity_id ausentes.")
            return False

        try:
            # 1. Download do snippet de imagem (ex: recorte de símbolo) do MinIO
            with NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
                self.s3_client.download_file(os.getenv("MINIO_BUCKET_SNIPPETS", "petroscan-snippets"), s3_key, tmp_file.name)
                temp_path = tmp_file.name

            # 2. Carregar Imagem
            image = Image.open(temp_path).convert("RGB")
            
            # 3. Gerar Embedding Visual via CLIP
            logger.info(f"Gerando embedding visual para a entidade: {entity_id}...")
            inputs = self.processor(images=image, return_tensors="pt").to(self.device)
            
            with torch.no_grad():
                image_features = self.model.get_image_features(**inputs)
                # Normalizar o vetor para busca por similaridade de cosseno
                image_features /= image_features.norm(dim=-1, keepdim=True)
                visual_embedding = image_features.cpu().numpy()[0].tolist()

            # 4. Salvar na tabela 'entities' do PostgreSQL
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            cur.execute(
                "UPDATE entities SET visual_embedding = %s WHERE id = %s",
                (visual_embedding, entity_id)
            )
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Limpeza
            os.unlink(temp_path)
            logger.info(f"Vetor visual da entidade {entity_id} processado e salvo com sucesso.")
            return True

        except Exception as e:
            logger.error(f"Erro no processamento multmodal CLIP: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = CLIPWorker()
    worker.run()
