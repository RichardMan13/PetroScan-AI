import os
import torch
import logging
import json
import psycopg2
from PIL import Image
from typing import Dict, Any
from transformers import TrOCRProcessor, VisionEncoderDecoderModel
import boto3
from tempfile import NamedTemporaryFile

# Importar o BaseWorker
from base_worker import BaseWorker

logger = logging.getLogger("OCRWorker")

class OCRWorker(BaseWorker):
    """
    Worker especialista em HTR (Handwritten Text Recognition).
    Utiliza o modelo TrOCR para converter caligrafia em texto digital.
    """

    def __init__(self, model_name: str = "microsoft/trocr-base-handwritten"):
        # A fila que este worker escuta
        super().__init__(queue_name="task.ocr")
        
        # Conexão com S3 (MinIO)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )

        # Conexão com Postgres (Para salvar as anotações manuscritas)
        self.db_params = {
            'host': 'localhost',
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': int(os.getenv("POSTGRES_PORT", 5432))
        }

        # Verificar se há GPU disponível
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Carregando TrOCR no dispositivo: {self.device}...")
        
        # Inicializa o Processor e Modelo
        self.processor = TrOCRProcessor.from_pretrained(model_name)
        self.model = VisionEncoderDecoderModel.from_pretrained(model_name).to(self.device)
        logger.info("Modelo TrOCR carregado com sucesso.")

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Executa a transcrição de um snippet de imagem e salva no banco de dados.
        Esperado no corpo: { 's3_key': 'snippet.png', 'bucket': '...', 'document_id': '...' }
        """
        s3_key = body.get('s3_key')
        bucket = body.get('bucket', 'petroscan-docs')
        document_id = body.get('document_id')

        if not s3_key or not document_id:
            logger.error("Mensagem malformada: s3_key ou document_id ausentes.")
            return False

        try:
            # 1. Download do snippet de imagem do MinIO
            with NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
                self.s3_client.download_file(bucket, s3_key, tmp_file.name)
                temp_path = tmp_file.name

            # 2. Carregar e Processar
            image = Image.open(temp_path).convert("RGB")
            pixel_values = self.processor(images=image, return_tensors="pt").pixel_values.to(self.device)

            # 3. Gerar a transcrição via TrOCR
            logger.info(f"Iniciando HTR para o snippet: {s3_key}...")
            with torch.no_grad():
                generated_ids = self.model.generate(pixel_values)
                generated_text = self.processor.batch_decode(generated_ids, skip_special_tokens=True)[0]

            logger.info(f"Transcrição concluída: [{generated_text}]")

            # 4. PERSISTÊNCIA REAL NO BANCO DE DADOS
            # O texto manuscrito se torna um novo fragmento (chunk) do documento pai
            logger.info(f"Persistindo anotação manuscrita para o documento {document_id}...")
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            metadata = {
                "source_type": "handwritten_ocr",
                "source_image": s3_key,
                "engine": "TrOCR"
            }
            
            cur.execute("""
                INSERT INTO document_chunks (document_id, content, metadata)
                VALUES (%s, %s, %s)
            """, (
                document_id, 
                f"[HTR]: {generated_text}", 
                json.dumps(metadata)
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Limpeza
            os.unlink(temp_path)
            logger.info(f"Nota manuscrita do documento {document_id} salva com sucesso.")
            return True

        except Exception as e:
            logger.error(f"Erro no processamento de OCR/HTR: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = OCRWorker()
    worker.run()
