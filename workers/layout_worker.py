import os
import torch
import logging
import json
import psycopg2
from PIL import Image
from typing import Dict, Any
from transformers import LayoutLMv3Processor, LayoutLMv3ForTokenClassification
import boto3
from tempfile import NamedTemporaryFile

# Importar o BaseWorker
from base_worker import BaseWorker

logger = logging.getLogger("LayoutWorker")

class LayoutWorker(BaseWorker):
    """
    Worker especialista em análise de Layout (Computer Vision).
    Utiliza LayoutLMv3 para identificar blocos de tabelas, símbolos e texto em P&IDs e Normas.
    """

    def __init__(self, model_name: str = "microsoft/layoutlmv3-base"):
        # A fila que este worker escuta (Fase 3 do README)
        super().__init__(queue_name="task.layout")
        
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

        # Configuração de dispositivo
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Carregando LayoutLMv3 no dispositivo: {self.device}...")
        
        # Inicializa Processor e Modelo
        self.processor = LayoutLMv3Processor.from_pretrained(model_name, apply_ocr=True) # docling já faz o OCR, mas mantemos flexible
        self.model = LayoutLMv3ForTokenClassification.from_pretrained(model_name).to(self.device)
        logger.info("Modelo LayoutLMv3 carregado com sucesso.")

    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Executa a análise de layout em uma página de documento.
        Esperado no corpo: { 's3_key': 'doc_page.png', 'bucket': '...', 'document_id': '...' }
        """
        s3_key = body.get('s3_key')
        bucket = body.get('bucket', 'petroscan-docs')
        document_id = body.get('document_id')

        if not s3_key or not document_id:
            logger.error("Mensagem malformada: s3_key ou document_id ausentes.")
            return False

        try:
            # 1. Download da página (imagem) do MinIO
            with NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
                self.s3_client.download_file(bucket, s3_key, tmp_file.name)
                temp_path = tmp_file.name

            # 2. Carregar Imagem
            image = Image.open(temp_path).convert("RGB")
            
            # 3. Processar via LayoutLMv3
            # O modelo analisa Imagem + Coordenadas de Texto de forma multimodal
            logger.info(f"Iniciando análise de layout real para: {s3_key}...")
            
            # Realizar a predição (Inference)
            encoding = self.processor(image, return_tensors="pt").to(self.device)
            with torch.no_grad():
                outputs = self.model(**encoding)
            
            # --- LÓGICA DE DECODIFICAÇÃO REAL ---
            # Obter a predição para cada token (ex: 0=Text, 8=Table)
            predictions = outputs.logits.argmax(-1).squeeze().tolist()
            token_boxes = encoding.bbox.squeeze().tolist()
            
            # Mapeamento de IDs para nomes de labels (Ex: 'table', 'header')
            id2label = self.model.config.id2label
            
            # Dimensões para desnormalização (LayoutLMv3 usa grade 0-1000)
            width, height = image.size
            
            detected_entities = []
            for i, pred_id in enumerate(predictions):
                label = id2label[pred_id].lower()
                
                # Filtrar o que é relevante para o Golden Join (Ignorar o que não é estrutural)
                if label in ["table", "header", "figure", "symbol", "caption"]:
                    box = token_boxes[i]
                    
                    # Converter de 1000x1000 para escala real do documento original
                    real_box = {
                        "x1": int((box[0] * width) / 1000),
                        "y1": int((box[1] * height) / 1000),
                        "x2": int((box[2] * width) / 1000),
                        "y2": int((box[3] * height) / 1000)
                    }
                    detected_entities.append((label, real_box))

            # 4. Salvar Entidades/Blocos reais detectados no Postgres
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            # Log de detecção
            logger.info(f"Detectadas {len(detected_entities)} entidades estruturais na página.")
            
            for label, box in detected_entities:
                query = """
                    INSERT INTO entities (document_id, tag, entity_type, bounding_box, confidence)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                cur.execute(query, (
                    document_id,
                    f"DETECTED_{label.upper()}", # Tag provisória (OCR em cima disso virá depois)
                    label,
                    json.dumps(box), 
                    0.95 # Confiança fixa para o MVP, no real viria do softmax
                ))
            
            conn.commit()
            cur.close()
            conn.close()
            
            # Limpeza
            os.unlink(temp_path)
            logger.info(f"Análise de layout e persistência geométrica concluída para {document_id}.")
            return True

        except Exception as e:
            logger.error(f"Erro na análise de layout: {str(e)}")
            return False

if __name__ == "__main__":
    # Inicializa e roda o worker
    worker = LayoutWorker()
    worker.run()
