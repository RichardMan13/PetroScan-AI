import os
import redis
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BaseWorker")

class BaseWorker(ABC):
    """
    Classe base para todos os workers do ecossistema PetroScan-AI.
    Implementa a conexão ao Redis para consumo de tarefas.
    """

    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.host = os.getenv("REDIS_HOST", "127.0.0.1")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.redis_client: Optional[redis.Redis] = None
        self._running = False

    def connect(self) -> None:
        """Estabelece conexão com o Redis."""
        self.redis_client = redis.Redis(host=self.host, port=self.port, decode_responses=False)
        self.redis_client.ping()
        logger.info(f"Worker conectado à fila Redis: {self.queue_name}")

    @abstractmethod
    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Método abstrato que deve ser implementado pelo Worker especialista.
        Retorna True se o processamento foi bem-sucedido, False caso contrário.
        """
        pass

    def run(self) -> None:
        """Inicia o loop de consumo de mensagens."""
        if not self.redis_client:
            self.connect()
        
        self._running = True
        logger.info(f"Aguardando mensagens na fila [{self.queue_name}]... Pressione CTRL+C para sair.")
        try:
            while self._running:
                # Usa brpop para bloquear até que uma mensagem chegue
                message = self.redis_client.brpop(self.queue_name, timeout=1)
                if message:
                    _, body = message
                    
                    try:
                        data = json.loads(body.decode())
                        logger.info(f"Processando nova tarefa para o documento: {data.get('document_id', 'N/A')}")
                        
                        success = self.process_task(data)
                        
                        if success:
                            logger.info(f"Tarefa concluída com sucesso.")
                        else:
                            logger.error(f"Falha no processamento. Movendo para inspeção humana.")
                            # Adiciona à fila de erro (DLQ simplificada)
                            self.redis_client.lpush("human.review", body)
                            
                    except Exception as e:
                        logger.error(f"Erro crítico no worker: {str(e)}")
                        self.redis_client.lpush("human.review", body)
                        
        except KeyboardInterrupt:
            logger.info("Worker interrompido pelo usuário.")
            self.stop()

    def stop(self) -> None:
        """Encerra a execução e desconecta do Redis."""
        self._running = False
        if self.redis_client:
            self.redis_client.close()
            logger.info("Conexão com Redis fechada.")
