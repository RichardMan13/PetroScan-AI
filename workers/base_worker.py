import os
import pika
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
    Implementa a conexão resiliente e a lógica de Dead Letter Exchange (DLX).
    """

    def __init__(self, queue_name: str, exchange_name: str = "petroscan_exchange"):
        self.queue_name = queue_name
        self.exchange_name = exchange_name
        self.dlx_exchange = f"dlx_{exchange_name}"
        self.dlx_queue = "human.review"
        
        self.host = os.getenv("RABBITMQ_HOST", "localhost")
        self.port = int(os.getenv("RABBITMQ_PORT", 5672))
        self.user = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
        self.password = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")
        
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def connect(self) -> None:
        """Estabelece conexão com o RabbitMQ e configura as filas/exchanges."""
        credentials = pika.PlainCredentials(self.user, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        
        # 1. Declarar a Dead Letter Exchange e sua Fila
        self.channel.exchange_declare(exchange=self.dlx_exchange, exchange_type='direct')
        self.channel.queue_declare(queue=self.dlx_queue, durable=True)
        self.channel.queue_bind(exchange=self.dlx_exchange, queue=self.dlx_queue, routing_key=self.queue_name)
        
        # 2. Declarar a Main Exchange
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
        
        # 3. Declarar a Fila Principal com argumentos de DLX e política de retry (max 3x)
        # Nota: O TTL ou Rejeição direcionará automaticamente para a dlx_exchange
        args = {
            'x-dead-letter-exchange': self.dlx_exchange,
            'x-dead-letter-routing-key': self.queue_name
        }
        
        self.channel.queue_declare(queue=self.queue_name, durable=True, arguments=args)
        self.channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.queue_name)
        
        # Fair dispatch (consumir apenas 1 por vez para não sobrecarregar GPU)
        self.channel.basic_qos(prefetch_count=1)
        
        logger.info(f"Worker conectado à fila: {self.queue_name} (Exchange: {self.exchange_name})")

    @abstractmethod
    def process_task(self, body: Dict[str, Any]) -> bool:
        """
        Método abstrato que deve ser implementado pelo Worker especialista.
        Retorna True se o processamento foi bem-sucedido, False caso contrário.
        """
        pass

    def on_message(self, ch, method, properties, body: bytes) -> None:
        """Callback acionado quando uma mensagem chega na fila."""
        try:
            data = json.loads(body.decode())
            logger.info(f"Processando nova tarefa para o documento: {data.get('document_id', 'N/A')}")
            
            success = self.process_task(data)
            
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logger.info(f"Tarefa concluída com sucesso (ack).")
            else:
                # Rejeita e envia para a fila de erro (DLX)
                logger.error(f"Falha no processamento. Movendo para DLX.")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                
        except Exception as e:
            logger.error(f"Erro crítico no worker: {str(e)}")
            # Em caso de erro de parse/crítico, movemos para DLX imediatamente
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def run(self) -> None:
        """Inicia o loop de consumo de mensagens."""
        if not self.channel:
            self.connect()
        
        logger.info(f"Aguardando mensagens na fila [{self.queue_name}]... Pressione CTRL+C para sair.")
        try:
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.on_message
            )
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Worker interrompido pelo usuário.")
            self.stop()

    def stop(self) -> None:
        """Fecha a conexão com o RabbitMQ com segurança."""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Conexão com RabbitMQ fechada.")
