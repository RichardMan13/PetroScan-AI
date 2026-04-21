import os
import psycopg2
import logging
from sentence_transformers import SentenceTransformer
from typing import List, Dict

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("EvaluateSearch")

# 1. DEFINIÇÃO DO GOLDEN DATASET (Perguntas vs IDs de Documentos Esperados)
# Aqui você define o que a IA DEVE encontrar.
GOLDEN_DATASET = [
    {
        "query": "Como deve ser feita a troca de calor em permutadores?",
        "expected_doc_title": "Heat Exchanges.pdf"
    },
    {
        "query": "Quais as simbologias para diagramas de tubulação?",
        "expected_doc_title": "Simplified P&ID.pdf"
    }
]

class SearchEvaluator:
    def __init__(self):
        self.model = SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        self.db_params = {
            'host': os.getenv("POSTGRES_HOST", "localhost"),
            'database': os.getenv("POSTGRES_DB", "petroscan_db"),
            'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
            'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
            'port': os.getenv("POSTGRES_PORT", "5433")
        }

    def evaluate_recall(self, k: int = 3):
        """Calcula o Recall@K para o dataset de ouro."""
        logger.info(f"Iniciando avaliação de Recall@{k}...")
        
        hits = 0
        total = len(GOLDEN_DATASET)
        
        try:
            conn = psycopg2.connect(**self.db_params)
            cur = conn.cursor()
            
            for item in GOLDEN_DATASET:
                query = item["query"]
                expected = item["expected_doc_title"]
                
                # Gerar Vector
                query_vec = self.model.encode(query).tolist()
                
                # Buscar no Banco
                cur.execute("""
                    SELECT d.title, 1 - (c.embedding <=> %s::vector) as sim
                    FROM document_chunks c
                    JOIN documents d ON c.document_id = d.id
                    ORDER BY sim DESC
                    LIMIT %s;
                """, (query_vec, k))
                
                results = [row[0] for row in cur.fetchall()]
                
                if expected in results:
                    logger.info(f" HIT: '{query}' -> Encontrou '{expected}' no Top-{k}")
                    hits += 1
                else:
                    logger.warning(f" MISS: '{query}' -> Não encontrou '{expected}' no Top-{k}. Resultados: {results}")
            
            recall = hits / total
            logger.info("=" * 40)
            logger.info(f"RESULTADO FINAL RECALL@{k}: {recall * 100:.2f}%")
            logger.info("=" * 40)
            
            cur.close()
            conn.close()
            return recall
            
        except Exception as e:
            logger.error(f"Erro na avaliação: {e}")
            return 0

if __name__ == "__main__":
    evaluator = SearchEvaluator()
    evaluator.evaluate_recall(k=3)
