import os
import psycopg2
import logging
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("VerifyEmbeddings")

def verify_db_state():
    """
    Consulta o PostgreSQL para verificar se os documentos e embeddings foram salvos corretamente.
    """
    host = "127.0.0.1"
    port = os.getenv("POSTGRES_PORT", 5433)
    db = os.getenv("POSTGRES_DB", "petroscan_db")
    user = os.getenv("POSTGRES_USER", "petroscan_admin")
    pwd = os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd")

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=pwd
        )
        cur = conn.cursor()

        print("\n" + "="*60)
        print("🔍 AUDITORIA DE DADOS: PETROSCAN-AI")
        print("="*60)

        # 1. Verificar Documentos Ingeridos
        cur.execute("SELECT id, title, category, created_at FROM documents;")
        docs = cur.fetchall()
        
        print(f"\nDOCUMENTOS NO BANCO ({len(docs)}):")
        if not docs:
            print("   (Nenhum documento encontrado)")
        for doc in docs:
            print(f"   - ID: {doc[0]} | Título: {doc[1]} | Tipo: {doc[2]}")

        # 2. Verificar Fragmentos (Chunks) e Vetores
        cur.execute("""
            SELECT d.title, COUNT(c.id) as total_chunks 
            FROM documents d
            LEFT JOIN document_chunks c ON d.id = c.document_id
            GROUP BY d.title;
        """)
        stats = cur.fetchall()
        
        print(f"\nESTATÍSTICAS DE VETORIZAÇÃO:")
        for stat in stats:
            status = "VETORIZADO" if stat[1] > 0 else "AGUARDANDO EMBEDDING"
            print(f"   - {stat[0]}: {stat[1]} fragmentos {status}")

        # 3. Amostra de Vetor (Prova Real)
        cur.execute("SELECT content, embedding FROM document_chunks LIMIT 1;")
        sample = cur.fetchone()
        if sample:
            print(f"\nAMOSTRA DE VETOR (Prova Técnica):")
            print(f"   - Texto: {sample[0][:70]}...")
            # Pega as primeiras 5 dimensões do vetor
            vector_preview = sample[1][:5] if sample[1] else "N/A"
            print(f"   - Embedding (dim 0-5): {vector_preview}")
        
        print("\n" + "="*60 + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Erro ao conectar ou consultar o banco: {e}")

if __name__ == "__main__":
    verify_db_state()
