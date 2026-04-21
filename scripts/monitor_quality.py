import os
import psycopg2
import pandas as pd
import logging
from evidently import Report
from evidently.presets import DataDriftPreset, DataSummaryPreset
from evidently import metrics

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("QualityMonitor")

def run_evidently_report():
    logger.info("Coletando dados para relatório de qualidade (Evidently AI)...")
    
    db_params = {
        'host': os.getenv("POSTGRES_HOST", "localhost"),
        'database': os.getenv("POSTGRES_DB", "petroscan_db"),
        'user': os.getenv("POSTGRES_USER", "petroscan_admin"),
        'password': os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
        'port': os.getenv("POSTGRES_PORT", "5433")
    }

    try:
        conn = psycopg2.connect(**db_params)
        
        # 1. Carregar dados atuais de chunks
        query = """
            SELECT char_length(content) as document_length,
                   (SELECT COUNT(*) FROM document_pages WHERE document_id = document_chunks.document_id) as pages_count
            FROM document_chunks 
            ORDER BY created_at DESC LIMIT 100
        """
        curr_data = pd.read_sql(query, conn)
        
        if curr_data.empty:
            logger.warning("Nenhum dado encontrado para monitoramento.")
            return

        # 2. Criar Relatório de Visão Geral de Texto
        # Comparamos o dataset atual com ele mesmo ou com uma referência anterior se existisse
        report = Report(metrics=[
            DataSummaryPreset(),
            DataDriftPreset()
        ])
        
        snapshot = report.run(reference_data=curr_data, current_data=curr_data)
        
        # 3. Salvar Relatório
        report_path = "data/reports/quality_report.html"
        os.makedirs("data/reports", exist_ok=True)
        snapshot.save_html(report_path)
        
        logger.info("=" * 40)
        logger.info(f"✅ RELATÓRIO DE QUALIDADE GERADO: {report_path}")
        logger.info("=" * 40)
        
        conn.close()
    except Exception as e:
        logger.error(f"Erro ao gerar relatório: {e}")

if __name__ == "__main__":
    run_evidently_report()
