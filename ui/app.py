import streamlit as st
import os
import psycopg2
import boto3
import json
import pandas as pd
from datetime import datetime
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# 1. CONFIGURAÇÃO DA PÁGINA
st.set_page_config(
    page_title="PetroScan-AI | Document Intelligence",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. ESTILIZAÇÃO CUSTOMIZADA
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;700;800&display=swap');
    html, body, [class*="css"]  { font-family: 'Outfit', sans-serif; }
    
    .stMetric {
        background: rgba(255, 255, 255, 0.05);
        padding: 15px;
        border-radius: 12px;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    
    .main-title {
        background: linear-gradient(135deg, #00f2fe 0%, #4facfe 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3.5rem;
        font-weight: 800;
        margin-bottom: 0px;
    }
    
    .result-card {
        background: linear-gradient(145deg, #1e2130, #161924);
        padding: 20px;
        border-radius: 16px;
        border-left: 6px solid #4facfe;
        margin-bottom: 15px;
        transition: all 0.3s ease;
    }
    
    .tag {
        background: #4facfe;
        color: white;
        padding: 4px 12px;
        border-radius: 50px;
        font-size: 0.75rem;
        font-weight: 700;
        text-transform: uppercase;
    }
</style>
""", unsafe_allow_html=True)

# 3. CARREGAR VARIÁVEIS E CONEXÕES
load_dotenv()

@st.cache_resource
def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        database=os.getenv("POSTGRES_DB", "petroscan_db"),
        user=os.getenv("POSTGRES_USER", "petroscan_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "petroscan_secure_pwd"),
        port=5432
    )

@st.cache_resource
def get_embedding_model():
    return SentenceTransformer("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "petroscan_root"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "petroscan_secret_key"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )

def get_page_image(bucket, key):
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except: return None

# 4. ESTADO DA SESSÃO
if 'selected_doc' not in st.session_state:
    st.session_state.selected_doc = None

# 5. SIDEBAR
with st.sidebar:
    st.markdown("<h2 style='color:#4facfe;'>PetroScan AI</h2>", unsafe_allow_html=True)
    st.divider()
    
    # Status Visual dos Workers
    st.markdown("🚦 **Worker Status**")
    for w in ["Ingestion", "Layout Vision", "Vector Search"]:
        cols = st.columns([1, 5])
        cols[0].markdown("🟢")
        cols[1].markdown(f"{w} Active")
    
    st.divider()
    if st.session_state.selected_doc:
        if st.button("⬅️ Voltar para a Busca", use_container_width=True):
            st.session_state.selected_doc = None
            st.rerun()

# 6. ABAS PRINCIPAIS
tab_search, tab_audit = st.tabs(["🔍 Busca Inteligente", "📊 Audit Trail & Monitoramento"])

# --- ABA DE BUSCA ---
with tab_search:
    if st.session_state.selected_doc:
        doc = st.session_state.selected_doc
        st.markdown(f"<h1 style='font-size:2.5rem; color:#4facfe;'>📄 {doc['title']}</h1>", unsafe_allow_html=True)
        st.markdown(f"**{doc['category'].upper()}** | Página {doc['page_number']}")
        st.divider()
        
        col1, col2 = st.columns([4, 6])
        with col1:
            st.markdown("### 🖼 Página Original")
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT s3_key FROM document_pages WHERE document_id = %s AND page_number = %s", 
                            (doc['document_id'], doc['page_number']))
                page_data = cur.fetchone()
                if page_data:
                    img_data = get_page_image("petroscan-pages", page_data[0])
                    if img_data: st.image(img_data, use_container_width=True)
                cur.close()
            except: pass

        with col2:
            st.markdown("### 📝 Conteúdo Granular")
            st.markdown(f"""<div style="background: rgba(255,255,255,0.03); padding: 25px; border-radius: 12px; line-height: 1.6; border: 1px solid rgba(255,255,255,0.05);">
                {doc['content']}</div>""", unsafe_allow_html=True)
            st.json({"Worker": "LayoutLMv3", "Score": f"{doc['score']:.4f}"})

    else:
        st.markdown('<p class="main-title">PetroScan-AI</p>', unsafe_allow_html=True)
        st.markdown('<p style="color:#8a8da4; font-size:1.2rem; margin-top:-10px;">Document Intelligence Engine</p>', unsafe_allow_html=True)

        query = st.text_input("", placeholder="🔍 O que você procura? (ex: normas de permutadores, segurança em FPSO...)")

        if query:
            try:
                model = get_embedding_model()
                conn = get_db_connection()
                cur = conn.cursor()
                query_embedding = model.encode(query).tolist()
                cur.execute("""
                    SELECT c.content, d.title, d.category, c.page_number, 1 - (c.embedding <=> %s::vector) as similarity, d.id
                    FROM document_chunks c JOIN documents d ON c.document_id = d.id
                    ORDER BY similarity DESC LIMIT 5;
                """, (query_embedding,))
                results = cur.fetchall()
                for content, title, category, page, score, doc_id in results:
                    st.markdown(f"""<div class="result-card"><span class="tag">{category}</span>
                        <h3 style="color:white; margin-top:10px;">{title}</h3>
                        <p style="color:#8a8da4;">Página {page} | Confiança: {score*100:.1f}%</p>
                        <p style="color:#d1d1d1;">{content[:400]}...</p></div>""", unsafe_allow_html=True)
                    if st.button("👁️ Visualizar Detalhes Side-by-Side", key=f"btn_{doc_id}_{page}_{score}"):
                        st.session_state.selected_doc = {"document_id": doc_id, "title": title, "category": category, "page_number": page, "content": content, "score": score}
                        st.rerun()
                cur.close()
            except Exception as e: st.error(f"Erro: {e}")
        else:
            st.markdown("### 📑 Documentos Recentes")
            try:
                conn = get_db_connection()
                cur = conn.cursor()
                cur.execute("SELECT title, category, id FROM documents ORDER BY created_at DESC LIMIT 4")
                docs = cur.fetchall()
                if docs:
                    cols = st.columns(len(docs))
                    for i, (title, cat, doc_id) in enumerate(docs):
                        with cols[i]:
                            st.info(f"**{title[:20]}...**\n\n_{cat}_")
                            if st.button(f"Abrir Page 1", key=f"recent_{doc_id}"):
                                st.session_state.selected_doc = {"document_id": doc_id, "title": title, "category": cat, "page_number": 1, "content": "Visualização prévia do documento.", "score": 1.0}
                                st.rerun()
                cur.close()
            except: pass

# --- ABA DE AUDITORIA (NOVO) ---
with tab_audit:
    st.markdown("## 📊 Audit Trail & Monitoramento do Sistema")
    st.markdown("Rastreabilidade total do processamento de documentos técnicos.")
    
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # 1. Métricas de Resumo
        cur.execute("SELECT COUNT(*) FROM documents")
        total_docs = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM document_pages")
        total_pages = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM document_chunks")
        total_chunks = cur.fetchone()[0]
        
        m_col1, m_col2, m_col3 = st.columns(3)
        m_col1.metric("Documentos Totais", total_docs)
        m_col2.metric("Páginas Analisadas (Layout)", total_pages)
        m_col3.metric("Fragmentos Vetorizados", total_chunks)
        
        st.divider()
        
        # 2. Tabela Detalhada de Auditoria
        st.markdown("### 🕒 Fluxo de Processamento Recente")
        cur.execute("""
            SELECT 
                d.created_at as data,
                d.title as documento,
                d.category as tipo,
                (SELECT COUNT(*) FROM document_pages WHERE document_id = d.id) as paginas,
                (SELECT COUNT(*) FROM document_chunks WHERE document_id = d.id) as fragmentos,
                '✅ CONCLUÍDO' as status
            FROM documents d
            ORDER BY d.created_at DESC
        """)
        audit_data = cur.fetchall()
        df = pd.DataFrame(audit_data, columns=["Data", "Documento", "Tipo", "Páginas", "Fragmentos", "Status"])
        st.dataframe(df, use_container_width=True, hide_index=True)
        
        # 3. Gráfico de Distribuição por Categoria
        if not df.empty:
            st.markdown("### 📈 Distribuição por Categoria")
            category_counts = df['Tipo'].value_counts()
            st.bar_chart(category_counts)
            
        cur.close()
    except Exception as e:
        st.error(f"Erro ao carregar Audit Trail: {e}")

st.divider()
st.caption("PetroScan-AI v1.0 | Audit Trail & Semantic Search System")
