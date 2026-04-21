-- ============================================================================
-- PETROSCAN-AI: SCHEMA DE BANCO DE DADOS (POSTGRESQL + PGVECTOR)
-- Missão: Golden Join entre Normas (Unstructured), P&ID (Semi) e Inventário (Structured)
-- ============================================================================

-- 1. EXTENSÕES
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- Para Fuzzy Matching em TAGs

-- 2. TABELAS CORE

-- Tabela de Documentos (Normas N-XXXX, Regulamentos ANP/IBP)
CREATE TABLE IF NOT EXISTS documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    category TEXT, -- Norm, Resolution, Manual
    version TEXT,
    source_url TEXT,
    metadata JSONB, -- Contém links para o storage S3 (original e parsed)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Páginas (Para Análise de Layout e OCR)
CREATE TABLE IF NOT EXISTS document_pages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    page_number INTEGER NOT NULL,
    s3_key TEXT, -- Link para a imagem PNG da página
    layout_results JSONB, -- Blocos detectados (LayoutLMv3)
    ocr_text TEXT, -- Texto consolidado da página
    status TEXT DEFAULT 'pending', -- pending, processing, completed, error
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_id, page_number)
);


-- Tabela de Chunks (Fragmentos para Busca Semântica Granular)
CREATE TABLE IF NOT EXISTS document_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    content TEXT NOT NULL, -- O parágrafo/trecho específico
    page_number INTEGER,
    chunk_index INTEGER,
    embedding VECTOR(384), -- Vetor para Busca Semântica (all-MiniLM-L6-v2)
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Equipamentos/Tags extraídos Visualmente (P&IDs)
CREATE TABLE IF NOT EXISTS entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id),
    tag TEXT NOT NULL, -- Ex: P-101-A
    entity_type TEXT, -- Pump, Valve, Vessel
    bounding_box JSONB, -- Coordenadas (x, y, w, h)
    confidence REAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de Inventário Estruturado (Dados mestre da planta)
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    tag TEXT UNIQUE NOT NULL,
    description TEXT,
    location TEXT,
    installation_date DATE,
    maintenance_status TEXT,
    last_audit_date DATE,
    metadata JSONB -- Armazena colunas extras via ETL (Pandas)
);

-- 3. INDEXAÇÃO VETORIAL E TEXTUAL

-- Indexação Vetorial (HNSW) para Busca Semântica em nível de fragmento
CREATE INDEX IF NOT EXISTS idx_document_chunks_embedding_hnsw 
    ON document_chunks USING hnsw (embedding vector_cosine_ops);

-- Índice de Trigramas para aceleração do Fuzzy Matching em TAGs de ativos
CREATE INDEX IF NOT EXISTS idx_inventory_tag_trgm 
    ON inventory USING gin (tag gin_trgm_ops);
