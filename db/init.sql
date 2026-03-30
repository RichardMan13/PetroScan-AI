-- Extensões Necessárias
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- Para Fuzzy Matching

-- ... (Tabelas existentes permanecem iguais)

-- Índice de Trigramas para aceleração do Fuzzy Matching em TAGs de ativos
CREATE INDEX IF NOT EXISTS idx_inventory_tag_trgm ON inventory USING gin (tag gin_trgm_ops);

-- VIEW DO GOLDEN JOIN: A "Alma" do Projeto
-- Conecta: (1) O Ativo no Inventário <= (2) A Identificação Visual no P&ID <= (3) A Norma Técnica Brasileira (IBP/ANP)
CREATE OR REPLACE VIEW vw_golden_join AS
SELECT 
    inv.tag AS inventory_tag,
    inv.description AS inventory_desc,
    inv.maintenance_status,
    ent.tag AS detected_tag,
    ent.entity_type,
    ent.confidence AS vision_confidence,
    doc.title AS norm_title,
    doc.version AS norm_version,
    doc.content AS norm_requirement_snippet,
    ent.image_snippet_url
FROM inventory inv
-- O Join utiliza Fuzzy Matching se a tag não for idêntica (similaridade > 0.4)
JOIN entities ent ON (inv.tag = ent.tag OR similarity(inv.tag, ent.tag) > 0.4)
JOIN documents doc ON (ent.document_id = doc.id)
WHERE ent.entity_type IS NOT NULL;

-- Schema para o PetroScan-AI
-- O foco é conectar informações heterogêneas via "Golden Join"

-- Tabela de Documentos (Normas N-XXXX)
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

-- Tabela de Chunks (Fragmentos para Busca Semântica Granular)
-- Única tabela que armazena os trechos textuais para busca no banco
CREATE TABLE IF NOT EXISTS document_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id) ON DELETE CASCADE,
    content TEXT NOT NULL, -- O parágrafo/trecho específico
    page_number INTEGER,
    chunk_index INTEGER,
    embedding VECTOR(384), -- Vetor para Busca Semântica
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexação Vetorial (HNSW) para Busca Semântica em nível de fragmento (ALTA PRECISÃO)
CREATE INDEX IF NOT EXISTS idx_document_chunks_embedding_hnsw 
    ON document_chunks USING hnsw (embedding vector_cosine_ops);

-- Tabela de Equipamentos/Tags extraídos de P&IDs
CREATE TABLE IF NOT EXISTS entities (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(id),
    tag TEXT NOT NULL, -- Ex: P-101-A
    entity_type TEXT, -- Pump, Valve, Vessel
    bounding_box JSONB, -- Coordenadas (x, y, w, h)
    confidence REAL,
    image_snippet_url TEXT, -- MinIO path
    visual_embedding VECTOR(512), -- Dimensão padrão do CLIP ViT-B/32
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexação Vetorial (HNSW) para Busca Visual (CLIP)
CREATE INDEX IF NOT EXISTS idx_entities_visual_embedding_hnsw 
    ON entities USING hnsw (visual_embedding vector_cosine_ops);

-- Tabela de Inventário Estruturado
CREATE TABLE IF NOT EXISTS inventory (
    id SERIAL PRIMARY KEY,
    tag TEXT UNIQUE NOT NULL,
    description TEXT,
    location TEXT,
    installation_date DATE,
    maintenance_status TEXT,
    last_audit_date DATE,
    metadata JSONB
);

-- Indexação Vetorial (HNSW) para Busca Semântica
CREATE INDEX IF NOT EXISTS idx_documents_embedding_hnsw 
    ON documents USING hnsw (embedding vector_cosine_ops);
