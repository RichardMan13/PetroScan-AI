# PetroScan-AI: Gemini Context & Guidelines

Este arquivo serve como o **Guia de Contexto Mestre** para a interação de IA (Gemini/Antigravity) neste projeto. Ele define quem somos, o que estamos construindo e as restrições técnicas intransponíveis.

---

## Contexto do Projeto

**Missão:** Transmutar documentos complexos (Normas N-XXXX), diagramas de engenharia (P&IDs) e inventários da Petrobras em insights acionáveis através de um motor de Busca Semântica de alta precisão (Information Retrieval). O foco é conectar informações heterogêneas via "Golden Join" no banco de dados e prover evidências matemáticas de confiabilidade empíricas da busca (Recall@K, MRR), não utilizando agentes LLMs abstratos para geração de respostas por texto, bloqueando o risco de alucinações técnicas.

---

## Stack Tecnológica (Imutável)
Ao sugerir código ou arquitetura, **sempre** utilize estas bibliotecas ou este ecossistema primário:

* **Orquestração e Integração:** Docker, Docker Compose, RabbitMQ (com filas DLX obrigatórias).
* **Armazenamento Transacional e de Arquivos:** PostgreSQL 16+ (com extensão `pgvector`) e MinIO (armazenamento compatível com o protocolo S3).
* **Core de Inteligência Visual e Semântica (Python):** `torch` (PyTorch base), Hugging Face `transformers` (modelos pré-treinados: LayoutLMv3, TrOCR, CLIP) e exclusivamente a suíte `sentence-transformers` para o cômputo semântico e embedding.
* **Processamento e Filas de Trabalho:** `pandas` (ETL de inventário), drivers padronizados de PostgreSQL e `pika` / `celery` para consumo seguro das filas atômicas.
* **MLOps e Governança:** `mlflow` (Model Registry/Metrics), `evidently` (Circuit Breaker / Data Drift) acoplados a uma rotina de checagem contra os chamados *Golden Datasets*.
* **Interface e Qualidade:** `streamlit` para painéis técnicos Side-by-Side e `pytest` blindando toda lógica dos workers com testes de software.

---

## Diretrizes de Arquitetura

1. **Comunicação Orientada a Eventos:** O custo computacional pesado dos extratores PDF e modelos visuais exige que tudo ocorra isoladamente em "Workers" escutando as filas do RabbitMQ, escalonando horizontalmente através do parque das GPUs (usando NVIDIA Container Toolkit).
2. **Resiliência Falha Segura:** Se o `docling` ou `OCR` falharem na leitura de uma planta/diagrama mais de 3 vezes (retry), essas sub-tarefas morrem no *Dead Letter Exchange* para a fila de `human.review`.
3. **Metodologia "Golden Join":** Os pilares do banco de dados relacional. Toda a validação acontece quando a extração textual pura, um Bounding Box originado visualmente num diagrama, e um histórico estruturado da métrica do arquivo CSV combinam identicamente em valor sintático ou matemático. 

---

## Regras de Comportamento para a IA (Mandatório)

- **Idioma Padrão:** O desenvolvimento conceitual e explicações serão conduzidos em **Português (BR)**. A nomeação estrutural (variáveis, defs, classes, logs, esquemas do BD) deve seguir o padrão corporativo em **Inglês**.
- **Qualidade de Código:** Obrigatório a inserção de tipagem explícita (**Type Hinting**) no Python, com docstrings no formato padronizado detalhando a semântica da função sempre que a linha algorítmica for complexa.
- **Restrição de Conduta Estética:** **Não utilize nenhum tipo de emoji** nas documentações centrais (`README.md`, `GEMINI.md`, painéis em Streamlit) nem comentários visuais textuais coloridos nas matrizes dos códigos Python.

---

## Knowledge Items Prioritários
- Inicie qualquer formulação nova apenas após a leitura técnica dos arquivos alocados sob `.gemini/rules/` para manter-se estritamente sob as exigências de conduta de MLOps, tipagem, containers e sintaxe da documentação.
