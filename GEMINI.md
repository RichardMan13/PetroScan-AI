1: # PetroScan-AI: Gemini Context & Guidelines
2: 
3: Este arquivo serve como o **Guia de Contexto Mestre** para a interação de IA (Gemini/Antigravity) neste projeto. Ele define quem somos, o que estamos construindo e as restrições técnicas.
4: 
5: ---
6: 
7: ## Contexto do Projeto
8: 
9: **Missão:** Transmutar documentos complexos (Normas N-XXXX), diagramas de engenharia (P&IDs) e inventários da Petrobras em insights acionáveis através de um motor de Busca Semântica de alta precisão (Information Retrieval). O foco é prover evidências matemáticas de confiabilidade empíricas da busca (Recall@K, MRR) conectando metadados técnicos, sem utilizar agentes LLMs abstratos para geração de respostas por texto, bloqueando o risco de alucinações técnicas.
10: 
11: ---
12: 
13: ## Stack Tecnológica
14: Ao sugerir código ou arquitetura, **sempre** utilize estas bibliotecas ou este ecossistema primário:
15: 
16: * **Orquestração e Integração:** Docker, Docker Compose, Redis (utilizado como broker de mensagens e cache).
17: * **Armazenamento Transacional e de Arquivos:** PostgreSQL 16+ (com extensão `pgvector`) e MinIO (armazenamento compatível com o protocolo S3).
18: * **Core de Inteligência Visual e Semântica (Python):** `torch` (PyTorch base), Hugging Face `transformers` (modelos pré-treinados: LayoutLMv3) e exclusivamente a suíte `sentence-transformers` para o cômputo semântico e embedding.
19: * **Processamento e Filas de Trabalho:** `pandas` (ETL de inventário), drivers padronizados de PostgreSQL e `redis-py` para consumo seguro das filas atômicas.
20: * **MLOps e Governança:** `mlflow` (Model Registry/Metrics), `evidently` (Circuit Breaker / Data Drift) acoplados a uma rotina de checagem contra os chamados *Golden Datasets*.
21: * **Interface e Qualidade:** `streamlit` para painéis técnicos Side-by-Side e `pytest` blindando toda lógica dos workers com testes de software.
22: 
23: ---
24: 
25: ## Diretrizes de Arquitetura
26: 
27: 1. **Comunicação Orientada a Eventos:** O custo computacional pesado dos extratores PDF e modelos visuais exige que tudo ocorra isoladamente em "Workers" escutando as filas do Redis, escalonando horizontalmente através do parque das GPUs (usando NVIDIA Container Toolkit).
28: 2. **Resiliência e Simplificação:** O sistema utiliza Redis para coordenação de tarefas. Falhas persistentes são registradas em logs de auditoria e movidas para inspeção humana.
29: 3. **Recuperação de Informação:** A busca é baseada em similaridade vetorial combinada com filtragem por metadados estruturados (Tags, IDs de ativos), garantindo que os resultados técnicos correspondam aos ativos reais.
30: 
31: ---
32: 
33: ## Regras de Comportamento para a IA (Mandatório)
34: 
35: - **Idioma Padrão:** O desenvolvimento conceitual e explicações serão conduzidos em **Português (BR)**. A nomeação estrutural (variáveis, defs, classes, logs, esquemas do BD) deve seguir o padrão corporativo em **Inglês**.
36: - **Qualidade de Código:** Obrigatório a inserção de tipagem explícita (**Type Hinting**) no Python, com docstrings no formato padronizado detalhando a semântica da função sempre que a linha algorítmica for complexa.
37: - **Restrição de Conduta Estética:** **Não utilize nenhum tipo de emoji** nas documentações centrais (`README.md`, `GEMINI.md`, painéis em Streamlit) nem comentários visuais textuais coloridos nas matrizes dos códigos Python.
38: 
39: ---
40: 
41: ## Knowledge Items Prioritários
42: - Inicie qualquer formulação nova apenas após a leitura técnica dos arquivos alocados sob `.gemini/rules/` para manter-se estritamente sob as exigências de conduta de MLOps, tipagem, containers e sintaxe da documentação.
43: 
