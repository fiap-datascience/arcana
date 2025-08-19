# Arcana

## 📌 Descrição

O **Arcana** é um projeto de análise e **clusterização de clientes da TOTVS**, com o objetivo de identificar perfis semelhantes, compreender padrões de satisfação e uso, e gerar insights estratégicos para retenção e crescimento.  

O projeto contempla desde a ingestão e organização dos dados em camadas até a aplicação futura de técnicas de machine learning para agrupamento de clientes, além da construção de dashboards analíticos.


## 🎯 Objetivos

- Consolidar uma visão **360º do cliente**, integrando informações contratuais, de suporte, NPS e telemetria.  
- Realizar **análises exploratórias (EDA)** para identificar padrões, hipóteses e direcionar a clusterização.  
- Desenvolver e validar **modelos de clusterização** que agrupem clientes com base em perfil, comportamento e satisfação.  
- Fornecer **insights acionáveis** para estratégias personalizadas de relacionamento.  
- Criar **dashboards interativos** que facilitem a visualização e o monitoramento contínuo dos clusters.  


## 📂 Dados Utilizados

- **Dados cadastrais**: informações contratuais, produtos, segmentos, faturamento.  
- **Tickets de suporte**: chamados abertos, SLA, prioridade e resolutividade.  
- **NPS transacional e relacional**: produto, implantação, onboarding, aquisição e suporte.  
- **Dados de telemetria**: uso de módulos, duração de eventos, status de licença.  
- **Histórico de contratações e MRR**.  


## 🛠️ Etapas (Roadmap)

- [x] Ingestão de dados na camada **bronze** (CSV bruto).
- [x] Análise exploratória inicial para entendimento dos dados.
- [x] Criação da camada **silver** (dados tratados, padronizados e prontos para análise).  
- [x] Análise exploratória completa.  
- [ ] **Engenharia de atributos para clusterização (em andamento)** .  
- [ ] Desenvolvimento e validação de modelos de clusterização.  
- [ ] Construção de dashboards interativos para análise dos clusters.  
- [ ] Automatização do pipeline de ingestão e atualização periódica.  
- [ ] Documentação final do projeto e publicação dos resultados.  


## 🗃️ Kanban

Para a organização e definição das atividades de cada etapa, estamos utilizando um Kanban no Trello.

Link para acesso ao Kanban: [Kanban](https://trello.com/invite/b/682fca65088d0a8826829116/ATTI3be9d02d2f1b29bb1b2e38895400c9d407137146/kanban-arcana-ec2025)


## 🧑‍💻 Tecnologias

- **Linguagem**: Python (pandas, numpy, scikit-learn, seaborn, matplotlib).  
- **Infraestrutura**: AWS (S3, Athena, Glue, Lakehouse).  
- **Visualização**: Jupyter Notebook, Power BI.  
- **Versionamento**: Git & GitHub.  


## 📊 Status Atual

- Dados já organizados em bronze e silver.  
- Análise exploratória inicial concluída (qualidade de dados, distribuições, produtos contratados, receita).  
- Em andamento:  
  - **Análise detalhada de todas as tabelas**.  
  - **Preparação de tabela consolidada para feature engineering**.  
  - **Definição de campos estratégicos** para direcionar a clusterização.
