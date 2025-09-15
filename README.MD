# Arcana  

## 📌 Descrição  

O **Arcana** é um projeto de análise e **clusterização de clientes da TOTVS**, com o objetivo de identificar perfis semelhantes, compreender padrões de satisfação e uso, e gerar insights estratégicos para retenção e crescimento.  

O projeto abrangeu desde a ingestão e organização dos dados em camadas até a aplicação de **modelos de clusterização baseados em Gaussian Mixture Models Bayesianos (BGMM)**, resultando na criação de **personas de clientes** que apoiam estratégias de personalização e relacionamento.  


## 🎯 Objetivos  

- Consolidar uma visão **360º do cliente**, integrando informações contratuais, de suporte, NPS e telemetria.  
- Realizar **análises exploratórias (EDA)** para identificar padrões, hipóteses e direcionar a clusterização.  
- Desenvolver e validar **modelos de clusterização (BGMM)** para agrupar clientes com base em perfil, comportamento e satisfação.  
- Criar **personas representativas** dos clusters obtidos, facilitando a definição de estratégias específicas.  
- Fornecer **insights acionáveis** para retenção, satisfação e crescimento.  
- Construir **dashboards interativos** para visualização e monitoramento contínuo.  


## 📂 Dados Utilizados  

- **Dados cadastrais**: informações contratuais, produtos, segmentos, faturamento.  
- **Tickets de suporte**: chamados abertos, SLA, prioridade e resolutividade.  
- **NPS transacional e relacional**: produto, implantação, onboarding, aquisição e suporte.  
- **Dados de telemetria**: uso de módulos, duração de eventos, status de licença.  
- **Histórico de contratações e MRR**.  


## 🛠️ Etapas  

- [x] Ingestão de dados na camada **bronze** (CSV bruto).  
- [x] Criação da camada **silver** (dados tratados e padronizados).  
- [x] Análises exploratórias (EDA).  
- [x] Engenharia de atributos para clusterização.  
- [x] **Desenvolvimento e validação do modelo BGMM** para definição dos clusters.  
- [x] **Construção das personas** a partir dos clusters.  
- [x] Construção de dashboards interativos no Power BI.  
- [x] Documentação final e publicação dos resultados.  


## 📊 Resultados  

- **Modelo de Clusterização**: Gaussian Mixture Bayesian (BGMM), escolhido por sua capacidade de lidar com incerteza e determinar automaticamente o número ótimo de clusters.  
- **Clusters obtidos**: foram definidos **8 clusters principais** que representam diferentes perfis de clientes.  
- **Personas criadas**: cada cluster foi traduzido em uma persona, facilitando a comunicação dos resultados para áreas de negócio.  
- **Dashboard no Power BI**: consolidou informações dos clusters e personas, possibilitando acompanhamento contínuo.  


## 👥 Personas Criadas  

A clusterização via **BGMM** resultou em 8 grupos principais de clientes, traduzidos em personas estratégicas. Segue exemplo de 5 identificados:  

| Cluster | Persona                  | Perfil / Segmento                                    | Valor      | Lealdade   | Engajamento | Necessidades / Dores principais                                                                 | Ação Recomendável                                                                 |
|---------|--------------------------|------------------------------------------------------|------------|------------|-------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------|
| 0       | **Clientes Adormecidos** | Pequeno porte de serviços e varejo                   | Mais baixo | Média-Baixa | Nulo        | Retomar contato e oferecer relevância                                                         | Campanhas de reativação, ofertas personalizadas, contato humano                   |
| 1       | **VIPs da Indústria**    | Grande porte, concentrado em manufatura              | Muito Alto | Alta       | Ativo       | Suporte premium, parceria estratégica                                                         | Gestão de contas (KAM), reuniões executivas, suporte técnico diferenciado         |
| 2       | **Veteranos em Manutenção** | Perfil demográfico diversificado                   | Moderado   | Alta       | Nulo        | Reforçar o valor de novas soluções                                                            | Reconexão estratégica, workshops, consultoria pró-ativa                           |
| 3       | **Parceiros Estratégicos** | Grandes empresas, perfil diversificado             | Extremo    | Alta       | Baixo       | Confiança, segurança e visão de longo prazo                                                   | Atendimento "Concierge", contato executivo dedicado, alinhamento estratégico      |
| 4       | **Clientes Padrão**      | Pequeno porte, perfil diversificado                  | Moderado   | Média-Alta | Ativo       | Estabilidade, confiança e evolução gradativa                                                  | Relacionamento contínuo, upsell e suporte consultivo                              |


## 🗃️ Kanban  

Organização do projeto no Trello: [Kanban](https://trello.com/invite/b/682fca65088d0a8826829116/ATTI3be9d02d2f1b29bb1b2e38895400c9d407137146/kanban-arcana-ec2025)  


## 📊 DataViz  

Dashboard interativo no Power BI: [Power BI](https://app.powerbi.com/view?r=eyJrIjoiNjY3NTM5NTUtODQ0Yy00ODEzLWJhZDEtZTcxMjA0MTYzYjkwIiwidCI6IjU4YjBjYWY5LWFkZjUtNDQxNC1hOThlLTQyM2JlYjEzZGRkZCJ9)  


## 🧑‍💻 Tecnologias  

- **Linguagem**: Python (pandas, numpy, scikit-learn, seaborn, matplotlib).  
- **Infraestrutura**: AWS (S3, Athena, Glue, Lakehouse).  
- **Visualização**: Jupyter Notebook, Power BI.  
- **Versionamento**: Git & GitHub.  


## ✅ Status Final  

- Projeto concluído.  
- Modelo **BGMM validado e aplicado**.  
- **5 Personas definidas** e disponíveis em dashboard.  
- Entregues **insights estratégicos** para retenção, satisfação e expansão da base de clientes.  
