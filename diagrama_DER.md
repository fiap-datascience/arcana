# Diagrama ER TOTVS

```mermaid
erDiagram
    TB_DADOS_CLIENTES {
        string COD_CLIENTE PK
        string DS_PROD
        string DS_LIN_REC
        string FAT_FAIXA
        string MARCA_TOTVS
        string DS_SEGMENTO
        string DS_SUBSEGMENTO
        string CIDADE_CLIENTE
        string UF_CLIENTE
        string PAIS_CLIENTE
        string MODAL_COMERC
        string PERIODICIDADE
        string SIT_CONTRATO
        string DS_CNAE
        float  VL_TOTAL_CONTRATO
        date   DATA_ASSINATURA_CONTRATO
    }

    TB_TELEMETRIA {
        string COD_CLIENTE FK
        float  DURACAO_EVENTO
        string ID_MODULO
        string ID_LINHA_PRODUTO
        date   DATA_REFERENCIA
        string ID_SLOT
        string STATUS_LICENCA
        string CLOUD
        string CLIENTE_PRIME
    }

    TB_CLIENTES_DESDE {
        string COD_CLIENTE FK
        date   DATA_CLIENTE_DESDE
    }

    TB_CONTRATACOES_ULTIMOS_12_MESES {
        string COD_CLIENTE FK
        int    QT_CONTRATACOES_12M
        float  VL_CONTRATACOES_12M
    }

    TB_HISTORICO {
        string COD_CLIENTE FK
        string NR_PROPOSTA
        int    ITEM_PROPOSTA
        date   DATA_UPLOAD
        string HOSPEDAGEM
        string FAT_FAIXA
        string COD_PROD
        int    QTD
        int    MESES_BONIF
        float  VL_PCT_TEMP
        float  VL_PCT_DESCONTO
        float  PRC_UNITARIO
        float  VL_DESCONTO_TEMPORARIO
        float  VL_TOTAL
        float  VL_FULL
        float  VL_DESCONTO
    }

    TB_TICKETS {
        string COD_CLIENTE FK
        string NOME_GRUPO
        string TIPO_TICKET
        string STATUS_TICKET
        date   DATA_CRIACAO
        date   DATA_ATUALIZACAO
        string BK_TICKET PK
        string PRIORIDADE_TICKET
    }

    TB_NPS_RELACIONAL {
        string COD_CLIENTE FK
        date   DATA_RESPOSTA
        int    NOTA_NPS
        int    NOTA_RESPOSTA_UNIDADE
        int    NOTA_SUPTEC_AGILIDADE
        int    NOTA_SUPTEC_ATENDIMENTO
        int    NOTA_COMERCIAL
        int    NOTA_CUSTOS
        int    NOTA_ADMFIN_ATENDIMENTO
        int    NOTA_SOFTWARE
        int    NOTA_SOFTWARE_ATUALIZACAO
    }

    TB_NPS_TRANSACIONAL_AQUISICAO {
        string COD_CLIENTE FK
        date   DATA_RESPOSTA
        int    NOTA_NPS
        int    NOTA_AGILIDADE
        int    NOTA_CONHECIMENTO
        int    NOTA_CUSTO
        int    NOTA_FACILIDADE
        int    NOTA_FLEXIBILIDADE
    }

    TB_NPS_TRANSACIONAL_IMPLANTACAO {
        string COD_CLIENTE FK
        date   DATA_RESPOSTA
        int    NOTA_NPS
        int    NOTA_METODOLOGIA
        int    NOTA_GESTAO
        int    NOTA_CONHECIMENTO
        int    NOTA_QUALIDADE
        int    NOTA_COMUNICACAO
        int    NOTA_PRAZOS
    }

    TB_NPS_TRANSACIONAL_ONBOARDING {
        string COD_CLIENTE FK
        date   DATA_RESPOSTA
        int    NOTA_RECOMENDACAO
        int    NOTA_ATENDIMENTO_CS_ONBOARDING
        int    NOTA_DURACAO_REUNIAO_ONBOARDING
        int    NOTA_CLAREZA_CANAIS_COMUNICACAO
        int    NOTA_CLAREZA_INFORMACOES_CS
        int    NOTA_EXPECTATIVAS_ATENDIDAS_ONBOARDING
        int    NOTA_PRAZOS
    }

    TB_NPS_TRANSACIONAL_PRODUTO {
        string COD_CLIENTE FK
        date   DATA_RESPOSTA
        string LINHA_PRODUTO
        string NOME_PRODUTO
        int    NOTA_NPS
    }

    TB_NPS_TRANSACIONAL_SUPORTE {
        string COD_CLIENTE FK
        string COD_TICKET
        int    NOTA_NPS
        string GRUPO_NPS
        int    NOTA_CONHECIMENTO_AGENTE
        int    NOTA_SOLUCAO
        int    NOTA_TEMPO_RETORNO
        int    NOTA_FACILIDADE
        int    NOTA_SATISFACAO
    }

    %% RELACIONAMENTOS
    TB_DADOS_CLIENTES ||--o{ TB_TELEMETRIA : possui
    TB_DADOS_CLIENTES ||--o{ TB_CLIENTES_DESDE : possui
    TB_DADOS_CLIENTES ||--o{ TB_CONTRATACOES_ULTIMOS_12_MESES : possui
    TB_DADOS_CLIENTES ||--o{ TB_HISTORICO : possui
    TB_DADOS_CLIENTES ||--o{ TB_TICKETS : possui
    TB_DADOS_CLIENTES ||--o{ TB_NPS_RELACIONAL : responde
    TB_DADOS_CLIENTES ||--o{ TB_NPS_TRANSACIONAL_AQUISICAO : responde
    TB_DADOS_CLIENTES ||--o{ TB_NPS_TRANSACIONAL_IMPLANTACAO : responde
    TB_DADOS_CLIENTES ||--o{ TB_NPS_TRANSACIONAL_ONBOARDING : responde
    TB_DADOS_CLIENTES ||--o{ TB_NPS_TRANSACIONAL_PRODUTO : responde
    TB_DADOS_CLIENTES ||--o{ TB_NPS_TRANSACIONAL_SUPORTE : responde
