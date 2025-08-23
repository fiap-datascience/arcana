import pandas as pd
import warnings
import os
import csv
from io import StringIO
import glob

warnings.filterwarnings("ignore")

# Configurações para o ambiente local
INPUT_PATH = "C:\\Users\\Ana CSP\\Desktop\\challenge - fiap\\arcana NO AWS\\dados\\raw"
OUTPUT_PATH = "C:\\Users\\Ana CSP\\Desktop\\challenge - fiap\\arcana NO AWS\\dados\\curated"

# Certifique-se de que a pasta de saída existe
os.makedirs(OUTPUT_PATH, exist_ok=True)

# Mapeamento de renomeação de colunas por tabela
RENOMEACAO_COLUNAS = {
    "nps_transacional_onboarding.csv": {
        "Data de resposta": "DATA_RESPOSTA",
        "Cod Cliente": "COD_CLIENTE",
        "Em uma escala de 0 a 10, quanto você recomenda o Onboarding da TOTVS para um amigo ou colega?.": "NOTA_RECOMENDACAO",
        "Em uma escala de 0 a 10, o quanto você acredita que o atendimento CS Onboarding ajudou no início da sua trajetória com a TOTVS?": "NOTA_ATENDIMENTO_CS_ONBOARDING",
        "- Duração do tempo na realização da reunião de Onboarding;": "NOTA_DURACAO_REUNIAO_ONBOARDING",
        "- Clareza no acesso aos canais de comunicação da TOTVS;": "NOTA_CLAREZA_CANAIS_COMUNICACAO",
        "- Clareza nas informações em geral transmitidas pelo CS que lhe atendeu no Onboarding;": "NOTA_CLAREZA_INFORMACOES_CS",
        "- Expectativas atendidas no Onboarding da TOTVS.": "NOTA_EXPECTATIVAS_ATENDIDAS_ONBOARDING"
    },
    "dados_clientes.csv": {
        "CD_CLIENTE": "COD_CLIENTE",
        "DT_ASSINATURA_CONTRATO": "DATA_ASSINATURA_CONTRATO",
        "CIDADE": "CIDADE_CLIENTE",
        "UF": "UF_CLIENTE",
        "PAIS": "PAIS_CLIENTE",
        "SITUACAO_CONTRATO": "SIT_CONTRATO",
        "PERIODICIDADE": "PERIODICIDADE_COBRANCA"
    },
    "historico.csv": {
        "NR_PROPOSTA": "NR_PROPOSTA",
        "DT_UPLOAD": "DATA_UPLOAD",
        "CD_CLI": "COD_CLIENTE",
        "CD_PROD": "COD_PROD"
    },
    "nps_transacional_implantacao.csv": {
        "Cód. Cliente": "COD_CLIENTE",
        "Data da Resposta": "DATA_RESPOSTA",
        "Nota NPS": "NOTA_NPS",
        "Nota Metodologia": "NOTA_METODOLOGIA",
        "Nota Gestao": "NOTA_GESTAO",
        "Nota Conhecimento": "NOTA_CONHECIMENTO",
        "Nota Qualidade": "NOTA_QUALIDADE",
        "Nota Comunicacao": "NOTA_COMUNICACAO",
        "Nota Prazos": "NOTA_PRAZOS"
    },
    "nps_transacional_suporte.csv": {
        "ticket": "COD_TICKET",
        "resposta_NPS": "NOTA_NPS",
        "grupo_NPS": "GRUPO_NPS",
        "Nota_ConhecimentoAgente": "NOTA_CONHECIMENTO_AGENTE",
        "Nota_Solucao": "NOTA_SOLUCAO",
        "Nota_TempoRetorno": "NOTA_TEMPO_RETORNO",
        "Nota_Facilidade": "NOTA_FACILIDADE",
        "Nota_Satisfacao": "NOTA_SATISFACAO",
        "cliente": "COD_CLIENTE"
    },
    "clientes_desde.csv": {
        "CLIENTE": "COD_CLIENTE",
        "CLIENTE_DESDE": "DATA_CLIENTE_DESDE"
    },
    "mrr.csv": {
        "CLIENTE": "COD_CLIENTE",
        "MRR_12M": "MRR_12M"
    },
    "nps_relacional.csv": {
        "respondedAt": "DATA_RESPOSTA",
        "metadata_codcliente": "COD_CLIENTE",
        "resposta_NPS": "NOTA_NPS",
        "resposta_unidade": "NOTA_RESPOSTA_UNIDADE",
        "Nota_SupTec_Agilidade": "NOTA_SUPTEC_AGILIDADE",
        "Nota_SupTec_Atendimento": "NOTA_SUPTEC_ATENDIMENTO",
        "Nota_Comercial": "NOTA_COMERCIAL",
        "Nota_Custos": "NOTA_CUSTOS",
        "Nota_AdmFin_Atendimento": "NOTA_ADMFIN_ATENDIMENTO",
        "Nota_Software": "NOTA_SOFTWARE",
        "Nota_Software_Atualizacao": "NOTA_SOFTWARE_ATUALIZACAO"
    },
    "nps_transacional_aquisicao.csv": {
        "Cód. Cliente": "COD_CLIENTE",
        "Data da Resposta": "DATA_RESPOSTA",
        "Nota NPS": "NOTA_NPS",
        "Nota Agilidade": "NOTA_AGILIDADE",
        "Nota Conhecimento": "NOTA_CONHECIMENTO",
        "Nota Custo": "NOTA_CUSTO",
        "Nota Facilidade": "NOTA_FACILIDADE",
        "Nota Flexibilidade": "NOTA_FLEXIBILIDADE"
    },
    "contratacoes_ultimos_12_meses.csv": {
        "CD_CLIENTE": "COD_CLIENTE",
        "QTD_CONTRATACOES_12M": "QT_CONTRATACOES_12M",
        "VLR_CONTRATACOES_12M": "VL_CONTRATACOES_12M"
    },
    "tickets.csv": {
        "CODIGO_ORGANIZACAO": "COD_CLIENTE",
        "NOME_GRUPO": "NOME_GRUPO",
        "TIPO_TICKET": "TIPO_TICKET",
        "STATUS_TICKET": "STATUS_TICKET",
        "DT_CRIACAO": "DATA_CRIACAO",
        "DT_ATUALIZACAO": "DATA_ATUALIZACAO",
        "BK_TICKET": "BK_TICKET",
        "PRIORIDADE_TICKET": "PRIORIDADE_TICKET"
    },
    "nps_transacional_produto.csv": {
        "Data da Resposta": "DATA_RESPOSTA",
        "Linha de Produto": "LINHA_PRODUTO",
        "Nome do Produto": "NOME_PRODUTO",
        "Nota": "NOTA_NPS",
        "Cód. T": "COD_CLIENTE"
    },
    "telemetria_1.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_2.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_3.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_4.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_5.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_6.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_7.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_8.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_9.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_10.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    },
    "telemetria_11.csv": {
        "referencedatestart": "DATA_REFERENCIA",
        "clienteid": "COD_CLIENTE",
        "eventduration": "DURACAO_EVENTO",
        "moduloid": "ID_MODULO",
        "productlineid": "ID_LINHA_PRODUTO",
        "slotid": "ID_SLOT",
        "statuslicenca": "STATUS_LICENCA",
        "tcloud": "CLOUD",
        "clienteprime": "CLIENTE_PRIME",
    }
}

CAMPOS_TABELAS = {
    "nps_transacional_aquisicao.csv": {
        "colunas_data": ["DATA_RESPOSTA"],
        "colunas_numericas": ["NOTA_NPS", "NOTA_AGILIDADE", "NOTA_CONHECIMENTO", "NOTA_CUSTO", "NOTA_FACILIDADE", "NOTA_FLEXIBILIDADE"]
    },
    "clientes_desde.csv": {
        "colunas_data": ["DATA_CLIENTE_DESDE"],
        "colunas_numericas": []
    },
    "dados_clientes.csv": {
        "colunas_data": ["DATA_ASSINATURA_CONTRATO"],
        "colunas_numericas": []
    },
    "contratacoes_ultimos_12_meses.csv": {
        "colunas_data": [],
        "colunas_numericas": ["QT_CONTRATACOES_12M", "VL_CONTRATACOES_12M"]
    },
    "nps_transacional_onboarding.csv": {
        "colunas_data": ["DATA_RESPOSTA"],
        "colunas_numericas": ["NOTA_RECOMENDACAO", "NOTA_ATENDIMENTO_CS_ONBOARDING","NOTA_DURACAO_REUNIAO_ONBOARDING","NOTA_CLAREZA_CANAIS_COMUNICACAO", "NOTA_CLAREZA_INFORMACOES_CS", "NOTA_EXPECTATIVAS_ATENDIDAS_ONBOARDING"]
    },
    "historico.csv": {
        "colunas_data": ["DATA_UPLOAD"],
        "colunas_numericas": []
    },
    "nps_transacional_implantacao.csv": {
        "colunas_data": ["DATA_RESPOSTA"],
        "colunas_numericas": ["NOTA_NPS", "NOTA_METODOLOGIA", "NOTA_GESTAO", "NOTA_CONHECIMENTO", "NOTA_QUALIDADE", "NOTA_COMUNICACAO", "NOTA_PRAZOS"]
    },
    "nps_transacional_suporte.csv": {
        "colunas_data": [],
        "colunas_numericas": ["NOTA_NPS", "NOTA_CONHECIMENTO_AGENTE", "NOTA_TEMPO_RETORNO", "NOTA_FACILIDADE", "NOTA_QUALIDADE", "NOTA_SATISFACAO"]
    },
    "mrr.csv": {
        "colunas_data": [],
        "colunas_numericas": ["MRR_12M"]
    },
    "nps_relacional.csv": {
        "colunas_data": ["DATA_RESPOSTA"],
        "colunas_numericas": ["NOTA_NPS", "NOTA_RESPOSTA_UNIDADE", "NOTA_SUPTEC_AGILIDADE", "NOTA_SUPTEC_ATENDIMENTO", "NOTA_COMERCIAL", "NOTA_CUSTOS", "NOTA_ADMFIN_ATENDIMENTO", "NOTA_SOFTWARE", "NOTA_SOFTWARE_ATUALIZACAO"]
    },
    "tickets.csv": {
        "colunas_data": ["DATA_CRIACAO", "DATA_ATUALIZACAO"],
        "colunas_numericas": []
    },
    "nps_transacional_produto.csv": {
        "colunas_data": ["DATA_RESPOSTA"],
        "colunas_numericas": ["NOTA_NPS"]
    },
    "telemetria_1.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_2.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_3.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_4.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_5.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_6.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_7.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_8.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_9.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_10.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    },
    "telemetria_11.csv": {
        "colunas_data": ["DATA_REFERENCIA"],
        "colunas_numericas": ["DURACAO_EVENTO"]
    }
}

def lista_arquivos_csv_local(input_path):
    """Lista arquivos CSV em um diretório local."""
    return glob.glob(os.path.join(input_path, "*.csv"))

def valida_datas(df, colunas_data):
    """Valida colunas de data."""
    for col in colunas_data:
        if col in df.columns:
            converte = pd.to_datetime(df[col], errors="coerce")
            valores_invalidos = converte.isna().sum()
            if valores_invalidos > 0:
                print(f"Coluna '{col}' contém {valores_invalidos} valores inválidos para data.")
            df[col] = converte
    return df

def valida_numericos(df, colunas_numericas):
    """Valida colunas numéricas."""
    for col in colunas_numericas:
        if col in df.columns:
            valores = df[col].astype(str).str.strip()

            # Se houver vírgula em uma fração relevante das linhas, assumimos padrão BR
            if (valores.str.contains(",", na=False).mean() > 0.05):
                valores = valores.str.replace(".", "", regex=False)  # remove milhar
                valores = valores.str.replace(",", ".", regex=False) # vírgula -> ponto

            # remove símbolos não numéricos residuais (R$, espaços, etc.)
            valores = valores.str.replace(r"[^\d\.\-]", "", regex=True)

            converte = pd.to_numeric(valores, errors="coerce")
            valores_invalidos = converte.isna().sum()
            if valores_invalidos > 0:
                print(f"Coluna '{col}' contém {valores_invalidos} valores inválidos para número.")
            df[col] = converte
    return df

def decodificador_arquivo(caminho_arquivo):
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(caminho_arquivo, "r", encoding=enc) as f:
                return f.read(), enc
        except UnicodeDecodeError:
            continue
    with open(caminho_arquivo, "r", encoding="latin-1", errors="replace") as f:
        return f.read(), "latin-1"

def detecta_separador(texto):
    amostra = texto[:4096]
    try:
        dialect = csv.Sniffer().sniff(amostra, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return None

def processa_csv_para_parquet_local(csv_path):
    """Processa CSV: detecta encoding/sep, renomeia, valida e salva no curated."""
    filename = os.path.basename(csv_path)
    print(f"\nProcessando {filename}...")

    # Detectar encoding e decodificar
    text, enc = decodificador_arquivo(csv_path)
    print(f"Encoding detectado: {enc}")

    # Detectar separador
    sep = detecta_separador(text)
    print(f"Separador detectado: '{sep if sep else 'auto (pandas)'}'")

    # Ler CSV no pandas
    try:
        if sep:
            df = pd.read_csv(StringIO(text), sep=sep)
        else:
            df = pd.read_csv(StringIO(text), sep=None, engine="python")
    except Exception as e1:
        print(f"Falha na leitura ({type(e1).__name__}). Tentando novamente...")
        df = pd.read_csv(StringIO(text), sep=sep or None, engine="python", on_bad_lines="error")
    
    # Renomear colunas
    rename_map = RENOMEACAO_COLUNAS.get(filename, {})
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Limpeza inicial
    df.dropna(how="all", inplace=True)
    df.drop_duplicates(inplace=True)

    # Validar datas e numéricos conforme regras por tabela
    rules = CAMPOS_TABELAS.get(filename, {})
    df = valida_datas(df, rules.get("colunas_data", []))
    df = valida_numericos(df, rules.get("colunas_numericas", []))
    
    # Salvar no formato Parquet
    parquet_path = os.path.join(OUTPUT_PATH, filename.replace('.csv', '.parquet'))
    df.to_parquet(parquet_path, index=False)

    print(f"Arquivo salvo na camada curated: {parquet_path}")

def main():
    csv_files = lista_arquivos_csv_local(INPUT_PATH)
    if not csv_files:
        print(f"Nenhum arquivo CSV encontrado na pasta '{INPUT_PATH}'. Verifique se os arquivos existem e se o caminho está correto.")
        return

    for file_path in csv_files:
        processa_csv_para_parquet_local(file_path)

    print("\n✅ Todos os arquivos foram processados com sucesso!")

if __name__ == "__main__":
    main()