import boto3
import pandas as pd
from io import BytesIO, StringIO
import os
import warnings
import csv

warnings.filterwarnings("ignore")

# Configurações
BUCKET = "arcana-fiap"
PREFIXO_RAW = "bronze/"
PREFIXO_CURATED = "silver/"

# Mapeamento de renomeação de colunas por tabela
RENOMEACAO_COLUNAS = {
    "nps_transacional_onboarding.csv": {
        "Data de resposta": "DATA_RESPOSTA",
        "Cod Cliente": "COD_CLIENTE",
        "Em uma escala de 0 a 10, quanto você recomenda o Onboarding da TOTVS para um amigo ou colega?": "NOTA_RECOMENDACAO",
        "Em uma escala de 0 a 10, o quanto você acredita que o atendimento CS Onboarding ajudou no início da sua trajetória com a TOTVS?": "NOTA_ATENDIMENTO_CS_ONBOARDING",
        "- Duração do tempo na realização da reunião de Onboarding;": "NOTA_DURACAO_REUNIAO_ONBOARDING",
        "- Clareza no acesso aos canais de comunicação da TOTVS;": "NOTA_CLAREZA_CANAIS_COMUNICACAO",
        "- Clareza nas informações em geral transmitidas pelo CS que lhe atendeu no Onboarding;": "NOTA_CLAREZA_INFORMACOES_CS",
        "- Expectativas atendidas no Onboarding da TOTVS.": "NOTA_EXPECTATIVAS_ATENDIDAS_ONBOARDING"
    },
    "dados_clientes.csv": {
        "CD_CLIENTE": "COD_CLIENTE",
        "DT_ASSINATURA_CONTRATO": "DATA_ASSINATURA_CONTRATO"
    },
    "historico.csv": {
        "NR_PROPOSTA": "NR_PROPOSTA",
        "DT_UPLOAD": "DATA_UPLOAD",
        "CD_CLIENTE": "COD_CLIENTE",
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
        "date_columns": ["DATA_RESPOSTA"],
        "numeric_columns": ["NOTA_NPS", "NOTA_AGILIDADE", "NOTA_CONHECIMENTO", "NOTA_CUSTO", "NOTA_FACILIDADE", "NOTA_FLEXIBILIDADE"]
    },
    "clientes_desde.csv": {
        "date_columns": ["DATA_CLIENTE_DESDE"],
        "numeric_columns": []
    },
    "dados_clientes.csv": {
        "date_columns": ["DATA_ASSINATURA_CONTRATO"],
        "numeric_columns": []
    },
    "contratacoes_ultimos_12_meses.csv": {
        "date_columns": [],
        "numeric_columns": ["QT_CONTRATACOES_12M", "VL_CONTRATACOES_12M"]
    },
    "nps_transacional_onboarding.csv": {
        "date_columns": ["DATA_RESPOSTA"],
        "numeric_columns": ["NOTA_RECOMENDACAO", "NOTA_ATENDIMENTO_CS_ONBOARDING","NOTA_DURACAO_REUNIAO_ONBOARDING","NOTA_CLAREZA_CANAIS_COMUNICACAO", "NOTA_CLAREZA_INFORMACOES_CS", "NOTA_EXPECTATIVAS_ATENDIDAS_ONBOARDING"]
    },
    "historico.csv": {
        "date_columns": ["DATA_UPLOAD"],
        "numeric_columns": []
    },
    "nps_transacional_implantacao.csv": {
        "date_columns": ["DATA_RESPOSTA"],
        "numeric_columns": ["NOTA_NPS", "NOTA_METODOLOGIA", "NOTA_GESTAO", "NOTA_CONHECIMENTO", "NOTA_QUALIDADE", "NOTA_COMUNICACAO", "NOTA_PRAZOS"]
    },
    "nps_transacional_suporte.csv": {
        "date_columns": [],
        "numeric_columns": ["NOTA_NPS", "NOTA_CONHECIMENTO_AGENTE", "NOTA_TEMPO_RETORNO", "NOTA_FACILIDADE", "NOTA_QUALIDADE", "NOTA_SATISFACAO"]
    },
    "mrr.csv": {
        "date_columns": [],
        "numeric_columns": ["MRR_12M"]
    },
    "nps_relacional.csv": {
        "date_columns": ["DATA_RESPOSTA"],
        "numeric_columns": ["NOTA_NPS", "NOTA_RESPOSTA_UNIDADE", "NOTA_SUPTEC_AGILIDADE", "NOTA_SUPTEC_ATENDIMENTO", "NOTA_COMERCIAL", "NOTA_CUSTOS", "NOTA_ADMFIN_ATENDIMENTO", "NOTA_SOFTWARE", "NOTA_SOFTWARE_ATUALIZACAO"]
    },
    "tickets.csv": {
        "date_columns": ["DATA_CRIACAO", "DATA_ATUALIZACAO"],
        "numeric_columns": []
    },
    "nps_transacional_produto.csv": {
        "date_columns": ["DATA_RESPOSTA"],
        "numeric_columns": ["NOTA_NPS"]
    },
    "telemetria_1.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_2.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_3.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_4.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_5.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_6.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_7.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_8.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_9.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_10.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    },
    "telemetria_11.csv": {
        "date_columns": ["DATA_REFERENCIA"],
        "numeric_columns": ["DURACAO_EVENTO"]
    }
}

# Conexão S3
access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client('s3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name = 'us-east-2')

def lista_arquivos_csv(bucket, prefix):
    """Lista arquivos CSV no S3."""
    files = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".csv"):
            files.append(obj["Key"])
    return files

def valida_datas(df, date_columns):
    """Valida colunas de data."""
    for col in date_columns:
        if col in df.columns:
            converted = pd.to_datetime(df[col], errors="coerce")
            invalid_count = converted.isna().sum()
            if invalid_count > 0:
                print(f"⚠️ Coluna '{col}' contém {invalid_count} valores inválidos para data.")
            df[col] = converted
    return df

def valida_numericos(df, numeric_columns):
    """Valida colunas numéricas."""
    for col in numeric_columns:
        if col in df.columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            invalid_count = converted.isna().sum()
            if invalid_count > 0:
                print(f"⚠️ Coluna '{col}' contém {invalid_count} valores inválidos para número.")
            df[col] = converted
    return df

def detectar_separador_amostra(bytes_data):
    sample = bytes_data.decode("utf-8", errors="ignore")
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample.split("\n", 5)[0])  # testa na primeira linha
        return dialect.delimiter
    except csv.Error:
        return ","  # fallback padrão

def decode_bytes_with_fallback(raw_bytes):
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return raw_bytes.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("latin-1", errors="replace"), "latin-1"

def detecta_separador(text):
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except csv.Error:
        return None


def processa_csv_para_parquet(s3_key):
    """Processa CSV: detecta encoding/sep, renomeia, valida e salva no curated."""
    filename = s3_key.split("/")[-1]
    print(f"\n Processando {filename}...")

    # Baixar bytes do S3
    obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
    raw_bytes = obj["Body"].read()

    # Detectar encoding e decodificar
    text, enc = decode_bytes_with_fallback(raw_bytes)
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
        print(f"   ⚠️ Falha na leitura ({type(e1).__name__}). Nova tentativa com engine='python'.")
        df = pd.read_csv(StringIO(text), sep=sep or None, engine="python", on_bad_lines="error")

    # Renomear colunas 
    rename_map = RENOMEACAO_COLUNAS.get(filename, {})
    if rename_map:
        df.rename(columns=rename_map, inplace=True)

    # Limpeza inicial
    # df.dropna(how="all", inplace=True)
    # df.drop_duplicates(inplace=True)

    # Validar datas e numéricos conforme regras por tabela
    rules = CAMPOS_TABELAS.get(filename, {})
    df = valida_datas(df, rules.get("date_columns", []))
    df = valida_numericos(df, rules.get("numeric_columns", []))

    # Ajuste nos números com vírgula decimal
    for col in rules.get("numeric_columns", []):
        if col in df.columns and df[col].dtype == "object":
            df[col] = (df[col]
                       .astype(str)
                       .str.replace(".", "", regex=False)  # remove separador de milhar
                       .str.replace(",", ".", regex=False))  # troca vírgula por ponto
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Salvar no formato Parquet
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    curated_key = f"{PREFIXO_CURATED}{filename.replace('.csv', '.parquet')}"
    s3.put_object(Bucket=BUCKET, Key=curated_key, Body=parquet_buffer.getvalue())

    print(f"✅ Arquivo salvo na camada curated: {curated_key}")

def main():
    csv_files = lista_arquivos_csv(BUCKET, PREFIXO_RAW)
    for file_key in csv_files:
        processa_csv_para_parquet(file_key)

    print(f"✅ Todos os arquivos foram processados.")

if __name__ == "__main__":
    main()