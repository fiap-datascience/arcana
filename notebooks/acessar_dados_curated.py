# ==============================================================================
# 📦 Importação de Bibliotecas
# ==============================================================================

import os
import re
import warnings
from io import BytesIO
from typing import Dict, Iterable, List, Optional
import boto3
import botocore
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")

# Configurações padrão
BUCKET = os.getenv("AWS_S3_BUCKET_NAME", "arcana-fiap")
REGIAO = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-2"))
PREFIXO = "silver/"

# Conexão S3
def get_s3():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGIAO,
    )

# Listagem
def listar_parquets(bucket: str = BUCKET, prefix: str = PREFIXO) -> List[str]:
    """
    Lista keys .parquet em s3://bucket/prefix
    """
    s3 = get_s3()
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            if obj["Key"].lower().endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def _index_por_basename(bucket: str, prefix: str) -> dict[str, list[str]]:
    keys = listar_parquets(bucket, prefix)
    idx: dict[str, list[str]] = {}
    for k in keys:
        base_no_ext = os.path.splitext(os.path.basename(k))[0]
        idx.setdefault(base_no_ext, []).append(k)
    return idx

# Utilitários de nome
def _nome_variavel(basename: str, existentes: Iterable[str]) -> str:
    """
    Garante que não começa com número e não colide com nomes já usados.
    """
    name = os.path.splitext(basename)[0]
    name = re.sub(r"\W", "_", name)
    if re.match(r"^\d", name):
        name = f"df_{name}"
    original = name
    i = 1
    existentes = set(existentes)
    while name in existentes:
        name = f"{original}_{i}"
        i += 1
    return name

def _match_pattern(basename: str, pattern: Optional[str]) -> bool:
    if not pattern:
        return True
    pat = re.escape(pattern).replace(r"\*", ".*")
    return re.fullmatch(pat, basename) is not None

# Leitura pontual
def acessar_parquet_s3(
    ARQUIVO: str,
    bucket_name: Optional[str] = None,
    key_prefix: str = "silver",
    columns: Optional[Iterable[str]] = None,
) -> Optional[pd.DataFrame]:
    """
    Lê s3://bucket/silver/<ARQUIVO>.parquet (modo antigo) OU
    resolve s3://bucket/silver/**/<ARQUIVO>.parquet (novo layout com subpastas).
    Também aceita ARQUIVO com subpasta já informada (ex.: 'tb_dados_clientes/dados_clientes').
    """
    bucket_name = bucket_name or BUCKET
    s3 = get_s3()

    direct_key = f"{key_prefix.rstrip('/')}/{ARQUIVO}.parquet"
    if ARQUIVO.lower().endswith(".parquet"):
        direct_key = f"{key_prefix.rstrip('/')}/{ARQUIVO}"

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=direct_key)
        data = BytesIO(obj["Body"].read())
        return pd.read_parquet(data, columns=list(columns) if columns else None, engine="pyarrow")

    except s3.exceptions.NoSuchKey:
        basename = ARQUIVO[:-8] if ARQUIVO.lower().endswith(".parquet") else ARQUIVO
        idx = _index_por_basename(bucket_name, key_prefix)
        candidatos = idx.get(basename, [])
        if not candidatos:
            print(f"Arquivo não encontrado (nem direto nem por busca): {ARQUIVO}")
            return None
        if len(candidatos) > 1:
            print(f"Aviso: múltiplos arquivos encontrados para '{basename}':\n  - " + "\n  - ".join(candidatos))
        key = candidatos[0]
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            data = BytesIO(obj["Body"].read())
            return pd.read_parquet(data, columns=list(columns) if columns else None, engine="pyarrow")
        except Exception as e:
            print(f"Erro lendo {key}: {e}")
            return None

    except s3.exceptions.NoSuchBucket:
        print(f"O bucket '{bucket_name}' não existe ou não está acessível.")
        return None
    except botocore.exceptions.ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"Erro de cliente AWS ({code}) ao tentar acessar {direct_key}.")
        return None
    except Exception as e:
        print(f"Erro inesperado ao tentar acessar {direct_key}: {str(e)}")
        return None

# Carregamentos em lote (injeta no escopo)
def carregar_parquets_em_variaveis(
    bucket: str = BUCKET,
    prefix: str = PREFIXO,
    nomes: Optional[List[str]] = None,
    destino: Optional[dict] = None,
    columns: Optional[Iterable[str]] = None,
    pattern: Optional[str] = None,
) -> Dict[str, str]:

    s3 = get_s3()
    destino = destino if destino is not None else globals()

    if nomes is None:
        keys = listar_parquets(bucket, prefix)
    else:
        idx = _index_por_basename(bucket, prefix)
        keys = []
        for n in nomes:
            base = n[:-8] if n.lower().endswith(".parquet") else n
            if base in idx:
                keys.extend(idx[base])
            else:
                keys.append(f"{prefix.rstrip('/')}/{n}.parquet")

    if not keys:
        print("Nenhum parquet encontrado.")
        return {}

    criados: Dict[str, str] = {}
    existentes = set(destino.keys())

    for key in keys:
        base = os.path.basename(key)
        base_no_ext = os.path.splitext(base)[0]
        if not _match_pattern(base_no_ext, pattern):
            continue

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()), columns=list(columns) if columns else None, engine="pyarrow")
            var_name = _nome_variavel(base, existentes)
            destino[var_name] = df
            existentes.add(var_name)
            criados[var_name] = f"s3://{bucket}/{key}"
            print(f"{var_name} ← {key}  | {len(df)} linhas")
        except s3.exceptions.NoSuchKey:
            print(f"Não encontrado: s3://{bucket}/{key}")
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"Erro de cliente AWS ({code}) ao ler {key}")
        except Exception as e:
            print(f"Erro lendo {key}: {e}")

    if not criados:
        print("Nenhum arquivo carregado (verifique 'nomes' e/ou 'pattern').")
    return criados

# Carregamentos em lote (retorna dict)
def carregar_parquets(
    bucket: str = BUCKET,
    prefix: str = PREFIXO,
    nomes: Optional[List[str]] = None,
    columns: Optional[Iterable[str]] = None,
    pattern: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Versão que retorna dict {nome_df: DataFrame}.
    Resolve arquivos mesmo estando em subpastas de 'prefix'.
    """
    s3 = get_s3()

    if nomes is None:
        keys = listar_parquets(bucket, prefix)
    else:
        idx = _index_por_basename(bucket, prefix)
        keys = []
        for n in nomes:
            base = n[:-8] if n.lower().endswith(".parquet") else n
            if base in idx:
                keys.extend(idx[base])
            else:
                keys.append(f"{prefix.rstrip('/')}/{n}.parquet")

    if not keys:
        print("Nenhum parquet encontrado.")
        return {}

    dfs: Dict[str, pd.DataFrame] = {}
    existentes = set()

    for key in keys:
        base = os.path.basename(key)
        base_no_ext = os.path.splitext(base)[0]
        if not _match_pattern(base_no_ext, pattern):
            continue

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(BytesIO(obj["Body"].read()), columns=list(columns) if columns else None, engine="pyarrow")
            var_name = _nome_variavel(base, existentes)
            dfs[var_name] = df
            existentes.add(var_name)
            print(f"{var_name} OK ({len(df)} linhas)  ← {key}")
        except s3.exceptions.NoSuchKey:
            print(f"Não encontrado: s3://{bucket}/{key}")
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"Erro de cliente AWS ({code}) ao ler {key}")
        except Exception as e:
            print(f"Erro lendo {key}: {e}")

    if not dfs:
        print("Nenhum DataFrame retornado (verifique 'nomes' e/ou 'pattern').")
    return dfs
