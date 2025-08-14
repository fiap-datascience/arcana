import boto3
import pandas as pd
import numpy as np
from io import BytesIO, StringIO
import warnings
import os
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings("ignore")


# Função para detectar o separador do CSV, asumi-se que o separador mais frequente na primeira linha é o correto
def detectar_separador(conteudo_decodificado):
    primeira_linha = conteudo_decodificado.splitlines()[0]
    if primeira_linha.count(';') > primeira_linha.count(','):
        return ';'
    else:
        return ','

# Função para acessar e ler um arquivo CSV do S3
def acessar_csv_s3(bucket, object_key, encoding='latin1'):
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client('s3',
                      aws_access_key_id=access_key_id,
                      aws_secret_access_key=secret_access_key,
                      region_name='us-east-2')

    # Baixa o conteúdo do arquivo CSV
    resposta = s3.get_object(Bucket=bucket, Key=f"bronze/{object_key}.csv")
    conteudo = resposta['Body'].read()

    # Decodifica o conteúdo para detectar o separador
    conteudo_decodificado = conteudo.decode(encoding)
    sep = detectar_separador(conteudo_decodificado)

    try:
        # Reencapsula o conteúdo em BytesIO para leitura com pandas
        df = pd.read_csv(BytesIO(conteudo_decodificado.encode(encoding)),
                         encoding=encoding,
                         sep=sep)
        
    except Exception as e:
        print(f"[ERRO] Falha ao ler {object_key}.csv: {e}")
        return None

    return df