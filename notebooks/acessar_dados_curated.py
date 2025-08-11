import boto3
import pandas as pd
from io import BytesIO
import os
from dotenv import load_dotenv
import warnings

load_dotenv()
warnings.filterwarnings("ignore")

# Função para acessar e ler um arquivo Parquet do S3
def acessar_parquet_s3(ARQUIVO, bucket_name=None, key_prefix="silver"):
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    if bucket_name is None:
        bucket_name = os.getenv("AWS_S3_BUCKET_NAME", "arcana-fiap")

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name='us-east-2'
    )

    key = f"{key_prefix}/{ARQUIVO}.parquet"

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        return df

    except s3.exceptions.NoSuchKey:
        print(f"Arquivo não encontrado no S3: s3://{bucket_name}/{key}")
        return None

    except s3.exceptions.NoSuchBucket:
        print(f"O bucket '{bucket_name}' não existe ou não está acessível.")
        return None

    except Exception as e:
        print(f"Erro inesperado ao tentar acessar {key}: {str(e)}")
        return None
