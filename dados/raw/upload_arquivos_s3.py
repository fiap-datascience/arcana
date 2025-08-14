import boto3
import warnings
import os

warnings.filterwarnings("ignore")

def upload_para_s3(arquivo):
    # Configurações do S3
    access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client('s3',
                  aws_access_key_id=access_key_id,
                  aws_secret_access_key=secret_access_key,
                  region_name = 'us-east-2')

    # Carregar o arquivo CSV para o S3
    s3.upload_file(arquivo, 
                   'arcana-fiap', 
                   f'bronze/{os.path.basename(arquivo)}')
    
    print(f'Arquivo {os.path.basename(arquivo)} enviado para o S3 com sucesso!')


# Função principal
def main():
    
    arquivos = ['telemetria_11.csv','telemetria_6.csv','nps_transacional_onboarding.csv',
                'dados_clientes.csv','telemetria_4.csv','historico.csv','telemetria_5.csv',
                'telemetria_7.csv','nps_transacional_implantacao.csv','telemetria_9.csv','nps_transacional_suporte.csv',
                'telemetria_3.csv','telemetria_8.csv','telemetria_10.csv','clientes_desde.csv','mrr.csv',
                'nps_relacional.csv','telemetria_2.csv','nps_transacional_aquisicao.csv','contratacoes_ultimos_12_meses.csv',
                'tickets.csv','telemetria_1.csv','nps_transacional_produto.csv']
    
    for arquivo in arquivos:

        caminho = f'dados/raw/{arquivo}'
        upload_para_s3(caminho)


    print("✅ Todos os arquivos foram enviados para o S3 com sucesso!")

if __name__ == '__main__':
    main()