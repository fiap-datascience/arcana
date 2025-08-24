import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore")

# Função para detectar o separador do CSV.
def detectar_separador(caminho_arquivo):
    with open(caminho_arquivo, 'r', encoding='latin1') as f:
        primeira_linha = f.readline()
        if primeira_linha.count(';') > primeira_linha.count(','):
            return ';'
        else:
            return ','

# Função para acessar e ler um arquivo CSV local
def acessar_csv_local(caminho_arquivo):
    """
    Acessa e lê um arquivo CSV do sistema de arquivos local.

    Args:
        caminho_arquivo (str): O caminho completo do arquivo CSV,
                               ex: 'caminho/para/seus/dados.csv'.

    Returns:
        pd.DataFrame: O DataFrame com os dados do CSV, ou None em caso de erro.
    """
    if not os.path.exists(caminho_arquivo):
        print(f"[ERRO] O arquivo não foi encontrado: {caminho_arquivo}")
        return None

    try:
        # Detecta o separador para garantir a leitura correta
        sep = detectar_separador(caminho_arquivo)
        
        df = pd.read_csv(caminho_arquivo, 
                         encoding='latin1', 
                         sep=sep)
        
    except Exception as e:
        print(f"[ERRO] Falha ao ler o arquivo {caminho_arquivo}: {e}")
        return None

    return df

# Exemplo de uso:
def main():
    arquivos = ['telemetria_11.csv','telemetria_6.csv','nps_transacional_onboarding.csv',
                'dados_clientes.csv','telemetria_4.csv','historico.csv','telemetria_5.csv',
                'telemetria_7.csv','nps_transacional_implantacao.csv','telemetria_9.csv','nps_transacional_suporte.csv',
                'telemetria_3.csv','telemetria_8.csv','telemetria_10.csv','clientes_desde.csv','mrr.csv',
                'nps_relacional.csv','telemetria_2.csv','nps_transacional_aquisicao.csv','contratacoes_ultimos_12_meses.csv',
                'tickets.csv','telemetria_1.csv','nps_transacional_produto.csv']
    
    for nome_arquivo in arquivos:
        # Define o caminho local para cada arquivo.
        # CORREÇÃO APLICADA AQUI: Adicionei o 'r' antes da string.
        caminho_completo = fr'C:\Users\Ana CSP\Desktop\challenge - fiap\arcana NO AWS\dados\raw\{nome_arquivo}'

        # Usa a nova função para ler o arquivo localmente.
        df_local = acessar_csv_local(caminho_completo)

        if df_local is not None:
            print(f"✅ Dados de '{nome_arquivo}' lidos com sucesso!")
            # Mostra as 5 primeiras linhas do DataFrame para verificar.
            print(df_local.head())
            print("\n" + "-"*30 + "\n")

# Se o script for executado diretamente, chame a função main.
if __name__ == '__main__':
    main()