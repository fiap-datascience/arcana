import pandas as pd
import glob
import os

def consolidar_telemetria_local():
    """
    Consolida todos os arquivos de telemetria em um único arquivo Parquet.
    """
    CURATED_PATH = r"C:\Users\Ana CSP\Desktop\challenge - fiap\arcana NO AWS\dados\curated"
    OUTPUT_FILE = os.path.join(CURATED_PATH, "telemetria_consolidado.parquet")

    # Encontra todos os arquivos telemetria_*.parquet
    arquivos_telemetria = glob.glob(os.path.join(CURATED_PATH, "telemetria_*.parquet"))

    if not arquivos_telemetria:
        print("Nenhum arquivo de telemetria encontrado para consolidar. Abortando.")
        return

    print(f"Encontrados {len(arquivos_telemetria)} arquivos de telemetria. Consolidando...")

    # Lê cada arquivo e armazena em uma lista
    lista_dfs = []
    for arquivo in arquivos_telemetria:
        try:
            df_temp = pd.read_parquet(arquivo)
            lista_dfs.append(df_temp)
            print(f"✅ Arquivo '{os.path.basename(arquivo)}' lido com sucesso.")
        except Exception as e:
            print(f"❌ Falha ao ler o arquivo '{os.path.basename(arquivo)}': {e}")

    if not lista_dfs:
        print("Nenhum DataFrame válido para consolidar. Abortando.")
        return

    # Concatena todos os DataFrames em um único
    df_consolidado = pd.concat(lista_dfs, ignore_index=True)

    # Salva o arquivo consolidado
    df_consolidado.to_parquet(OUTPUT_FILE, index=False)

    print(f"\n✅ Consolidação concluída! Salvo em: {OUTPUT_FILE}")
    print(f"Tamanho do arquivo consolidado: {len(df_consolidado)} linhas.")

if __name__ == "__main__":
    consolidar_telemetria_local()