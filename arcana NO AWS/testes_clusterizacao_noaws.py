import os
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.manifold import TSNE

# -----------------
# 1. FUNÇÕES DE ACESSO E PRÉ-PROCESSAMENTO
# -----------------

def acessar_parquet_local(nome_arquivo):
    """
    Carrega um arquivo Parquet de um diretório local em um DataFrame do Pandas.
    """
    # CORREÇÃO: O caminho foi definido com base na localização dos seus arquivos
    CURATED_PATH = r"C:\Users\Ana CSP\Desktop\challenge - fiap\arcana NO AWS\dados\curated"
    caminho_completo = os.path.join(CURATED_PATH, nome_arquivo + ".parquet")
    
    if not os.path.exists(caminho_completo):
        print(f"[ERRO] Arquivo não encontrado: {caminho_completo}")
        return None
    
    try:
        df = pd.read_parquet(caminho_completo)
        print(f"✅ Arquivo '{nome_arquivo}.parquet' carregado com sucesso.")
        return df
    except Exception as e:
        print(f"[ERRO] Falha ao carregar o arquivo '{nome_arquivo}.parquet': {e}")
        return None

def carregar_dados_iniciais():
    """
    Carrega todos os arquivos .parquet da pasta 'curated' em um dicionário.
    """
    nomes = ["clientes_desde", "contratacoes_ultimos_12_meses", "dados_clientes", "historico",
             "mrr", "nps_relacional", "nps_transacional_aquisicao", "nps_transacional_implantacao", 
             "nps_transacional_onboarding", "nps_transacional_produto", "nps_transacional_suporte",
             "tickets", "telemetria_consolidado", "codigos_paises"]
    
    dataframes = {}
    for nome in nomes:
        df = acessar_parquet_local(nome)
        if df is not None:
            dataframes[nome] = df
    return dataframes

def preparar_dados(dataframes):
    """
    Realiza a preparação e junção dos dados de todas as tabelas.
    Retorna um único DataFrame final pronto para a clusterização.
    """
    necessarios = ["clientes_desde", "dados_clientes", "contratacoes_ultimos_12_meses", 
                   "mrr", "nps_relacional", "tickets", "codigos_paises"]
    if not all(df in dataframes for df in necessarios):
        print("❌ Dados necessários para a preparação não encontrados. Abortando.")
        return None

    df_nps_relacional_agg = dataframes["nps_relacional"].groupby("COD_CLIENTE").agg(
        **{f"NOTA_NPS_{col.split('_')[-1]}": (col, "mean") for col in dataframes["nps_relacional"].columns if "NOTA_" in col}
    ).reset_index()

    df_tickets_agg = dataframes["tickets"].groupby("COD_CLIENTE").agg(TICKETS=("BK_TICKET", "count")).reset_index()
    
    nps_transacionais_agg = {}
    for nome in ["nps_transacional_aquisicao", "nps_transacional_implantacao", 
                 "nps_transacional_onboarding", "nps_transacional_suporte", "nps_transacional_produto"]:
        df_nps = dataframes.get(nome)
        if df_nps is not None:
            col_nota = "NOTA_RECOMENDACAO" if "onboarding" in nome else "NOTA_NPS"
            agg_col = f"NOTA_NPS_{nome.replace('nps_transacional_', '').replace('.csv', '').upper()}"
            agg_df = df_nps.groupby("COD_CLIENTE").agg(**{agg_col: (col_nota, "mean")}).reset_index()
            nps_transacionais_agg[nome] = agg_df

    df_principal = dataframes["clientes_desde"].merge(dataframes["dados_clientes"], on="COD_CLIENTE", how="left")
    df_principal = df_principal.merge(dataframes["codigos_paises"][["CODIGO", "PAIS"]], left_on="PAIS_CLIENTE", right_on="CODIGO", how="left").drop(columns=["CODIGO", "PAIS_CLIENTE"])
    df_principal = df_principal.merge(dataframes["contratacoes_ultimos_12_meses"], on="COD_CLIENTE", how="left")
    df_principal = df_principal.merge(dataframes["mrr"], on="COD_CLIENTE", how="left")
    df_principal = df_principal.merge(df_nps_relacional_agg, on="COD_CLIENTE", how="left")
    df_principal = df_principal.merge(df_tickets_agg, on="COD_CLIENTE", how="left")

    for df_agg in nps_transacionais_agg.values():
        df_principal = df_principal.merge(df_agg, on="COD_CLIENTE", how="left")

    hoje = pd.to_datetime("today")
    df_principal["MESES_CADASTRO"] = ((hoje - df_principal["DATA_CLIENTE_DESDE"]).dt.days // 30)
    
    colunas_finais = df_principal.columns.tolist()
    colunas_finais.remove("DATA_CLIENTE_DESDE")
    colunas_finais.remove("CIDADE_CLIENTE")
    
    return df_principal[colunas_finais]

def pre_processar_dados_para_cluster(df, features_num, features_cat):
    """
    Trata dados faltantes, escala e codifica as features para o modelo de clusterização.
    """
    df_processado = df.copy()

    df_processado[features_num] = df_processado[features_num].fillna(0)
    df_processado[features_cat] = df_processado[features_cat].fillna("Desconhecido")

    df_encoded = pd.get_dummies(df_processado[features_cat], drop_first=True)

    scaler = StandardScaler()
    X_num = scaler.fit_transform(df_processado[features_num])

    X = np.hstack([X_num, df_encoded.values])
    
    print("Shape final do conjunto de features:", X.shape)
    
    return X, df_processado.set_index("COD_CLIENTE")


# -----------------
# 2. FLUXO PRINCIPAL DO PROGRAMA
# -----------------

def main():
    print("Iniciando o teste de clusterização...")
    
    dataframes_curated = carregar_dados_iniciais()
    if dataframes_curated is None:
        return

    df_clientes_consolidado = preparar_dados(dataframes_curated)
    if df_clientes_consolidado is None:
        return

    features_num = [
        "MESES_CADASTRO", "QT_CONTRATACOES_12M", "VL_CONTRATACOES_12M", "MRR_12M", "TICKETS",
        "NOTA_NPS_RELACIONAL", "NOTA_NPS_AQUISICAO", "NOTA_NPS_IMPLANTACAO", "NOTA_NPS_ONBOARDING",
        "NOTA_NPS_PRODUTO", "NOTA_NPS_SUPORTE", "NOTA_SUPTEC_AGILIDADE", "NOTA_SUPTEC_ATENDIMENTO",
        "NOTA_COMERCIAL", "NOTA_CUSTOS", "NOTA_ADMFIN_ATENDIMENTO", "NOTA_SOFTWARE", "NOTA_SOFTWARE_ATUALIZACAO"
    ]
    features_cat = ["PAIS", "UF_CLIENTE", "FAT_FAIXA", "DS_SEGMENTO", "DS_SUBSEGMENTO"]
    
    df_clientes_consolidado = df_clientes_consolidado.rename(columns={"NOTA_NPS": "NOTA_NPS_RELACIONAL"})
    
    X, df_clientes_modelo = pre_processar_dados_para_cluster(df_clientes_consolidado, features_num, features_cat)

    inertia = []
    silhouette_scores = []
    K = range(2, 11)

    for k in K:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        inertia.append(km.inertia_)
        if len(np.unique(labels)) > 1:
            silhouette_scores.append(silhouette_score(X, labels))
        else:
            silhouette_scores.append(0)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].plot(K, inertia, "o-")
    axes[0].set_xlabel("Número de clusters (k)")
    axes[0].set_ylabel("Inércia")
    axes[0].set_title("Método do Cotovelo")
    axes[1].plot(K, silhouette_scores, "o-", color="green")
    axes[1].set_xlabel("Número de clusters (k)")
    axes[1].set_ylabel("Silhouette Score")
    axes[1].set_title("Coeficiente de Silhouette")
    plt.tight_layout()
    plt.show()

    print("\nAplicando a clusterização final...")
    k_escolhido = 4
    kmeans = KMeans(n_clusters=k_escolhido, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)
    df_clientes_modelo["cluster"] = labels

    print("\nDistribuição de clientes por cluster:")
    print(df_clientes_modelo["cluster"].value_counts(normalize=True).mul(100).round(2))

    print("\nMédias das variáveis numéricas por cluster:")
    print(df_clientes_modelo.groupby("cluster")[features_num].mean().round(2))
    
    print("\nDistribuição das variáveis categóricas por cluster:")
    for col in features_cat:
        print(f"\n--- {col} ---")
        print(df_clientes_modelo.groupby("cluster")[col].value_counts(normalize=True).mul(100).round(2))

    print("\nCriando visualização com t-SNE...")
    tsne = TSNE(n_components=2, perplexity=30, random_state=42)
    X_tsne = tsne.fit_transform(X)
    
    df_plot = pd.DataFrame(X_tsne, columns=["Dim1", "Dim2"], index=df_clientes_modelo.index)
    df_plot["cluster"] = df_clientes_modelo["cluster"]
    
    plt.figure(figsize=(10, 7))
    for c in sorted(df_plot["cluster"].unique()):
        subset = df_plot[df_plot["cluster"] == c]
        plt.scatter(subset["Dim1"], subset["Dim2"], label=f"Cluster {c}", alpha=0.6)
    
    plt.xlabel("Dim 1")
    plt.ylabel("Dim 2")
    plt.title("Visualização da Clusterização (t-SNE)")
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()






