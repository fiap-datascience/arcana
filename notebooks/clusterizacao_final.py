# ==============================================================================
# 📦 Importação de Bibliotecas
# ==============================================================================
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.mixture import GaussianMixture, BayesianGaussianMixture
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import os
import boto3

from acessar_dados_curated import carregar_parquets_em_variaveis


# ==============================================================================
# 🔹 Carregamento das Tabelas
# ==============================================================================
print("🕜 Carregando as tabelas...\n")

carregar_parquets_em_variaveis(
    destino=globals(),
    nomes=[
        "clientes_desde", "codigos_paises", "contratacoes_ultimos_12_meses",
        "dados_clientes", "historico", "mrr", "nps_relacional",
        "nps_transacional_aquisicao", "nps_transacional_implantacao", 
        "nps_transacional_onboarding", "nps_transacional_produto", 
        "nps_transacional_suporte", "tickets", "telemetria_consolidado"
    ]
)

# Criando referências mais claras para os DataFrames
df_clientes_desde = clientes_desde
df_dados_clientes = dados_clientes
df_contratacoes_ultimos_12_meses = contratacoes_ultimos_12_meses
df_mrr = mrr
df_historico = historico
df_tickets = tickets
df_telemetria_consolidado = telemetria_consolidado
df_nps_relacional = nps_relacional
df_nps_transacional_aquisicao = nps_transacional_aquisicao
df_nps_transacional_implantacao = nps_transacional_implantacao
df_nps_transacional_onboarding = nps_transacional_onboarding
df_nps_transacional_produto = nps_transacional_produto
df_nps_transacional_suporte = nps_transacional_suporte

print("✅ Todas as tabelas foram carregadas corretamente!\n")
print("___________________________________________________ \n")


# ==============================================================================
# 🔹 Funções de Agregação
# ==============================================================================
def agg_dados_clientes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega informações contratuais e de segmentação por cliente.
    """
    return df.groupby("COD_CLIENTE").agg(
        n_produtos=("DS_PROD", "nunique"),
        vl_total_contrato=("VL_TOTAL_CONTRATO", "mean"),
        segmento=("DS_SEGMENTO", lambda x: x.mode().iloc[0] if len(x.mode())>0 else np.nan),
        subsegmento=("DS_SUBSEGMENTO", lambda x: x.mode().iloc[0] if len(x.mode())>0 else np.nan),
        porte=("FAT_FAIXA", lambda x: x.mode().iloc[0] if len(x.mode())>0 else np.nan),
        modal_comercio=("MODAL_COMERC", lambda x: x.mode().iloc[0] if len(x.mode())>0 else np.nan),
    ).reset_index()


def agg_contratacoes_12m(df: pd.DataFrame) -> pd.DataFrame:
    """
    Soma de contratações dos últimos 12 meses por cliente.
    """
    out = df.groupby("COD_CLIENTE").agg(
        n_contratos_12m=("QT_CONTRATACOES_12M", "sum"),
        vl_contratos_12m=("VL_CONTRATACOES_12M", "sum")
    ).reset_index()
    out["contratou_ult_12m"] = (out["n_contratos_12m"] > 0).astype(int)
    return out


def agg_nps_geral(df: pd.DataFrame, prefixo: str) -> pd.DataFrame:
    """
    Agrega notas médias de NPS por cliente e cria flag de resposta.
    """
    num_cols = [c for c in df.columns if c.startswith("NOTA")]
    grp = df.groupby("COD_CLIENTE")
    out = grp[num_cols].mean().add_prefix(f"{prefixo}_").reset_index()
    out[f"{prefixo}_respondeu"] = (grp.size() > 0).astype(int).reset_index(drop=True)
    return out


def agg_telemetria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega indicadores de uso da plataforma por cliente.
    """
    return df.groupby("COD_CLIENTE").agg(
        tele_eventos=("DURACAO_EVENTO","count"),
        tele_modulos=("ID_MODULO","nunique"),
        tele_linhas_produto=("ID_LINHA_PRODUTO","nunique"),
    ).reset_index()


def agg_tickets(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega quantidade de tickets e % de críticos por cliente.
    """
    if "PRIORIDADE_TICKET" in df.columns:
        df["is_critico"] = df["PRIORIDADE_TICKET"].astype(str).str.lower().isin(
            ["low","normal","high","urgent"]
        ).astype(int)
    else:
        df["is_critico"] = 0

    return df.groupby("COD_CLIENTE").agg(
        n_tickets=("BK_TICKET","count"),
        pct_tickets_criticos=("is_critico","mean")
    ).reset_index()


def agg_historico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega histórico de propostas por cliente.
    """
    return df.groupby("COD_CLIENTE").agg(
        n_propostas=("NR_PROPOSTA","nunique"),
        vl_total_propostas=("VL_TOTAL","sum"),
        prc_unitario_medio=("PRC_UNITARIO","mean")
    ).reset_index()


def left_merge(a: pd.DataFrame, b: pd.DataFrame, on="COD_CLIENTE") -> pd.DataFrame:
    """Realiza um merge à esquerda entre dois DataFrames."""
    return a.merge(b, on=on, how="left")


# ==============================================================================
# 🔹 Preparação das Features
# ==============================================================================
print("🕜 Iniciando a preparação das Features... \n")

HOJE = pd.to_datetime("today")

base = df_clientes_desde.copy()
base["TEMPO_CONTRATO_DIAS"] = (HOJE - base["DATA_CLIENTE_DESDE"]).dt.days
base["TEMPO_CONTRATO_MESES"] = base["TEMPO_CONTRATO_DIAS"] // 30
base["TEMPO_CONTRATO_ANOS"] = base["TEMPO_CONTRATO_DIAS"] // 365

# Normalizando código de cliente
df_telemetria_consolidado["COD_CLIENTE"] = df_telemetria_consolidado["COD_CLIENTE"].str[:-2]

# Executa agregações
agg_parts = [
    agg_dados_clientes(df_dados_clientes),
    agg_contratacoes_12m(df_contratacoes_ultimos_12_meses),
    df_mrr,
    agg_nps_geral(df_nps_relacional, "nps_rel"),
    agg_nps_geral(df_nps_transacional_aquisicao, "nps_aquis"),
    agg_nps_geral(df_nps_transacional_implantacao, "nps_impl"),
    agg_nps_geral(df_nps_transacional_onboarding, "nps_onb"),
    agg_nps_geral(df_nps_transacional_suporte, "nps_sup"),
    agg_telemetria(df_telemetria_consolidado),
    agg_tickets(df_tickets),
    agg_historico(df_historico)
]

# Merge progressivo
df = base.copy()
for part in agg_parts:
    df = left_merge(df, part)

# Tratamento de faltantes
count_cols = ["n_produtos","n_contratos_12m","contratou_ult_12m",
              "tele_eventos","tele_modulos","tele_linhas_produto",
              "n_tickets","n_propostas"]
for c in count_cols:
    if c in df.columns:
        df[c] = df[c].fillna(0)

for c in [col for col in df.columns if c.endswith("_respondeu")]:
    df[c] = df[c].fillna(0).astype(int)

# Seleção de features finais
df_final = df[["COD_CLIENTE", "TEMPO_CONTRATO_MESES", "n_produtos",
               "vl_total_contrato", "segmento", "porte", "n_contratos_12m",
               "tele_eventos", "n_tickets", "n_propostas"]]

# Mapping de porte
dict_porte = {
    'Sem Informações de Faturamento': 0,
    'Faixa 00 - Ate 4,5 M': 1,
    'Faixa 01 - De 4,5 M ate 7,5 M': 2,
    'Faixa 02 - De 7,5 M ate 15 M': 3,
    'Faixa 03 - De 15 M ate 25 M': 4,
    'Faixa 04 - De 25 M ate 35 M': 5,
    'Faixa 05 - De 35 M ate 50 M': 6,
    'Faixa 06 - De 50 M ate 75 M': 7,
    'Faixa 07 - De 75 M ate 150 M': 8,
    'Faixa 08 - De 150 M ate 300 M': 9,
    'Faixa 09 - De 300 M ate 500 M': 10,
    'Faixa 10 - De 500 M ate 850 M': 11,
    'Faixa 11 - Acima de 850 M': 12
}
df_final['PORTE_CLASSIFICACAO'] = df['porte'].map(dict_porte)

print("✅ Features preparadas para o modelo!\n")
print("___________________________________________________ \n")

print("🕜 Iniciando a Clusterização...\n")

# ==============================================================================
# 🔹 Modelagem com GMM Bayesiano
# ==============================================================================
features_para_modelo = df_final.drop(
    ['COD_CLIENTE', 'porte', 'segmento', 'PORTE_CLASSIFICACAO', 'n_propostas'], 
    axis=1
)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(features_para_modelo)

# Avaliando AIC e BIC
n_components = range(2, 11)
aics, bics = [], []

print("📊 Calculando AIC e BIC para diferentes números de clusters...")
for k in n_components:
    gmm = GaussianMixture(n_components=k, random_state=42, n_init=10)
    gmm.fit(X_scaled)
    aics.append(gmm.aic(X_scaled))
    bics.append(gmm.bic(X_scaled))

optimal_k = bics.index(min(bics))
print(f"📊 Número ideal de clusters (k) encontrado pelo BIC: {optimal_k}\n")

# Rodando Bayesian GMM
print("🕜 Iniciando a Clusterização com GMM Bayesiano...\n")

bgm = BayesianGaussianMixture(
    n_components=10,
    covariance_type='diag',
    n_init=10,
    random_state=42,
    weight_concentration_prior=0.1
)
bgm.fit(X_scaled)

pesos_dos_clusters = bgm.weights_
clusters_ativos = np.sum(pesos_dos_clusters > 0.01)
print(f"✅ O modelo encontrou {clusters_ativos} clusters efetivos! \n")

# Rodando modelo final com clusters ativos
bgm2 = BayesianGaussianMixture(
    n_components=clusters_ativos,
    covariance_type='diag',
    n_init=10,
    random_state=42,
    weight_concentration_prior=0.1
)
bgm2.fit(X_scaled)

df_clusterizado = df_final.copy()
df_clusterizado['Cluster_Bayesiano'] = bgm2.predict(X_scaled)

# Perfil dos clusters
perfil_bayesiano = df_clusterizado.groupby('Cluster_Bayesiano').mean(numeric_only=True)
perfil_bayesiano['N_Clientes'] = df_clusterizado['Cluster_Bayesiano'].value_counts()

print("📊 --- Perfil dos Clusters Encontrados pelo Modelo Bayesiano ---")
print(perfil_bayesiano)
print("\n✅ Clusterização Finalizada! \n")



# ==============================================================================
# 🔹 Envio da base clusterizada para a AWS
# ==============================================================================

print("🕜 Enviando os resultados para o S3... \n")
# Conexão S3
access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

s3 = boto3.client('s3',
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                region_name = 'us-east-2')

# Converter para Parquet em memória
buffer = BytesIO()
df_clusterizado.to_parquet(buffer, index=False, engine="pyarrow") 
buffer.seek(0)

# Configurar cliente S3
s3 = boto3.client("s3")

# Subir para o bucket
s3.put_object(Bucket="arcana-fiap", Key="gold/tb_clientes_clusterizados/tb_clientes_clusterizados.parquet", Body=buffer.getvalue())

print("✅ Envio finalizado! \n")

print("✅ Clusterização Concluída com Sucesso! \n")