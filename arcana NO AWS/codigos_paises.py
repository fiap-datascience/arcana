import pandas as pd
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time

CURATED_PATH = r"C:\Users\Ana CSP\Desktop\challenge - fiap\arcana NO AWS\dados\curated"
CAMINHO_ARQUIVO = os.path.join(CURATED_PATH, "dados_clientes.parquet")
df_clientes = pd.read_parquet(CAMINHO_ARQUIVO)
#Resultado: [105 586  63 249 676  40 845 589 239  97 607 158]

#A partir disso, pretende-se descobrir o codigo de cada país a partir da cidade:
df_pais_586 = df_clientes[df_clientes['PAIS_CLIENTE'] == 586]
#['DDELESTE' 'PARAGUAY' 'ESTRANGEIRO' 'EXTRANGEIRO']

df_pais_63 = df_clientes[df_clientes['PAIS_CLIENTE'] == 63]
#['ESTRANGEIRO' 'ROSARIOSANTAFE' 'PURMAMARCA' 'CABA' 'COMODORARIVADAVIA'
# 'CORDOBA' 'MENDOZA' 'TUCUMAN' 'CHACO' 'CORRIENTES' 'SALTA' 'PALERMO'
# 'CALAFATE' 'IGUAZU' 'ELCALAFATESANTACRUZ' 'CDADAUTONOMADEBUENOSAIRES'
# 'SANJUAN' 'BARILOCHE' 'PUERTOIGUAZU' 'VILLADEMERLOSANLUIS' 'SANNICOLAS']

df_pais_249 = df_clientes[df_clientes['PAIS_CLIENTE'] == 249]
#['ESTRANGEIRO']

df_pais_676 = df_clientes[df_clientes['PAIS_CLIENTE'] == 676]
#['ESTRANGEIRO' 'MOSCOW']

df_pais_40 = df_clientes[df_clientes['PAIS_CLIENTE'] == 40]
#['ESTRANGEIRO']

df_pais_845 = df_clientes[df_clientes['PAIS_CLIENTE'] == 845]
#['ESTRANGEIRO' 'URUGUAY']

df_pais_589 = df_clientes[df_clientes['PAIS_CLIENTE'] == 589]
#['ESTRANGEIRO']

df_pais_239 = df_clientes[df_clientes['PAIS_CLIENTE'] == 239]
#['ESTRANGEIRO']

df_pais_97 = df_clientes[df_clientes['PAIS_CLIENTE'] == 97]
#['LAPAZBOLIVIA' 'BOLIVIA']

df_pais_607 = df_clientes[df_clientes['PAIS_CLIENTE'] == 607]
#['ELVAS']

df_pais_158 = df_clientes[df_clientes['PAIS_CLIENTE'] == 158]
#['ESTRANGEIRO' 'SANTIAGODECHILE']

#Os códigos 249, 40, 589 e 239: Para esses códigos, a única informação disponível é ['ESTRANGEIRO'] ou ['EXTRANGEIRO']
#Nesses casos, decidiu-se pesquisar na internet o tipo de lista em que o Brasil é o código 105. 
#Descobriu-se que se refere a lista a tabela oficial da Receita Federal / Siscomex / SPED (fiscal/aduaneiro) (Anexo III da IN RFB nº 1.836/2018 e atualizações).
#A partir dessa lista foi possível criar o dicionário:
mapeamento_paises = {
    105: 'BRASIL',
    586: 'PARAGUAI',
    63: 'ARGENTINA',
    249: 'EUA',
    40: 'ANGOLA',
    589: 'PERU',
    239: 'EQUADOR',
    676: 'RUSSIA',
    845: 'URUGUAI',
    97: 'BOLIVIA',
    158: 'CHILE',
    607: 'PORTUGAL'
}
#Substituir o codigo dos países pelos nomes
df_clientes['PAIS_CLIENTE'] = df_clientes['PAIS_CLIENTE'].map(mapeamento_paises)

#Substituir 'EXTRANGEIRO' por 'ESTRANGEIRO' na tabela de CIDADE_CLIENTE:
df_clientes['CIDADE_CLIENTE'] = df_clientes['CIDADE_CLIENTE'].replace('EXTRANGEIRO', 'ESTRANGEIRO')

#Criar um Parquet atualizado dados_clientes_ana 
NOVO_CAMINHO_ARQUIVO = os.path.join(CURATED_PATH, "dados_clientes_ana.parquet")
# Salve o dataframe atualizado no novo arquivo Parquet
df_clientes.to_parquet(NOVO_CAMINHO_ARQUIVO)
print(f"O novo dataframe foi salvo com sucesso em: {NOVO_CAMINHO_ARQUIVO}")
#----------------------------------------------------------------------------------------------------------------------------------------------------------------
#CALCULAR A LOCALIZAÇÃO (Lat, Long)
#Como todas as cidades 
#Todas as cidades estrangeiras em que estiver escrito ESTRANGEIRO substituir pela capital
mapeamento_capitais_estrangeiras = {
    'PARAGUAI': 'ASSUNCAO',
    'ARGENTINA': 'BUENOSAIRES', 
    'EUA': 'WASHINGTONDC',
    'ANGOLA': 'LUANDA',
    'PERU': 'LIMA',
    'EQUADOR': 'QUITO',
    'RUSSIA': 'MOSCOW',
    'URUGUAI': 'MONTEVIDEU',
    'BOLIVIA': 'LA PAZ',
    'CHILE': 'SANTIAGO',
    'PORTUGAL': 'LISBOA'
}

#As cidades estrangeiras que estiverem categorizadas como "ESTRANGEIRO" serão substituídas pelas capitais:
condicao = (df_clientes['PAIS_CLIENTE'] != 'BRASIL') & (df_clientes['CIDADE_CLIENTE'] == 'ESTRANGEIRO')
df_clientes.loc[condicao, 'CIDADE_CLIENTE'] = df_clientes['PAIS_CLIENTE'].map(mapeamento_capitais_estrangeiras)

#Criação de um DF que contém a lista de combinações unicas (PAIS, CIDADE)
df_localizacao_unica = df_clientes[['PAIS_CLIENTE', 'CIDADE_CLIENTE']].drop_duplicates() 
print(f"Total de combinações únicas de cidade/país: {len(df_localizacao_unica)}")

# --- Geocodificação das Localizações Únicas ---
geolocator = Nominatim(user_agent="meu_aplicativo_geocodificacao")

def get_coordinates(row):
    try:
        # Adicionar uma pequena pausa para não sobrecarregar a API
        time.sleep(1.2)
        location_name = f"{row['CIDADE_CLIENTE']}, {row['PAIS_CLIENTE']}"
        location = geolocator.geocode(location_name, timeout=10)
        
        if location:
            print(f"Encontrado: {location_name} -> ({location.latitude}, {location.longitude})")
            return pd.Series([location.latitude, location.longitude])
        else:
            print(f"Não encontrado: {location_name}")
            return pd.Series([None, None])
            
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        print(f"Erro de geocodificação para {location_name}: {e}")
        return pd.Series([None, None])
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")
        return pd.Series([None, None])

print("\nIniciando a geocodificação...")
# Aplicar a função para obter as coordenadas
df_localizacao_unica[['LATITUDE', 'LONGITUDE']] = df_localizacao_unica.apply(get_coordinates, axis=1)

print("\nGeocodificação concluída. Exemplo do DataFrame com Lat/Long:")
print(df_localizacao_unica.head())

# --- Salvando o DataFrame de Localizações Únicas ---
NOVO_CAMINHO_ARQUIVO = os.path.join(CURATED_PATH, "df_localizacao_unica_com_lat_long.parquet")
df_localizacao_unica.to_parquet(NOVO_CAMINHO_ARQUIVO)
print(f"\nO DataFrame de localizações únicas foi salvo em: {NOVO_CAMINHO_ARQUIVO}")

