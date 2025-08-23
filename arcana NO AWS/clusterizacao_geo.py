import pandas as pd
import os
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.metrics import silhouette_score
import folium
import matplotlib.cm as cm
import matplotlib.colors as colors
import matplotlib.pyplot as plt
#-----------------------------------------------------------------------------------------------------------------------------------------------
#IMPORTAR DADOS
#-----------------------------------------------------------------------------------------------------------------------------------------------
#Importar dados silver
CURATED_PATH = r"C:\Users\Ana CSP\Desktop\challenge - fiap\arcana NO AWS\dados\curated"
CAMINHO_ARQUIVO = os.path.join(CURATED_PATH, "dados_clientes.parquet")
df_clientes = pd.read_parquet(CAMINHO_ARQUIVO)

#Importar dados de localizacao
CAMINHO_ARQUIVO_LOCALIZACAO = os.path.join(CURATED_PATH, "df_localizacao_unica_atualizada.parquet")
df_localizacao = pd.read_parquet(CAMINHO_ARQUIVO_LOCALIZACAO)
#-----------------------------------------------------------------------------------------------------------------------------------------------
#PREPARACAO DE DADOS
#-----------------------------------------------------------------------------------------------------------------------------------------------
# --- 2. PADRONIZAR AS COLUNAS CHAVE PARA O MERGE ---
# Dicionário para mapear códigos de país para nomes
mapeamento_paises = {
    105: 'BRASIL', 586: 'PARAGUAI', 63: 'ARGENTINA', 249: 'EUA', 40: 'ANGOLA', 589: 'PERU', 239: 'EQUADOR',
    676: 'RUSSIA', 845: 'URUGUAI', 97: 'BOLIVIA', 158: 'CHILE', 607: 'PORTUGAL'
}
# Dicionário para corrigir nomes de cidades
mapeamento_cidades = {
    'SAOPAULO': 'SAO PAULO', 'SANTOANDRE': 'SANTO ANDRE', 'SAOBERNARDODOCAMPO': 'SAO BERNARDO DO CAMPO',
    'NOVAODESSA': 'NOVA ODESSA', 'CACHOEIRODEITAPEMIRIM': 'CACHOEIRO DE ITAPEMIRIM', 'MOGIDASCRUZES': 'MOGI DAS CRUZES',
    'RIOGRANDEDASERRA': 'RIO GRANDE DA SERRA', 'PORTOALEGRE': 'PORTO ALEGRE', 'BELOHORIZONTE': 'BELO HORIZONTE',
    'SAOJOSEDOSCAMPOS': 'SAO JOSE DOS CAMPOS', 'NOVALIMA': 'NOVA LIMA', 'TABOAODASERRA': 'TABOAO DA SERRA',
    'NOVAFRIBURGO': 'NOVA FRIBURGO', 'RIBEIRAOPRETO': 'RIBEIRAO PRETO', 'CAXIASDOSUL': 'CAXIAS DO SUL',
    'TRESCORACOES': 'TRES CORACOES', 'PORTOFERREIRA': 'PORTO FERREIRA', 'CHAPADADOSGUIMARAES': 'CHAPADA DOS GUIMARAES',
    'PONTAGROSSA': 'PONTA GROSSA', 'MORRODAFUMACA': 'MORRO DA FUMACA', 'FLORESDACUNHA': 'FLORES DA CUNHA',
    'SAOBENTODOSUL': 'SAO BENTO DO SUL', 'SAOJOSEDOSPINHAIS': 'SAO JOSE DOS PINHAIS', 'MOJIMIRIM': 'MOJI MIRIM',
    'MATIASBARBOSA': 'MATIAS BARBOSA', 'SAOCAETANODOSUL': 'SAO CAETANO DO SUL', 'LAGOASANTA': 'LAGOA SANTA',
    'NOVAIGUACU': 'NOVA IGUACU', 'SANTABARBARADOESTE': 'SANTA BARBARA DO OESTE', 'SANTAISABEL': 'SANTA ISABEL',
    'SANTARITADOSAPUCAI': 'SANTA RITA DO SAPUCAI', 'JUIZDEFORA': 'JUIZ DE FORA', 'JOAOPESSOA': 'JOAO PESSOA',
    'CAMPOLARGO': 'CAMPO LARGO', 'TRESLAGOAS': 'TRES LAGOAS', 'CORUMBADEGOIAS': 'CORUMBA DE GOIAS',
    'CONSELHEIROPENA': 'CONSELHEIRO PENA', 'BENTOGONCALVES': 'BENTO GONCALVES', 'DDELESTE': 'CIUDAD DEL ESTE',
    'POUSOALEGRE': 'POUSO ALEGRE', 'VARZEAGRANDE': 'VARZEA GRANDE', 'NOVOHAMBURGO': 'NOVO HAMBURGO',
    'SAOCARLOS': 'SAO CARLOS', 'PONTENOVA': 'PONTE NOVA', 'CAMPOMOURAO': 'CAMPO MOURAO', 'ARROIODOMEIO': 'ARROIO DO MEIO',
    'DUQUEDECAXIAS': 'DUQUE DE CAXIAS', 'BRAGANCAPAULISTA': 'BRAGANCA PAULISTA', 'SAOLUIS': 'SAO LUIS',
    'PATOSDEMINAS': 'PATOS DE MINAS', 'VENANCIOAIRES': 'VENANCIO AIRES', 'PIMENTABUENO': 'PIMENTA BUENO',
    'SANTACRUZDOSUL': 'SANTA CRUZ DO SUL', 'VITORIADESANTOANTAO': 'VITORIA DE SANTO ANTAO',
    'SAOJOAODEMERITI': 'SAO JOAO DE MERITI', 'DOISCORREGOS': 'DOIS CORREGOS', 'SIMOESFILHO': 'SIMOES FILHO',
    'SANTANADEPARNAIBA': 'SANTANA DE PARNAIBA', 'FERNANDODENORONHA': 'FERNANDO DE NORONHA',
    'MATADESAOJOAO': 'MATA DE SAO JOAO', 'GOVERNADORVALADARES': 'GOVERNADOR VALADARES',
    'PRESIDENTEPRUDENTE': 'PRESIDENTE PRUDENTE', 'APARECIDADEGOIANIA': 'APARECIDA DE GOIANIA',
    'SAOLEOPOLDO': 'SAO LEOPOLDO', 'CARMODOPARANAIBA': 'CARMO DO PARANAIBA',
    'SAOMIGUELDOSCAMPOS': 'SAO MIGUEL DOS CAMPOS', 'SAOJOSEDORIOPRETO': 'SAO JOSE DO RIO PRETO',
    'SANTAMARIADEJETIBA': 'SANTA MARIA DE JETIBA', 'CAMPINAGRANDE': 'CAMPINA GRANDE',
    'PATROCINIOPAULISTA': 'PATROCINIO PAULISTA', 'CAMPOGRANDE': 'CAMPO GRANDE', 'SANTACECILIA': 'SANTA CECILIA',
    'RIBEIRAODASNEVES': 'RIBEIRAO DAS NEVES', 'SERAFINACORREA': 'SERAFINA CORREA',
    'SAOSEBASTIAODOPARAISO': 'SAO SEBASTIAO DO PARAISO', 'TRESPONTAS': 'TRES PONTAS', 'VARZEAPAULISTA': 'VARZEA PAULISTA',
    'FRANCISCOBELTRAO': 'FRANCISCO BELTRAO', 'SENADORCANEDO': 'SENADOR CANEDO', 'FOZDOIGUACU': 'FOZ DO IGUACU',
    'PEREIRABARRETO': 'PEREIRA BARRETO', 'NOVOMUNDO': 'NOVO MUNDO', 'SETELAGOAS': 'SETE LAGOAS',
    'RIOBRANCO': 'RIO BRANCO', 'PORTOVELHO': 'PORTO VELHO', 'CAMPINAVERDE': 'CAMPINA VERDE',
    'PEDROLEOPOLDO': 'PEDRO LEOPOLDO', 'SANTARITADOPASSAQUATRO': 'SANTA RITA DO PASSA QUATRO',
    'CABOFRIO': 'CABO FRIO', 'CAPIVARIDEBAIXO': 'CAPIVARI DE BAIXO',
    'SAOLOURENCODAMATA': 'SAO LOURENCO DA MATA', 'EMBUDASARTES': 'EMBU DAS ARTES', 'PIRESDORIO': 'PIRES DO RIO',
    'SANTANADOPARAISO': 'SANTANA DO PARAISO', 'NOVAVENEZA': 'NOVA VENEZA',
    'JABOATAODOSGUARARAPES': 'JABOATAO DOS GUARARAPES', 'CORONELFABRICIANO': 'CORONEL FABRICIANO',
    'TRESRIOS': 'TRES RIOS', 'BELOJARDIM': 'BELO JARDIM', 'MONTESCLAROS': 'MONTES CLAROS',
    'PARADEMINAS': 'PARA DE MINAS', 'NOVAOLIMPIA': 'NOVA OLIMPIA', 'SAOJOSEDALAPA': 'SAO JOSE DA LAPA',
    'DIASDAVILA': 'DIAS D AVILA', 'SAORAIMUNDODASMANGABEIRAS': 'SAO RAIMUNDO DAS MANGABEIRAS',
    'PAULOAFONSO': 'PAULO AFONSO', 'POCOSDECALDAS': 'POCOS DE CALDAS', 'RIOCLARO': 'RIO CLARO',
    'SEBASTIAOLEAL': 'SEBASTIAO LEAL', 'LUCASDORIOVERDE': 'LUCAS DO RIO VERDE', 'ABREUELIMA': 'ABREU E LIMA',
    'CAMPOSDOSGOYTACAZES': 'CAMPOS DOS GOYTACAZES', 'CABODESANTOAGOSTINHO': 'CABO DE SANTO AGOSTINHO',
    'BOMDESPACHO': 'BOM DESPACHO', 'ANGRADOSREIS': 'ANGRA DOS REIS',
    'CORNELIOPROCOPIO': 'CORNELIO PROCOPIO', 'FEIRADESANTANA': 'FEIRA DE SANTANA',
    'NOVABASSANO': 'NOVA BASSANO', 'RIODASOSTRAS': 'RIO DAS OSTRAS', 'SANTAFEDOSUL': 'SANTA FE DO SUL',
    'BARRAFUNDA': 'BARRA FUNDA', 'SAOFRANCISCO': 'SAO FRANCISCO', 'UNIAODAVITORIA': 'UNIAO DA VITORIA',
    'MARECHALDEODORO': 'MARECHAL DEODORO', 'JUAZEIRODONORTE': 'JUAZEIRO DO NORTE', 'QUATROBARRAS': 'QUATRO BARRAS',
    'VARGEMGRANDEPAULISTA': 'VARGEM GRANDE PAULISTA', 'VISTAALEGREDOALTO': 'VISTA ALEGRE DO ALTO',
    'RIOFORMOSO': 'RIO FORMOSO', 'SANTOANTONIODOMONTE': 'SANTO ANTONIO DO MONTE',
    'ESPIRITOSANTODOPINHAL': 'ESPIRITO SANTO DO PINHAL', 'VOLTAREDONDA': 'VOLTA REDONDA',
    'SAOJOSEDORIOPARDO': 'SAO JOSE DO RIO PARDO', 'PARAGUACUPAULISTA': 'PARAGUACU PAULISTA',
    'CORONELVIVIDA': 'CORONEL VIVIDA', 'SAOJOAO': 'SAO JOAO', 'NOVAAMERICADACOLINA': 'NOVA AMERICA DA COLINA',
    'FAZENDARIOGRANDE': 'FAZENDA RIO GRANDE', 'SAOLOURENCODOOESTE': 'SAO LOURENCO DO OESTE',
    'CAMPINAGRANDEDOSUL': 'CAMPINA GRANDE DO SUL', 'LAURODEFREITAS': 'LAURO DE FREITAS',
    'NOVAPETROPOLIS': 'NOVA PETROPOLIS', 'NAZAREPAULISTA': 'NAZARE PAULISTA', 'NOVAPRATA': 'NOVA PRATA',
    'WASHINGTONDC': 'WASHINGTON, DC', 'ALTOFELIZ': 'ALTO FELIZ',
    'SAOJOAQUIMDEBICAS': 'SAO JOAQUIM DE BICAS', 'LIMEIRADOOESTE': 'LIMEIRA DO OESTE',
    'ALVARESMACHADO': 'ALVARES MACHADO', 'NOVAMARILANDIA': 'NOVA MARILANDIA',
    'ALTOHORIZONTE': 'ALTO HORIZONTE', 'RIBEIRAOPIRES': 'RIBEIRAO PIRES', 'ARTURNOGUEIRA': 'ARTUR NOGUEIRA',
    'ARMACAODOSBUZIOS': 'ARMACAO DOS BUZIOS', 'JARAGUADOSUL': 'JARAGUA DO SUL',
    'CAMBARADOSUL': 'CAMBARA DO SUL', 'CALDASNOVAS': 'CALDAS NOVAS', 'SAOJOAONEPOMUCENO': 'SAO JOAO NEPOMUCENO',
    'SEBASTIANOPOLISDOSUL': 'SEBASTIANOPOLIS DO SUL', 'PILARDOSUL': 'PILAR DO SUL',
    'BALNEARIOCAMBORIU': 'BALNEARIO CAMBORIU', 'SERRATALHADA': 'SERRA TALHADA',
    'CAMPOLIMPOPAULISTA': 'CAMPO LIMPO PAULISTA', 'RIOQUENTE': 'RIO QUENTE', 'RIOLARGO': 'RIO LARGO',
    'SAOGONCALO': 'SAO GONCALO', 'SANTOANTONIODEPADUA': 'SANTO ANTONIO DE PADUA',
    'SANTAMARIADOPARA': 'SANTA MARIA DO PARA', 'SAOJOAODORIODOPEIXE': 'SAO JOAO DO RIO DO PEIXE',
    'SAOGONCALODOPARA': 'SAO GONCALO DO PARA', 'TEOFILOOTONI': 'TEOFILO OTONI',
    'BARRADOGARCAS': 'BARRA DO GARCAS', 'TIBAUDOSUL': 'TIBAU DO SUL',
    'SAOGONCALODOSCAMPOS': 'SAO GONCALO DOS CAMPOS', 'MUNIZFREIRE': 'MUNIZ FREIRE',
    'VITORIADACONQUISTA': 'VITORIA DA CONQUISTA', 'CRUZEIRODOSUL': 'CRUZEIRO DO SUL',
    'SAOJOSEDEMIPIBU': 'SAO JOSE DE MIPIBU', 'BARAODEGRAJAU': 'BARAO DE GRAJAU',
    'TEIXEIRADEFREITAS': 'TEIXEIRA DE FREITAS', 'SANTOANTONIODEJESUS': 'SANTO ANTONIO DE JESUS',
    'SANTACRUZDORIOPARDO': 'SANTA CRUZ DO RIO PARDO', 'PORTOSEGURO': 'PORTO SEGURO',
    'VIDALRAMOS': 'VIDAL RAMOS', 'LIMOEIRODONORTE': 'LIMOEIRO DO NORTE',
    'SANTANADOIPANEMA': 'SANTANA DO IPANEMA', 'SENADORPOMPEU': 'SENADOR POMPEU',
    'SANTAISABELDOPARA': 'SANTA ISABEL DO PARA', 'ENTREIJUIS': 'ENTRE IJUIS', 'CAPAOBONITO': 'CAPAO BONITO',
    'PRESIDENTECASTELOBRANCO': 'PRESIDENTE CASTELO BRANCO', 'TERRABOA': 'TERRA BOA',
    'RIODOSUL': 'RIO DO SUL', 'TRESCOROAS': 'TRES COROAS', 'NOSSASENHORADASDORES': 'NOSSA SENHORA DAS DORES',
    'BRACODONORTE': 'BRACO DO NORTE', 'FRANCODAROCHA': 'FRANCO DA ROCHA',
    'SAOLUISDEMONTESBELOS': 'SAO LUIS DE MONTES BELOS', 'PASSOFUNDO': 'PASSO FUNDO',
    'SAOJOAODABOAVISTA': 'SAO JOAO DA BOA VISTA', 'SAOBENTODOSAPUCAI': 'SAO BENTO DO SAPUCAI',
    'NOSSASENHORADAGLORIA': 'NOSSA SENHORA DA GLORIA', 'NOVASERRANA': 'NOVA SERRANA',
    'PASTOSBONS': 'PASTOS BONS', 'NOVASANTARITA': 'NOVA SANTA RITA', 'BOMJESUSDALAPA': 'BOM JESUS DA LAPA',
    'SAOPEDRODEALCANTARA': 'SAO PEDRO DE ALCANTARA', 'SAOPEDRODAALDEIA': 'SAO PEDRO DA ALDEIA',
    'ABADIADEGOIAS': 'ABADIA DE GOIAS', 'OLHODAGUADOCASADO': 'OLHO D AGUA DO CASADO',
    'JOAOCAMARA': 'JOAO CAMARA', 'SAOMATEUS': 'SAO MATEUS', 'RIOVERDE': 'RIO VERDE',
    'UNIAODOSPALMARES': 'UNIAO DOS PALMARES', 'PATOBRANCO': 'PATO BRANCO',
    'BOAVISTADOSUL': 'BOA VISTA DO SUL', 'PASSAQUATRO': 'PASSA QUATRO',
    'OUROVERDEDOOESTE': 'OURO VERDE DO OESTE', 'TELEMACOBORBA': 'TELEMACO BORBA',
    'SAOFRANCISCODOSUL': 'SAO FRANCISCO DO SUL', 'MATEUSLEME': 'MATEUS LEME',
    'PRIMAVERADOLESTE': 'PRIMAVERA DO LESTE', 'PORTOFELIZ': 'PORTO FELIZ', 'NOVATRENTO': 'NOVA TRENTO',
    'RIOBRILHANTE': 'RIO BRILHANTE', 'TRESBARRASDOPARANA': 'TRES BARRAS DO PARANA',
    'RIOBONITO': 'RIO BONITO', 'NOVAMONTEVERDE': 'NOVA MONTE VERDE', 'ESPERAFELIZ': 'ESPERA FELIZ',
    'RIOVERDEDEMATOGROSSO': 'RIO VERDE DE MATO GROSSO', 'SAOGONCALODOAMARANTE': 'SAO GONCALO DO AMARANTE',
    'SANTANADOARAGUAIA': 'SANTANA DO ARAGUAIA', 'RIONEGRINHO': 'RIO NEGRINHO', 'PACODOLUMIAR': 'PACO DO LUMIAR',
    'SAOTIAGO': 'SAO TIAGO', 'MARECHALCANDIDORONDON': 'MARECHAL CANDIDO RONDON',
    'GOVERNADORCELSORAMOS': 'GOVERNADOR CELSO RAMOS', 'PAULORAMOS': 'PAULO RAMOS',
    'BOMJESUSDOARAGUAIA': 'BOM JESUS DO ARAGUAIA', 'IGARAPEMIRI': 'IGARAPE MIRI',
    'SAOBERNARDO': 'SAO BERNARDO', 'SENHORDOBONFIM': 'SENHOR DO BONFIM', 'NOVAMUTUM': 'NOVA MUTUM',
    'JOAOMONLEVADE': 'JOAO MONLEVADE', 'RANCHOQUEIMADO': 'RANCHO QUEIMADO',
    'SAPUCAIADOSUL': 'SAPUCAIA DO SUL', 'MONTESANTODEMINAS': 'MONTE SANTO DE MINAS',
    'BREJODAMADREDEDEUS': 'BREJO DA MADRE DE DEUS', 'LUISEDUARDOMAGALHAES': 'LUIS EDUARDO MAGALHAES',
    'CERROAZUL': 'CERRO AZUL', 'SANTOANGELO': 'SANTO ANGELO', 'SAOJOAOBATISTA': 'SAO JOAO BATISTA',
    'SAOJOAODABALIZA': 'SAO JOAO DA BALIZA', 'BARRADORIBEIRO': 'BARRA DO RIBEIRO', 'NOVOCABRAIS': 'NOVO CABRAIS',
    'NOSSASENHORADOSOCORRO': 'NOSSA SENHORA DO SOCORRO', 'BAIXOGUANDU': 'BAIXO GUANDU',
    'SAOPEDRO': 'SAO PEDRO', 'SAOSEBASTIAODOCAI': 'SAO SEBASTIAO DO CAI', 'PRAIAGRANDE': 'PRAIA GRANDE',
    'SANTACRUZDOCAPIBARIBE': 'SANTA CRUZ DO CAPIBARIBE', 'PORTOCALVO': 'PORTO CALVO',
    'SANTACRUZDOXINGU': 'SANTA CRUZ DO XINGU', 'BARRABONITA': 'BARRA BONITA',
    'SAOMATEUSDOSUL': 'SAO MATEUS DO SUL', 'SAOFELIXDOARAGUAIA': 'SAO FELIX DO ARAGUAIA',
    'SERRADOSALITRE': 'SERRA DO SALITRE', 'ANTONIOCARLOS': 'ANTONIO CARLOS',
    'VALPARAISODEGOIAS': 'VALPARAISO DE GOIAS', 'CANTODOBURITI': 'CANTO DO BURITI',
    'BELEMDESAOFRANCISCO': 'BELEM DE SAO FRANCISCO', 'BARRADOPIRAI': 'BARRA DO PIRAI',
    'PIRAIDOSUL': 'PIRAI DO SUL', 'PAULOLOPES': 'PAULO LOPES', 'SAOJOSEDOINHACORA': 'SAO JOSE DO INHACORA',
    'ITAPECERICADASERRA': 'ITAPECERICA DA SERRA', 'BARRADESAOFRANCISCO': 'BARRA DE SAO FRANCISCO',
    'RIBEIRAOBRANCO': 'RIBEIRAO BRANCO', 'TANGARADASERRA': 'TANGARA DA SERRA',
    'DOUTORPEDRINHO': 'DOUTOR PEDRINHO', 'CARAZINHO': 'CARAZINHO', 'SANTATERESA': 'SANTA TERESA',
    'MINEIROSDOTIETE': 'MINEIROS DO TIETE', 'SAOJOSEDOOURO': 'SAO JOSE DO OURO',
    'BALNEARIOPICARRAS': 'BALNEARIO PICARRAS', 'PORTODOSGAUCHOS': 'PORTO DOS GAUCHOS',
    'CESARIOLANGE': 'CESARIO LANGE', 'BELFORDROXO': 'BELFORD ROXO',
    'DOMINGOSMARTINS': 'DOMINGOS MARTINS', 'GOVERNADOREDISONLOBAO': 'GOVERNADOR EDISON LOBAO',
    'ARACOIABADASERRA': 'ARACOIABA DA SERRA', 'SAOJOAODELREI': 'SAO JOAO DEL REI',
    'SAOBORJA': 'SAO BORJA', 'SANTOANTONIODOAMPARO': 'SANTO ANTONIO DO AMPARO',
    'VARGEMGRANDE': 'VARGEM GRANDE', 'SANTATEREZADOOESTE': 'SANTA TEREZA DO OESTE',
    'BARRAVELHA': 'BARRA VELHA', 'ILHADASFLORES': 'ILHA DAS FLORES',
    'SANTAMARIADEITABIRA': 'SANTA MARIA DE ITABIRA', 'NAOMETOQUE': 'NAO ME TOQUE',
    'PORTOREAL': 'PORTO REAL', 'MARTINHOCAMPOS': 'MARTINHO CAMPOS',
    'SANTARITADECALDAS': 'SANTA RITA DE CALDAS', 'NOVAPORTEIRINHA': 'NOVA PORTEIRINHA',
    'SALTODEPIRAPORA': 'SALTO DE PIRAPORA', 'BORDADAMATA': 'BORDA DA MATA',
    'BARRADOSCOQUEIROS': 'BARRA DOS COQUEIROS', 'ELDORADODOSUL': 'ELDORADO DO SUL',
    'NOVAUBIRATA': 'NOVA UBIRATA', 'ASTOLFODUTRA': 'ASTOLFO DUTRA',
    'CACAPAVADOSUL': 'CACAPAVA DO SUL', 'LENCOISPAULISTA': 'LENCOIS PAULISTA',
    'CAMPOLIMPODEGOIAS': 'CAMPO LIMPO DE GOIAS', 'CAMPOSDOJORDAO': 'CAMPOS DO JORDAO',
    'SANTABRANCA': 'SANTA BRANCA', 'SAOJOSEDERIBAMAR': 'SAO JOSE DE RIBAMAR',
    'IRAIDEMINAS': 'IRAIDE DE MINAS', 'NOVAGRANADA': 'NOVA GRANADA',
    'GOVERNADORMANGABEIRA': 'GOVERNADOR MANGABEIRA', 'CASIMIRODEABREU': 'CASIMIRO DE ABREU',
    'MONTEAZULPAULISTA': 'MONTE AZUL PAULISTA', 'AGUASDESAOPEDRO': 'AGUAS DE SAO PEDRO',
    'ITAPORANGADAJUDA': 'ITAPORANGA D AJUDA', 'CAMPONOVODOPARECIS': 'CAMPO NOVO DO PARECIS',
    'CAPELADOALTO': 'CAPELA DO ALTO', 'PARAIBADOSUL': 'PARAIBA DO SUL', 'CARMODAMATA': 'CARMO DA MATA',
    'PRUDENTEDEMORAIS': 'PRUDENTE DE MORAIS', 'NOVOGAMA': 'NOVO GAMA', 'CAMPOMAGRO': 'CAMPO MAGRO',
    'VARGEMGRANDEDOSUL': 'VARGEM GRANDE DO SUL', 'SANTOAMARODAIMPERATRIZ': 'SANTO AMARO DA IMPERATRIZ',
    'MONTEALEGREDOPIAUI': 'MONTE ALEGRE DO PIAUI', 'ALFREDOCHAVES': 'ALFREDO CHAVES',
    'BAIXAGRANDEDORIBEIRO': 'BAIXA GRANDE DO RIBEIRO', 'SAOMIGUELDOARAGUAIA': 'SAO MIGUEL DO ARAGUAIA',
    'TEREZOPOLISDEGOIAS': 'TEREZOPOLIS DE GOIAS', 'LAUROMULLER': 'LAURO MULLER',
    'BARAODECOCAIS': 'BARAO DE COCAIS', 'PARAISODOTOCANTINS': 'PARAISO DO TOCANTINS',
    'BRASILIADEMINAS': 'BRASILIA DE MINAS', 'SANTOANTONIODOTAUA': 'SANTO ANTONIO DO TAUA',
    'DELMIROGOUVEIA': 'DELMIRO GOUVEIA', 'OLHODAGUADASFLORES': 'OLHO D AGUA DAS FLORES',
    'TERRAROXA': 'TERRA ROXA', 'SANTACARMEM': 'SANTA CARMEM', 'SAODESIDERIO': 'SAO DESIDERIO',
    'PALMEIRADOSINDIOS': 'PALMEIRA DOS INDIOS', 'FERRAZDEVASCONCELOS': 'FERRAZ DE VASCONCELOS',
    'PONTALDOPARANA': 'PONTAL DO PARANA', 'CHAPADAODOSUL': 'CHAPADAO DO SUL',
    'SAOMIGUEL': 'SAO MIGUEL', 'SAOJOAQUIMDABARRA': 'SAO JOAQUIM DA BARRA',
    'CARMODOCAJURU': 'CARMO DO CAJURU', 'CONCEICAODOCOITE': 'CONCEICAO DO COITE',
    'LARANJALDOJARI': 'LARANJAL DO JARI', 'PORTONACIONAL': 'PORTO NACIONAL',
    'AGUASLINDASDEGOIAS': 'AGUAS LINDAS DE GOIAS', 'BOMJESUSDOSPERDOES': 'BOM JESUS DOS PERDOES',
    'NOVAVENECIA': 'NOVA VENECIA', 'SAOLUISDOQUITUNDE': 'SAO LUIS DO QUITUNDE',
    'LAPAZBOLIVIA': 'LA PAZ', 'CHAPADAODOCEU': 'CHAPADAO DO CEU', 'CRUZDASALMAS': 'CRUZ DAS ALMAS',
    'MIMOSODOSUL': 'MIMOSO DO SUL', 'IGUABAGRANDE': 'IGUABA GRANDE', 'AGUAAZULDONORTE': 'AGUA AZUL DO NORTE',
    'SAOMIGUELDOGOSTOSO': 'SAO MIGUEL DO GOSTOSO', 'ROSARIOSANTAFE': 'ROSARIO, SANTA FE',
    'SALTODOJACUI': 'SALTO DO JACUI', 'DOISVIZINHOS': 'DOIS VIZINHOS',
    'NOVOREPARTIMENTO': 'NOVO REPARTIMENTO', 'PEDROAFONSO': 'PEDRO AFONSO',
    'CONSELHEIROLAFAIETE': 'CONSELHEIRO LAFAIETE', 'NOVASANTAROSA': 'NOVA SANTA ROSA',
    'SANTAROSADEVITERBO': 'SANTA ROSA DE VITERBO', 'PALMEIRASDEGOIAS': 'PALMEIRAS DE GOIAS',
    'AGUASMORNAS': 'AGUAS MORNAS', 'COMODORARIVADAVIA': 'COMODORO RIVADAVIA',
    'SAOSEBASTIAODOOESTE': 'SAO SEBASTIAO DO OESTE', 'RIOPOMBA': 'RIO POMBA', 'RIOCASCA': 'RIO CASCA',
    'SAOMIGUELDOIGUACU': 'SAO MIGUEL DO IGUACU', 'NOVAMAMORE': 'NOVA MAMORE',
    'AMERICOBRASILIENSE': 'AMERICO BRASILIENSE', 'AGUACOMPRIDA': 'AGUA COMPRIDA',
    'SAOMARCOS': 'SAO MARCOS', 'DOISIRMAOS': 'DOIS IRMAOS', 'BOAESPERANCA': 'BOA ESPERANCA',
    'SAOMIGUELDOGUAMA': 'SAO MIGUEL DO GUAMA', 'SAOJOAODABARRA': 'SAO JOAO DA BARRA',
    'BARRADOCORDA': 'BARRA DO CORDA', 'PEIXOTODEAZEVEDO': 'PEIXOTO DE AZEVEDO',
    'CURRAISNOVOS': 'CURRAIS NOVOS', 'NOVALACERDA': 'NOVA LACERDA',
    'SAOJOAOBATISTADOGLORIA': 'SAO JOAO BATISTA DO GLORIA', 'SAOCRISTOVAO': 'SAO CRISTOVAO',
    'BELACRUZ': 'BELA CRUZ', 'MONTEDOCARMO': 'MONTE DO CARMO', 'SAOLUDGERO': 'SAO LUDGERO',
    'CANAADOSCARAJAS': 'CANA DOS CARAJAS', 'AGUASDELINDOIA': 'AGUAS DE LINDOIA',
    'PAUDOSFERROS': 'PAU DOS FERROS', 'CARLOSBARBOSA': 'CARLOS BARBOSA',
    'RAULSOARES': 'RAUL SOARES', 'SAOJOAODOPARAISO': 'SAO JOAO DO PARAISO', 'BELOVALE': 'BELO VALE',
    'CAMPOMAIOR': 'CAMPO MAIOR', 'SALTOVELOSO': 'SALTO VELOSO', 'LAGOANOVA': 'LAGOA NOVA',
    'CIDADEOCIDENTAL': 'CIDADE OCIDENTAL', 'PRESIDENTEMEDICI': 'PRESIDENTE MEDICI',
    'SAOMIGUELDOOESTE': 'SAO MIGUEL DO OESTE', 'NOVAROMA': 'NOVA ROMA',
    'APARECIDADOTABOADO': 'APARECIDA DO TABOADO', 'OLHODAGUADASCUNHAS': 'OLHO D AGUA DAS CUNHAS',
    'CAJUEIRODAPRAIA': 'CAJUEIRO DA PRAIA', 'VALEREAL': 'VALE REAL',
    'BOMRETIRODOSUL': 'BOM RETIRO DO SUL', 'TRIZIDELADOVALE': 'TRIZIDELA DO VALE',
    'JACINTOMACHADO': 'JACINTO MACHADO', 'GUAJARAMIRIM': 'GUAJARA-MIRIM', 'LIMADUARTE': 'LIMA DUARTE',
    'NEVESPAULISTA': 'NEVES PAULISTA', 'BOMPRINCIPIO': 'BOM PRINCIPIO',
    'PRESIDENTEEPITACIO': 'PRESIDENTE EPITACIO',
    'SAODOMINGOSDOMARANHAO': 'SAO DOMINGOS DO MARANHAO', 'VARZEADOPOCO': 'VARZEA DO POCO',
    'SANTOANTONIODORACANGUA': 'SANTO ANTONIO DO ARACANGUA',
    'SAOGABRIELDOOESTE': 'SAO GABRIEL DO OESTE', 'ELCALAFATESANTACRUZ': 'EL CALAFATE, SANTA CRUZ',
    'AMERICANODOBRASIL': 'AMERICANO DO BRASIL', 'AGUABRANCA': 'AGUA BRANCA',
    'LARANJALPAULISTA': 'LARANJAL PAULISTA', 'SAOSIMAO': 'SAO SIMAO', 'DONAEMMA': 'DONA EMMA',
    'PADREBERNARDO': 'PADRE BERNARDO', 'SANTOANTONIODEPOSSE': 'SANTO ANTONIO DE POSSE',
    'CAPITAOPOCO': 'CAPITAO POCO', 'NOVAMARINGA': 'NOVA MARINGA', 'SAOGOTARDO': 'SAO GOTARDO',
    'AREIABRANCA': 'AREIA BRANCA', 'IGARACUDOTIETE': 'IGARACU DO TIETE',
    'MARIADAFE': 'MARIA DA FE', 'LAGOASALGADA': 'LAGOA SALGADA',
    'CDADAUTONOMADEBUENOSAIRES': 'CIUDAD AUTONOMA DE BUENOS AIRES', 'TEODOROSAMPAIO': 'TEODORO SAMPAIO',
    'PUERTOIGUAZU': 'PUERTO IGUAZU', 'VILLADEMERLOSANLUIS': 'VILLA DE MERLO, SAN LUIS',
    'SANNICOLAS': 'SAN NICOLAS', 'SANTIAGODECHILE': 'SANTIAGO, CHILE'
}
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

# 1. Substituir o código dos países pelos nomes no df_clientes
df_clientes['PAIS_CLIENTE'] = df_clientes['PAIS_CLIENTE'].replace(mapeamento_paises)

# 2. Substituir 'EXTRANGEIRO' por 'ESTRANGEIRO' na tabela de CIDADE_CLIENTE
df_clientes['CIDADE_CLIENTE'] = df_clientes['CIDADE_CLIENTE'].replace('EXTRANGEIRO', 'ESTRANGEIRO')

# 3. As cidades estrangeiras que estiverem categorizadas como "ESTRANGEIRO" serão substituídas pelas capitais:
condicao = (df_clientes['PAIS_CLIENTE'] != 'BRASIL') & (df_clientes['CIDADE_CLIENTE'] == 'ESTRANGEIRO')
df_clientes.loc[condicao, 'CIDADE_CLIENTE'] = df_clientes['PAIS_CLIENTE'].map(mapeamento_capitais_estrangeiras)
# 4. Substituir no nome das cidades (exemplo: "SANTOANDRE" para "SANTO ANDRE")
df_clientes['CIDADE_CLIENTE'] = df_clientes['CIDADE_CLIENTE'].replace(mapeamento_cidades)

# --- 2. CRIAR A MATRIZ OBJETO DE CLIENTES COM GEOLOCALIZAÇÃO ---
# Fazer o merge dos dois DataFrames com base em PAIS_CLIENTE e CIDADE_CLIENTE
# Use how='left' para manter todos os clientes e adicionar as colunas de lat/long
df_clientes_com_geolocalizacao = pd.merge(df_clientes, df_localizacao, on=['PAIS_CLIENTE', 'CIDADE_CLIENTE'], how='left')

# --- 3. SEPARAR A MATRIZ OBJETO ORIGINAL EM MATRIZES OBJETO DE ACORDO COM O PAÍS ---
#Decidiu-se criar 5 grupos: 
# Grupo 1: BRASIL
df_clientes_com_geolocalizacao_1 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'] == 'BRASIL']
print("País no Grupo 1: BRASIL")
print("\nNúmero de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1))
# Grupo 2 (Países do Leste da América Latina): ARGENTINA, URUGUAI e PARAGUAI
paises_grupo_2 = ['ARGENTINA', 'URUGUAI', 'PARAGUAI']
df_clientes_com_geolocalizacao_2 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'].isin(paises_grupo_2)]
print("Países no Grupo 2:", df_clientes_com_geolocalizacao_2['PAIS_CLIENTE'].unique())
print("\nNúmero de clientes no Grupo 3:", len(df_clientes_com_geolocalizacao_2))
# Grupo 3 (Países da América Latina do Oeste): PERU, EQUADOR, BOLIVIA, CHILE
paises_grupo_3 = ['PERU', 'EQUADOR', 'BOLIVIA', 'CHILE']
df_clientes_com_geolocalizacao_3 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'].isin(paises_grupo_3)]
print("Países no Grupo 3:", df_clientes_com_geolocalizacao_3['PAIS_CLIENTE'].unique())
print("\nNúmero de clientes no Grupo 3:", len(df_clientes_com_geolocalizacao_3))
# Grupo 4: EUA
df_clientes_com_geolocalizacao_4 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'] == 'EUA']
print("País no grupo 4: EUA")
print("\nNúmero de clientes no Grupo 4:", len(df_clientes_com_geolocalizacao_4))
# Grupo 5: RÚSSIA
df_clientes_com_geolocalizacao_5 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'] == 'RUSSIA']
print("País no grupo 5: RUSSIA")
print("\nNúmero de clientes no Grupo 5:", len(df_clientes_com_geolocalizacao_5))
# Grupo 6 (Mercados de baixo volume de clientes): ANGOLA e PORTUGAL
paises_grupo_6 = ['ANGOLA', 'PORTUGAL']
df_clientes_com_geolocalizacao_6 = df_clientes_com_geolocalizacao[df_clientes_com_geolocalizacao['PAIS_CLIENTE'].isin(paises_grupo_6)]
print("Países no Grupo 6:", df_clientes_com_geolocalizacao_6['PAIS_CLIENTE'].unique())
print("\nNúmero de clientes no Grupo 6:", len(df_clientes_com_geolocalizacao_6))

#--- 5. SEPARAR A MATRIZ FEATURES DO GRUPO 1 (BRASIL) DE ACORDO COM AS REGIÕES E ESTADOS
uf_brasileiras = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
    'SP', 'SE', 'TO'
]
df_clientes_1_filtrado = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'].isin(uf_brasileiras)]
# Agrupe o dataframe limpo por UF
grupo_1_por_uf = df_clientes_1_filtrado.groupby('UF_CLIENTE')
#GRUPO: SP 92330
df_clientes_com_geolocalizacao_1_1 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'SP']
print("País no Grupo 1.1: SP")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_1))
#MG    24381
df_clientes_com_geolocalizacao_1_2 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'MG']
print("País no Grupo 1.2: MG")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_2))
#RJ    16557
df_clientes_com_geolocalizacao_1_3 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'RJ']
print("País no Grupo 1.3: RJ")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_3))
#PR    13128
df_clientes_com_geolocalizacao_1_4 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'PR']
print("País no Grupo 1.4: PR")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_4))
#SC    12629
df_clientes_com_geolocalizacao_1_5 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'SC']
print("País no Grupo 1.5: SC")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_5))
#RS    12239
df_clientes_com_geolocalizacao_1_6 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'RS']
print("País no Grupo 1.6: RS")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_6))
#GO     8570
df_clientes_com_geolocalizacao_1_7 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'GO']
print("País no Grupo 1.7: GO")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_7))
#PE     8057
df_clientes_com_geolocalizacao_1_8 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'PE']
print("País no Grupo 1.8: PE")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_8))
#BA     7755
df_clientes_com_geolocalizacao_1_9 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'BA']
print("País no Grupo 1.9: BA")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_9))
#CE     6466
df_clientes_com_geolocalizacao_1_10 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'CE']
print("País no Grupo 1.10: CE")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_10))
#DF     4702
df_clientes_com_geolocalizacao_1_11 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'DF']
print("País no Grupo 1.11: DF")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_11))
#ES     4442
df_clientes_com_geolocalizacao_1_12 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'ES']
print("País no Grupo 1.12: ES")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_12))
#MT     3859
df_clientes_com_geolocalizacao_1_13 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'MT']
print("País no Grupo 1.13: MT")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_13))
#PA     3343
df_clientes_com_geolocalizacao_1_14 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'PA']
print("País no Grupo 1.14: PA")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_14))
#AM     3248
df_clientes_com_geolocalizacao_1_15 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'AM']
print("País no Grupo 1.15: AM")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_15))
#PB     3185
df_clientes_com_geolocalizacao_1_16 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'PB']
print("País no Grupo 1.16: PB")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_16))
#RN     1733
df_clientes_com_geolocalizacao_1_17 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'RN']
print("País no Grupo 1.17: RN")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_17))
#AL     1655
df_clientes_com_geolocalizacao_1_18 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'AL']
print("País no Grupo 1.18: AL")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_18))
#SE     1482
df_clientes_com_geolocalizacao_1_19 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'SE']
print("País no Grupo 1.19: SE")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_19))
#PI     1420
df_clientes_com_geolocalizacao_1_20 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'PI']
print("País no Grupo 1.20: PI")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_20))
#TO     1366
df_clientes_com_geolocalizacao_1_21 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'TO']
print("País no Grupo 1.21: TO")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_21))
#MA     1343
df_clientes_com_geolocalizacao_1_22 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'MA']
print("País no Grupo 1.22: MA")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_22))
#MS     1107
df_clientes_com_geolocalizacao_1_23 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'MS']
print("País no Grupo 1.23: MS")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_23))
#RO     1055
df_clientes_com_geolocalizacao_1_24 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'RO']
print("País no Grupo 1.24: RO")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_24))
#AC      451
df_clientes_com_geolocalizacao_1_25 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'AC']
print("País no Grupo 1.25: AC")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_25))
#RR      296
df_clientes_com_geolocalizacao_1_26 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'RR']
print("País no Grupo 1.26: RR")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_26))
#AP      295
df_clientes_com_geolocalizacao_1_27 = df_clientes_com_geolocalizacao_1[df_clientes_com_geolocalizacao_1['UF_CLIENTE'] == 'AP']
print("País no Grupo 1.27: AP")
print("Número de clientes no Grupo :", len(df_clientes_com_geolocalizacao_1_27))

# --- 6. CRIE A MATRIZ FEATURES PARA CLUSTERIZAR (X) COM AS COORDENADAS EM RADIANOS
# A métrica haversine do scikit-learn espera as coordenadas em radianos
# Copiar o DataFrame antes de remover os NaNs para manter o original
#df_para_clusterizar = df_clientes_com_geolocalizacao.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#X = np.deg2rad(df_para_clusterizar[['LATITUDE', 'LONGITUDE']].values)
# Grupo 1: BRASIL
df_para_clusterizar_1 = df_clientes_com_geolocalizacao_1.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
# Grupo 2: Países do Leste da América Latina
df_para_clusterizar_2 = df_clientes_com_geolocalizacao_2.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
# Grupo 3: Países da América Latina do Oeste
df_para_clusterizar_3 = df_clientes_com_geolocalizacao_3.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
# Grupo 4: EUA
df_para_clusterizar_4 = df_clientes_com_geolocalizacao_4.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
# Grupo 5: RÚSSIA
df_para_clusterizar_5 = df_clientes_com_geolocalizacao_5.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
# Grupo 6: Mercados de baixo volume (Angola e Portugal)
df_para_clusterizar_6 = df_clientes_com_geolocalizacao_6.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Quantidade de linhas em cada dataframe antes e depois
print("\nVerificação dos dataframes após remover os NaNs:")
print(f"Grupo 1 (Brasil): Original {len(df_clientes_com_geolocalizacao_1)} linhas, Clusterizável {len(df_para_clusterizar_1)} linhas")
print(f"Grupo 2 (Leste): Original {len(df_clientes_com_geolocalizacao_2)} linhas, Clusterizável {len(df_para_clusterizar_2)} linhas")
print(f"Grupo 3 (Oeste): Original {len(df_clientes_com_geolocalizacao_3)} linhas, Clusterizável {len(df_para_clusterizar_3)} linhas")
print(f"Grupo 4 (EUA): Original {len(df_clientes_com_geolocalizacao_4)} linhas, Clusterizável {len(df_para_clusterizar_4)} linhas")
print(f"Grupo 5 (Rússia): Original {len(df_clientes_com_geolocalizacao_5)} linhas, Clusterizável {len(df_para_clusterizar_5)} linhas")
print(f"Grupo 6 (Baixo Volume): Original {len(df_clientes_com_geolocalizacao_6)} linhas, Clusterizável {len(df_para_clusterizar_6)} linhas")
#Grupo 1.1 (SP): 
df_para_clusterizar_1_1 = df_clientes_com_geolocalizacao_1_1.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.2 (MG):
df_para_clusterizar_1_2 = df_clientes_com_geolocalizacao_1_2.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.3 (RJ):
df_para_clusterizar_1_3 = df_clientes_com_geolocalizacao_1_3.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.4 (PR):
df_para_clusterizar_1_4 = df_clientes_com_geolocalizacao_1_4.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.5 (SC):
df_para_clusterizar_1_5 = df_clientes_com_geolocalizacao_1_5.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.6 (RS):
df_para_clusterizar_1_6 = df_clientes_com_geolocalizacao_1_6.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.7 (GO):  
df_para_clusterizar_1_7 = df_clientes_com_geolocalizacao_1_7.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()   
#Grupo 1.8 (PE): 
df_para_clusterizar_1_8 = df_clientes_com_geolocalizacao_1_8.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.9 (BA):
df_para_clusterizar_1_9 = df_clientes_com_geolocalizacao_1_9.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.10 (CE):
df_para_clusterizar_1_10 = df_clientes_com_geolocalizacao_1_10.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.11 (DF):
df_para_clusterizar_1_11 = df_clientes_com_geolocalizacao_1_11.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.12 (ES):
df_para_clusterizar_1_12 = df_clientes_com_geolocalizacao_1_12.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.13 (MT):
df_para_clusterizar_1_13 = df_clientes_com_geolocalizacao_1_13.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.14 (PA):
df_para_clusterizar_1_14 = df_clientes_com_geolocalizacao_1_14.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.15 (AM):
df_para_clusterizar_1_15 = df_clientes_com_geolocalizacao_1_15.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.16 (PB):
df_para_clusterizar_1_16 = df_clientes_com_geolocalizacao_1_16.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.17 (RN):
df_para_clusterizar_1_17 = df_clientes_com_geolocalizacao_1_17.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.18 (AL):
df_para_clusterizar_1_18 = df_clientes_com_geolocalizacao_1_18.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.19 (SE):
df_para_clusterizar_1_19 = df_clientes_com_geolocalizacao_1_19.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.20 (PI): 
df_para_clusterizar_1_20 = df_clientes_com_geolocalizacao_1_20.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.21 (TO):
df_para_clusterizar_1_21 = df_clientes_com_geolocalizacao_1_21.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.22 (MA): 
df_para_clusterizar_1_22 = df_clientes_com_geolocalizacao_1_22.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.23 (MS):
df_para_clusterizar_1_23 = df_clientes_com_geolocalizacao_1_23.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.24 (RO): 
df_para_clusterizar_1_24 = df_clientes_com_geolocalizacao_1_24.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.25 (AC):
df_para_clusterizar_1_25 = df_clientes_com_geolocalizacao_1_25.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.26 (RR): 
df_para_clusterizar_1_26 = df_clientes_com_geolocalizacao_1_26.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()
#Grupo 1.27 (AP): 
df_para_clusterizar_1_27 = df_clientes_com_geolocalizacao_1_27.dropna(subset=['LATITUDE', 'LONGITUDE']).copy()

#-----------------------------------------------------------------------------------------------------------------------------------------------
#CLUSTERIZAR, VALIDAR E SALVAR
#-----------------------------------------------------------------------------------------------------------------------------------------------
def clusterizar_e_validar(df, eps, min_samples):
    """
    Realiza a clusterização DBSCAN, valida e retorna as métricas.
    Retorna o DataFrame com a coluna 'cluster' e um dicionário de métricas.

    Args:
        df (pd.DataFrame): DataFrame com as colunas 'LATITUDE' e 'LONGITUDE'.
        eps (float): Raio de busca para o DBSCAN.
        min_samples (int): Número mínimo de pontos para formar um cluster.
        
    Returns:
        tuple: (DataFrame com a coluna 'cluster' adicionada, dicionário de métricas)
    """
    
    # 1. Prepare os dados para clusterização
    coords_rad = np.radians(df[['LATITUDE', 'LONGITUDE']])
    
    # 2. Aplique o DBSCAN
    dbscan = DBSCAN(eps=eps, min_samples=min_samples, metric='haversine').fit(coords_rad)
    df['cluster'] = dbscan.labels_
    
    # 3. Calcule as métricas
    n_clusters = len(set(dbscan.labels_)) - (1 if -1 in dbscan.labels_ else 0)
    n_outliers = list(dbscan.labels_).count(-1)
    
    silhouette = -1 # Valor padrão para quando o cálculo não é possível
    if n_clusters > 1:
        silhouette = silhouette_score(coords_rad, dbscan.labels_)
    
    return df, {
        'clusters_labels': df['cluster'],
        'n_clusters': n_clusters,
        'n_outliers': n_outliers,
        'silhouette_score': silhouette
    }
#----------------------------------------------------------
# Clusterizar o Grupo 1.1 (SP):
df_para_clusterizar_1_1, resultados_1_1 = clusterizar_e_validar(
    df=df_para_clusterizar_1_1, # Não é mais necessário o .copy() aqui, pois a função já retorna o df
    eps=0.000157,
    min_samples=50
)
print("\nResultados para São Paulo (SP):")
print(f"Número de Clusters: {resultados_1_1['n_clusters']}")
print(f"Número de Clientes Fora dos Clusters (Ruído): {resultados_1_1['n_outliers']}")
print(f"Silhouette Score: {resultados_1_1['silhouette_score']:.4f}")
contagem_clusters_1_1 = resultados_1_1['clusters_labels'].value_counts()
print("\nContagem de Clientes por Cluster:")
print(contagem_clusters_1_1)
#-----------------------------------------------------------------------------------------------------------------------------------------------
#VISUALIZAR E SALVAR
#-----------------------------------------------------------------------------------------------------------------------------------------------
def visualizar_clusters_folium(df_clusterizado, regiao):
    """
    Cria e salva um mapa interativo Folium dos clusters geográficos.

    Args:
        df_clusterizado (pd.DataFrame): DataFrame com as colunas necessárias.
        regiao (str): Nome da região/estado para o nome do arquivo.
    """
    # Verifique se a coluna 'cluster' existe
    if 'cluster' not in df_clusterizado.columns:
        print("Erro: O DataFrame não contém a coluna 'cluster'.")
        return

    # Calcule o centro do mapa para a região
    centro_regiao = [df_clusterizado['LATITUDE'].mean(), df_clusterizado['LONGITUDE'].mean()]
    mapa = folium.Map(location=centro_regiao, zoom_start=6)

    # Crie uma paleta de cores para os clusters
    clusters_unicos = df_clusterizado['cluster'].unique()
    cores_paleta = cm.viridis(np.linspace(0, 1, len(clusters_unicos) - (1 if -1 in clusters_unicos else 0)))
    cluster_to_color = {cluster: colors.rgb2hex(cores_paleta[i]) for i, cluster in enumerate(sorted(clusters_unicos) if -1 not in clusters_unicos else sorted(clusters_unicos)[1:])}

    # Adicione os pontos ao mapa
    for idx, row in df_clusterizado.iterrows():
        cluster_id = row['cluster']
        
        if cluster_id == -1:
            cor_ponto = '#808080'  # Cinza para ruído
            label = 'Ruído'
        else:
            cor_ponto = cluster_to_color.get(cluster_id, '#FFFFFF')
            label = f'Cluster {cluster_id}'
        
        folium.CircleMarker(
            location=[row['LATITUDE'], row['LONGITUDE']],
            radius=3,
            color=cor_ponto,
            fill=True,
            fill_color=cor_ponto,
            fill_opacity=0.7,
            tooltip=label
        ).add_to(mapa)

    # Adicione a legenda
    legenda_html = f'''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 150px; height: 120px; 
                    border:2px solid grey; z-index:9999; font-size:14px;
                    background-color:white; opacity:0.9;">
          &nbsp; <b style="font-size:16px;">Clusters {regiao}</b> <br>
          &nbsp; <i style="background:gray;border:1px solid black;width:12px;height:12px;display:inline-block;"></i> Ruído (-1) <br>
          &nbsp; <i style="background:white;border:1px solid black;width:12px;height:12px;display:inline-block;"></i> Outros <br>
        </div>
    '''
    mapa.get_root().html.add_child(folium.Element(legenda_html))

    caminho_salvar = os.path.join(CURATED_PATH, f'mapa_clusters_interativo_{regiao}.html')
    mapa.save(caminho_salvar)
    print(f"\nMapa interativo salvo em: {caminho_salvar}")

#----------------------------------------------------------
#Figura Grupo 1.1:
visualizar_clusters_folium(df_para_clusterizar_1_1, 'SP')

exit()



# Criação de um mapa base centralizado para uma visão global
mapa = folium.Map(location=[0, 0], zoom_start=2)

# Gera uma paleta de cores para os clusters.
# A cor preta será usada para os pontos de ruído (cluster -1)
if n_clusters > 0:
    cores = plt.get_cmap('hsv', n_clusters)
else:
    cores = None
cor_ruido = 'gray'

for index, row in df_para_clusterizar.iterrows():
    cluster_label = row['CLUSTER']
    
    if cluster_label == -1:
        cor_hex = cor_ruido
    else:
        cor_hex = colors.rgb2hex(cores(cluster_label)[:3])
        
    folium.CircleMarker(
        location=[row['LATITUDE'], row['LONGITUDE']],
        radius=5,
        color=cor_hex,
        fill=True,
        fill_color=cor_hex,
        fill_opacity=0.7,
        popup=f"Cluster: {cluster_label}"
    ).add_to(mapa)

mapa.save(os.path.join(CURATED_PATH, "mapa_clusters.html"))

print("\nMapa de clusters globais gerado com sucesso. Verifique o arquivo 'mapa_clusters.html' na pasta 'curated'.")

# 1. Salvar os resultados da clusterização em um novo arquivo
CAMINHO_FINAL = os.path.join(CURATED_PATH, "dados_clientes_clusterizados.parquet")
df_para_clusterizar.to_parquet(CAMINHO_FINAL, index=False)
print(f"\nDataFrame com os clusters salvo com sucesso em: {CAMINHO_FINAL}")