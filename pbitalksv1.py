###################################################
#Criando um arquivo de banco de dados do SQLite
#É para ser executado em um script de criação de BD
###################################################
from pathlib import Path

EnderecoBDPBITalks = Path('C:\\Users\\claud\\OneDrive\\Dadoteca\\_Palestras\\Power BI Talks 10ed. - 10122022\\')

BDPBITalks = EnderecoBDPBITalks / 'BDPBITalks.db'

if EnderecoBDPBITalks.exists():
    if (BDPBITalks.exists()):
        print ('Banco de dados já existe')
    else:
        BDPBITalks.touch()
else:
    print ('Endereço não existe')


###########################################################################################
#Processo de extração, tratamento e carga de dados do site do INEP no Banco de dados SQLIte
###########################################################################################
#Bibliotecas
import pandas as pd #Manipulação de dados
import zipfile as zip #Manipulação de arquivos zipados
import requests #Coletar dados da web (Webscraping)
from io import BytesIO #Armazenar dados em Bytes na memória (Dar celeridade ao processo)
from datetime import datetime #Obter dados relacionados a datas e horas
import sqlite3 as sql #manipulação de dados no banco SQLite

#Abrindo uma conexão com o banco de dados
ConexaoPBITalks = sql.connect(EnderecoBDPBITalks / 'BDPBITalks.db')

#URL Padrão: https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2021.zip
#colentando ano corrente
Ano_Atual = datetime.today().year

#coletando o histório de 3 anos
Ano_historico = Ano_Atual-3

#Loop de carga de dados, considerando os 3 últimos anos
while Ano_historico < Ano_Atual:
    #########################################
    # Extração de dados do site do INEP
    #########################################
    url = "http://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{}.zip".format(Ano_historico)

    #obtendo o arquio zip na url e transformando-o Arquivo de Bytes
    arquivoZip = BytesIO(
        requests.get(url,verify=False).content
        )
    MeuArquivoZip = zip.ZipFile(arquivoZip)

    #realiza a extração dos arquivos na pasta dados2021
    MeuArquivoZip.extractall("./dados")

    #Coletando dados CSV, extraídos e descompactados na pasta e armazenando-os em um DF Pandas
    enderecoCSV = "C:\\Users\\claud\\OneDrive\\Dadoteca\\_Palestras\\Power BI Talks 10ed. - 10122022\\dados\\Microdados do Censo da Educaç╞o Superior {}\\dados\\MICRODADOS_CADASTRO_CURSOS_{}.CSV".format(Ano_historico,Ano_historico)
    df_dados = pd.DataFrame(pd.read_csv(enderecoCSV,encoding='ISO-8859-1',sep=';'))

    #########################################
    # Tratamento de dados
    #########################################
    #Definindo somente as colunas de interesse
    df_dados = df_dados[["NU_ANO_CENSO","NO_REGIAO","SG_UF","NO_MUNICIPIO","NO_CURSO","TP_MODALIDADE_ENSINO","QT_ING_FEM","QT_ING_MASC"]]
    #df_dados

    #Substituir valores de
    df_dados['TP_MODALIDADE_ENSINO']=df_dados['TP_MODALIDADE_ENSINO'].replace({1:'Presencial',2:"EaD"})
    
    #Excluindo nulos
    df_dados = df_dados.dropna()

    #Renomear o index
    df_dados.index.name = 'idINEP'
    
    ######################################
    #Carregando dados no banco de dados
    ######################################
    #Criando e inserindo dados
    df_dados.to_sql('tbINEP',ConexaoPBITalks,if_exists='append')


    #Mensagem de carga de dados
    print("------------------------------------------------------------------")
    print("Dados do ano de {} carregados com sucesso.".format(Ano_historico))
    print("------------------------------------------------------------------")

    #Soma 1 ano ano histórico e retorna para mais um loop
    Ano_historico = Ano_historico + 1

#fechando conexão com o banco
ConexaoPBITalks.close()
print("Processo de extração e carga finalizado!")