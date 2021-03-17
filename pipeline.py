####################################################################
# ETAPA 1: Extrair dados do mongodb. Estamos considerando que os dados do mongo foram inseridos pelo fluxo de compra no site
###################################################################
# extrair os dados que queremos importar do mongoDB e jogar em csv para uma stage area
# para essa solução vamos usar o mongoexport entao é importante que a tool existe no seu projeto
# como estmamos em um lab com mongo local a uri segue o formato utilizado mas poderia ser alterado para qualquer outra
import os
try:
    os.system('mongoexport --uri="mongodb://127.0.0.1:27017" --db luizalabs --collection compra --type=csv --fields PRODUTO,DEPARTAMENTO,CATEGORIA,USER_ID,CIDADE_USER,ANO,MES --out c:\luizalabs\carga\carga.csv')
    print ('Etapa 1: Extração de Dados Executada com Sucesso!')
except Exception as e:
    print(e)
#################################################################
# ETAPA 2: Importar os dados extraidos do MongoDb no Cassandra
#################################################################
# Import arquivo de dados csv to Cassandra
# Para melhor entendimento do modelo de dados adotado verificar SetupCassandra.py

import csv
from cassandra.cluster import Cluster
# abrindo a session
try:
    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace('luizalabs')
except Exception as e:
    print(e)
# caminho do arquivo
fc = 'c:\luizalabs\carga\carga.csv'
# carga da primeira tabela a partir do csv gerado do mongodb
try:
    with open(fc, 'r') as f:
        csvreader = csv.reader(f, delimiter = ',')
        next(csvreader)
        for line in csvreader:
            query = "INSERT INTO produto_cliente (user_id, produto, departamento)"
            query = query + "values (%s,%s,%s)"
            session.execute(query, (int(line[3]), line[0], line[1]))
        print ('Etapa 2.1: Insert de Dados 1 Executado com Sucesso!')
except Exception as e:
    print(e)
# carga da segunda tabela a partir do csv gerado do mongodb
try:
    with open(fc, 'r') as f:
        csvreader = csv.reader(f,delimiter = ',')
        next(csvreader)
        for line in csvreader:
            query = "INSERT INTO produto_departamento_ano (year, produto, departamento, categoria)"
            query = query + "values (%s,%s,%s,%s)"
            session.execute(query,(line[5], line[0], line[1],line[2]))
        print ('Etapa 2.2: Insert de Dados 2 Executado com Sucesso!')
except Exception as e:
    print(e)

# fechando a session
session.shutdown()





