from cassandra.cluster import Cluster

try:
    cluster = Cluster(['127.0.0.1']) #cassandra local
    session = cluster.connect()
except Exception as e:
    print(e)

## Create keyspace
try:
    session.execute(""" 
    CREATE KEYSPACE IF NOT EXISTS luizalabs  
    WITH REPLICATION =  
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
                    )
except Exception as e:
    print(e)

## Set keyspace Luizalabs
try:
    session.set_keyspace('luizalabs')
except Exception as e:
    print(e)

########### Modelo de Dados no Cassandra #################
## 1. O modelo de dados do cassandra é focado na clausula WHERE
## 2. O modelo de dados é criado em função das perguntas que queremos responder. Essa visão é super importante.
## 3. A desnormalização é esperada nesse cenario uma vez que nao temos o uso de join
## 4. Cassandra trabalha com os conceitos de primary key, partition key e clustering columns. A primary key é composta por
# partition key e clustering columns. O primeiro elemento da primary key é a partition key e vai determinar a distribuição
# dos daddos nos nodes. O segundo elemento da primary key é clustering column que determina o sort order em cada partição

##perguntas que queremos responder:
# 1) quais foram os produtos comprados por um cliente ordenado por departamento
# 2) quais foram os produtos comprados no departamento de livros no ano de 2021 ordenados por categoria

query = "DROP TABLE IF EXISTS luizalabs.produto_cliente"
session.execute(query)
query = "CREATE TABLE IF NOT EXISTS luizalabs.produto_cliente"
query = query + "(user_id int, produto text, departamento text, PRIMARY KEY (user_id,departamento))"
try:
    session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS luizalabs.produto_departamento_ano"
session.execute(query)
query = "CREATE TABLE IF NOT EXISTS luizalabs.produto_departamento_ano"
query = query + "(year text, produto text, departamento text ,categoria text, PRIMARY KEY ((year, departamento),categoria))"
try:
    session.execute(query)
except Exception as e:
    print(e)



session.shutdown()