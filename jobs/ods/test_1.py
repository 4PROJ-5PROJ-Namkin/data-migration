import pandas as pd
import pyodbc
import os
from dotenv import load_dotenv
# Paramètres de connexion
load_dotenv('../../.env')
server = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password

df_parquet = pd.read_csv('../../data/machines_parquet/ods_supply_chain.csv')
#df_parquet.to_parquet("../../data/machines_parquet/fact_true_supply_chain.parquet")
#df_parquet = pd.read_parquet("../../data/machines_parquet/fact_true_supply_chain.parquet")
#df_parquet.unitId = range(0, df_parquet.shape[0])
#
# print(df_parquet.dtypes)
# Établir une connexion
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Activer fast_executemany
cursor.fast_executemany = True

# Préparer la requête d'insertion
query = "INSERT INTO fact_supply_chain (machineId,partId,materialId,timeOfProduction,materialPrice,timeId,materialDate,partDefaultPrice,isDamaged,lastUpdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

# Taille du lot à insérer à la fois
batch_size = 1000000  # Ajustez ce nombre en fonction de la capacité de votre système

#time_df = pd.read_csv("../../data/machines_parquet/time.csv")
# Diviser les données en lots et les insérer séquentiellement
for i in range(0, df_parquet.shape[0], batch_size):
    batch = df_parquet[i:i + batch_size]
    params = batch[['machineId','partId','materialId','timeOfProduction','materialPrice','timeId','materialDate','partDefaultPrice','isDamaged','lastUpdate']].values.tolist()
    cursor.executemany(query, params)
    print('Query executed successfully')
    conn.commit()  