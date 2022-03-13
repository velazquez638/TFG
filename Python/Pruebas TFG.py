#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
ruta = 'Datos/2019/20190520.json'
datos = pd.read_json(ruta, orient = 'records')
datos

#tecnologías de seguimiento de personas
#ver cuales de las macs son dínamicas y cuales sonn reales
#relacionar la mac en el tiempo ej esta mac viene todos los viernes a las 8
#estudiar cuando las macs son dinamicas o no 


# In[32]:


pip install pymongo==3.10.1


# In[2]:


pip install pymongo


# Operaciones con filas y columnas
# 
# https://www.analyticslane.com/2019/06/21/seleccionar-filas-y-columnas-en-pandas-con-iloc-y-loc/
# 
# ---

# In[2]:


datos.iloc[0] # Primera fila


# In[3]:


data = datos.iloc[3] # fila observations
data


# In[4]:


columna_data = datos.iloc[:, [3]] #columna 3 (data)
columna_data


# In[5]:


observations = columna_data.iloc[[3]]
observations


# convertir JSON en un DataFrame de Pandas
# 
# https://towardsdatascience.com/how-to-convert-json-into-a-pandas-dataframe-100b2ae1e0d8
# 
# ---

# In[24]:


#primer array de datos de observaciones
for client in observations['data']:
    cliente1 = client[0]
    
cliente1


# In[25]:


#primeros 5 arrays de datos de observaciones
for client in observations['data']:
    cliente1_5 = client[0:5]

cliente1_5


# In[14]:


from pathlib import Path

path = (r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019")
def ls3(path):
    return [obj.name for obj in Path(path).iterdir() if obj.is_file()]

ls3(path)


# In[18]:


import pandas as pd
fichero = '20190520.json'
ruta = 'Datos/2019/' + (fichero)
datos = pd.read_json(ruta, orient = 'records')
datos


# In[16]:


import pandas as pd
list = ls3(path)

for n in list:
    ruta = 'Datos/2019/' + (n)
    datos = pd.read_json(ruta, orient = 'records')
    with open(r"D:\Álvaro Velázquez\Desktop\UNI\TFG\prueba.json") as outfile:
        json.dump(datos, outfile)
    print (datos)


# In[10]:


#lectura de todos los ficheros json de la carpeta 2019
import pandas as pd
list = ls3(path)

for n in list:
    ruta = 'Datos/2019/' + (n)
    datos = pd.read_json(ruta, orient = 'records')
    print (datos)


# In[9]:


import pandas as pd
list = ls3(path)

dir = (r"D:\Álvaro Velázquez\Desktop\UNI\TFG")
file_name = "data.json"
data = []
for n in list:
    ruta = 'Datos/2019/' + (n)
    datos = pd.read_json(ruta, orient = 'records')
    data.append(datos)
    print (datos)
    datos = datos.to_json()
    
with open('data.json', 'w') as file:
    json.dump(data, file, indent=4)
        


# In[4]:


import json

data = {}
data['people'] = []
data['people'].append({
    'name': 'Scott',
    'website': 'Pharos.sh.com',
    'from': 'Nebraska'
})
data['people'].append({
    'name': 'Larry',
    'website': 'google.com',
    'from': 'Michigan'
})
data['people'].append({
    'name': 'Tim',
    'website': 'apple.com',
    'from': 'Alabama'
})

with open(r"D:\Álvaro Velázquez\Desktop\UNI\TFG\prueba.json", 'w') as outfile:
    json.dump(data, outfile)


# In[4]:


import json

with open(r"D:\Álvaro Velázquez\Desktop\UNI\TFG\prueba.json") as json_file:
    data = json.load(json_file)
    for p in data['people']:
        print('Name: ' + p['name'])
        print('Website: ' + p['website'])
        print('From: ' + p['from'])
        print('')


# In[ ]:


import json
import pandas as pd

ruta_archivo_json = '/Users/Juan/Downloads/datos_ejemplo_2.json'
with open(ruta_archivo_json) as archivo_json:
    datos_json = json.load(archivo_json)
lista_de_diccionarios = datos_json['datos']
datos = pd.DataFrame(lista_de_diccionarios)


# In[3]:


import json 
from pymongo import MongoClient  
  
myclient = MongoClient("mongodb://localhost:27017/")  
   
db = myclient["TFG"] 
   
Collection = db["Data"] 
  
with open('Datos/2019/20190520.json') as file: 
    file_data = json.load(file) 
      
if isinstance(file_data, list): 
    Collection.insert_many(file_data) 
    print("Lista de datos insertados")
else: 
    Collection.insert_one(file_data)
    print("Datos insertados")


# In[2]:


pip install matplotlib


# In[32]:


import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
import json
import warnings
warnings.filterwarnings('ignore')


# In[33]:


Client = MongoClient('localhost', 27017)


# In[34]:


Client.list_database_names()


# In[35]:


print (Client.list_database_names())


# In[36]:


db = Client.TFG


# In[37]:


collect_names = db.list_collection_names()


# In[38]:


collect_names


# In[31]:


import json
from pymongo import MongoClient

URL = ( r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019\20190523.json")
client = MongoClient('localhost', 27017)
db = client['TFG']
collection_currency = db['Data']

with open(URL) as f:
    file_data = json.load(f)

import traceback
try:
    result = collection_currency.insert_one(file_data) 
    print (result.inserted_id)
except Exception:
    traceback.print_exc()

client.close()


# In[ ]:





# In[27]:


client = MongoClient('localhost', 27017)
db = client['TFG']
collection_currency = db['Data']


# In[28]:


print (collection_currency)


# In[7]:


import json
from pymongo import MongoClient 
   
URL = ( r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019\20190523.json")
myclient = MongoClient('localhost', 27017) 
   
db = myclient["TFG"]

Collection = db["Data"]
  
with open(URL) as file:
    file_data = json.load(file)
      

if isinstance(file_data, list):
    Collection.insert_many(file_data)  
else:
    Collection.insert_one(file_data)


# In[2]:


print (file_data)


# In[25]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["TFG"] ["Data2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
df = pd.DataFrame (data) # leer la tabla completa (DataFrame)


# In[26]:


df


# In[3]:


datos = df.iloc[215]
datos


# In[51]:


datos = df.iloc[100]
datos


# In[4]:


print(type(datos))


# In[5]:


data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
data


# In[6]:


Only_data = data.drop([0,1,2,3], axis = 0)
Only_data


# In[7]:


DATA = Only_data.iloc[:, [1]] #columna Data
DATA


# In[8]:


NewDatas = DATA.explode("Data")
NewDatas


# In[9]:


import pandas as pd
df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
df_final


# In[10]:


observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
observations


# In[22]:


NewDatas2 = observations.explode("observations")
NewDatas2


# In[12]:


datos_1 = NewDatas2.iloc[1000]['observations']
datos_1


# In[13]:


clientMac = datos_1['clientMac']
clientMac


# El siguiente script imprime sólo el primer elemento de cada objeto de la base de datos, hay que conseguir combinar este escript que recorre todos los objetos con el escript que recorre todas las observaciones.

# In[78]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []

index = 215
ids = -1

for i in range(index):
    ids = ids + 1
    print(ids)
    datos = df.iloc[0]
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    datos_x = NewDatas2.iloc[ids]['observations']
    clientMac = datos_x['clientMac']
    Direcciones_Mac.append(clientMac)
    rssi = datos_x['rssi']
    señal_rssi.append(rssi)
    seentime = datos_x['seenTime']
    seenTime.append(seentime)
    seenepoch = datos_x['seenEpoch']
    seenEpoch.append(seenepoch)


# In[ ]:





# In[14]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []

index = 215
ids = -1

for i in range(index):
    ids = ids + 1
    datos = df.iloc[ids]
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    datos_x = NewDatas2.iloc[ids]['observations']
    clientMac = datos_x['clientMac']
    Direcciones_Mac.append(clientMac)
    rssi = datos_x['rssi']
    señal_rssi.append(rssi)
    seentime = datos_x['seenTime']
    seenTime.append(seentime)
    seenepoch = datos_x['seenEpoch']
    seenEpoch.append(seenepoch)
    

data = {'Direcciones_Mac': Direcciones_Mac,
        'rssi_signal': señal_rssi,
        'seenTime': seenTime,
        'seenEpoch': seenEpoch}

df = pd.DataFrame(data, columns = ['Direcciones_Mac', 'rssi_signal', 'seenTime', 'seenEpoch'])
#df.to_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
df.to_csv(r"Datos_Para_Análisis2.csv")


# In[18]:


import pandas as pd
 
datos = pd.read_csv("Datos_Para_Análisis2.csv")
datos


# In[58]:


datos = df.iloc[1]
datos


# In[79]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []

index = 215
ids = -1
ids2 = -1

for i in range(index):
    ids = ids + 1
    datos = df.iloc[ids]
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    index2 = len(NewDatas2)
    print (len(NewDatas2))

    for p in range(index2):
        ids2 = ids2 + 1
        print(ids2)
        datos_x = NewDatas2.iloc[ids2]['observations']
        clientMac = datos_x['clientMac']
        Direcciones_Mac.append(clientMac)
        rssi = datos_x['rssi']
        señal_rssi.append(rssi)
        seentime = datos_x['seenTime']
        seenTime.append(seentime)
        seenepoch = datos_x['seenEpoch']
        seenEpoch.append(seenepoch)
    

#data = {'Direcciones_Mac': Direcciones_Mac,
#        'rssi_signal': señal_rssi,
#        'seenTime': seenTime,
#        'seenEpoch': seenEpoch}

#df = pd.DataFrame(data, columns = ['Direcciones_Mac', 'rssi_signal', 'seenTime', 'seenEpoch'])
#df.to_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
#df.to_csv(r"Datos_Para_Análisis3.csv")


# In[95]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []
lens = []

index = 215
ids = -1
ids2 = -1

for i in range(index):
    ids = ids + 1
    #print(ids)
    datos = df.iloc[ids]
    #print(datos)
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    index2 = len(NewDatas2)
    print(NewDatas2, index2)
    lens.append(index2)
    #print (len(NewDatas2))


# In[103]:


len(lens)


# In[87]:


ids = -1
index = len(NewDatas2)

for i in range(index):
    ids = ids + 1
    datos= NewDatas2.iloc[ids]['observations']
    clientMac = datos['clientMac']
    rssi = datos['rssi']
    seentime = datos['seenTime']
    seenepoch = datos['seenEpoch']
    
    Direcciones_Mac.append(clientMac)
    señal_rssi.append(rssi)
    seenTime.append(seentime)
    seenEpoch.append(seenepoch)


# In[31]:


Direcciones_Mac


# In[6]:


len(Direcciones_Mac)


# In[118]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []
lens = []

index = 215
ids = -1
ids2 = -1

for i in range(index):
    ids = ids + 1
    #print(ids)
    datos = df.iloc[ids]
    #print(datos)
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    index2 = len(NewDatas2)
    print(NewDatas2, index2)
    lens.append(index2)
    for i in range(index2):
        ids2 = ids2 + 1
        datas = NewDatas2.iloc[ids2]['observations']
        clientMac = datas['clientMac']
        rssi = datas['rssi']
        seentime = datas['seenTime']
        seenepoch = datas['seenEpoch']

        Direcciones_Mac.append(clientMac)
        señal_rssi.append(rssi)
        seenTime.append(seentime)
        seenEpoch.append(seenepoch)


# In[123]:


datas = NewDatas2.iloc[1776]['observations']
datas


# In[ ]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []
lens = []

index = 215
ids = -1
ids2 = -1

for i in range(index):
    ids = ids + 1
    #print(ids)
    datos = df.iloc[ids]
    #print(datos)
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    index2 = len(NewDatas2)
    print(NewDatas2, index2)
    lens.append(index2)
    ids2 = -1
    index = len(NewDatas2)

    for i in range(index):
        ids2 = ids2 + 1
        datos= NewDatas2.iloc[ids2]['observations']
        clientMac = datos['clientMac']
        rssi = datos['rssi']
        seentime = datos['seenTime']
        seenepoch = datos['seenEpoch']

        Direcciones_Mac.append(clientMac)
        señal_rssi.append(rssi)
        seenTime.append(seentime)
        seenEpoch.append(seenepoch)


# In[1]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection2020 = cliente ["TFG"] ["Data2020"]
data2020 = collection2020.find()
data2020 = list (data2020) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
df2 = pd.DataFrame (data2020) # leer la tabla completa (DataFrame)
df2


# In[3]:


len(df2)


# In[3]:


datos = df2.iloc[2]
datos


# In[4]:


import pandas as pd

Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []
lens = []

index = len(df2)
ids = -1

for i in range(index):
    ids = ids + 1
    #print(ids)
    datos = df2.iloc[ids]
    #print(datos)
    data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
    Only_data = data.drop([0,1,2,3], axis = 0)
    DATA = Only_data.iloc[:, [1]] #columna Data
    NewDatas = DATA.explode("Data")
    df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
    observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
    NewDatas2 = observations.explode("observations")
    index2 = len(NewDatas2)
    #print(NewDatas2, index2)
    lens.append(index2)
    ids2 = -1
    index = len(NewDatas2)

    for i in range(index):
        ids2 = ids2 + 1
        datos= NewDatas2.iloc[ids2]['observations']
        clientMac = datos['clientMac']
        rssi = datos['rssi']
        seentime = datos['seenTime']
        seenepoch = datos['seenEpoch']

        Direcciones_Mac.append(clientMac)
        señal_rssi.append(rssi)
        seenTime.append(seentime)
        seenEpoch.append(seenepoch)
        
len(Direcciones_Mac)


# In[5]:


Direcciones_Mac

