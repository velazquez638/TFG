#!/usr/bin/env python
# coding: utf-8

# # Procesado y limpieza de datos

# ## Índice

# 1- Visualizado de los datos de un archivo .Json
# 
# 2- Conexión con la BBDD mongoDB en local
# 
# 3- Realizamos la ingesta de un único archivo a la BBDD
# 
# 4- Ingesta masiva de datos del 2019 en mongoDB
# 
# 5- Ingesta masiva de datos del 2020 en mongoDB
# 
# 6- Limpieza de datos
# 
# 7- Recopilación de todos los datos útiles de 2019
# 
# 8- Extracción de todos los objetos y datos de 2019
# 
# 9- Extracción de todos los objetos y datos de 2020
# 
# 10- Ingesta de datos útiles en MongoDB
# 
#     - Ingesta de los datos de 2019
#     - Ingesta de los datos de 2020

# ## Visualizado de los datos .json

# In[1]:


import pandas as pd
ruta = (r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019\20190520.json")
datos = pd.read_json(ruta, orient = 'records')
datos


# In[2]:


print(type(datos))


# Primera fila -> appFloors, muestra las columnas version, secret, type y data + Name con el nombre de la columna y el tipo

# In[3]:


datos.iloc[0] #primera fila


# Cuarta fila -> Observations, muestra las columnas version, secret, type y data + Name con el nombre de la columna y el tipo

# In[4]:


data = datos.iloc[3] # fila observations
data


# A continuación aislamos la columna data de la fila de observaciones, dicha columna contiene todos los datos

# In[5]:


columna_data = datos.iloc[:, [3]] #columna 3 (data)
columna_data


# Array de datos (data) aislado

# In[6]:


observations = columna_data.iloc[[3]]
observations


# A continuación imprimimos el primer elemento de datos almacenado en el array de "data" en la fila de observaciones

# In[7]:


#primer array de datos de observaciones
for client in observations['data']:
    cliente1 = client[0]
    
cliente1


# In[49]:


print(type(cliente1))


# ## Conectamos con la BBDD mongoDB en local

# In[8]:


import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
import json
import warnings
warnings.filterwarnings('ignore')


# In[9]:


Client = MongoClient('localhost', 27017)
Client.list_database_names()


# In[10]:


db = Client.TFG
collect_names = db.list_collection_names()
collect_names


# In[11]:


client = MongoClient('localhost', 27017)
db = client['TFG']
collection_currency = db['Data']


# In[12]:


print (collection_currency)


# ## Realizamos la ingesta de un único archivo a la BBDD

# In[13]:


import json
from pymongo import MongoClient

nombre = "20190520.json"
archivo = "/"+ nombre
URL = ( r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019" + archivo)

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


# Visualizado de los datos en mongoDB

# In[14]:


import pymongo
 
client = pymongo.MongoClient('localhost', 27017)
 
# Database Name
db = client["TFG"]
 
# Collection Name
col = db["Data"]
 
x = col.find_one()
print(x)


# ## Ingesta masiva de datos del 2019 en mongoDB

# A continuación listamos todos los archivos json de la ruta especificada

# In[15]:


from pathlib import Path

path = (r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019")
def ls3(path):
    return [obj.name for obj in Path(path).iterdir() if obj.is_file()]

archivos = ls3(path)
print (archivos)


# In[16]:


archivos[1]


# In[17]:


len(archivos)


# Ingestamos los archivos json en la BBDD

# In[18]:


import traceback

client = MongoClient('localhost', 27017)
db = client['TFG']
collection_currency = db['Data2019']

for n in archivos:
    nombre = n
    archivo = "/"+ nombre
    URL = ( r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2019" + archivo)
    with open(URL) as f:
        file_data = json.load(f)
    try:
        result = collection_currency.insert_one(file_data) 
        print (result.inserted_id)
    except Exception:
        traceback.print_exc()
        client.close()


# ## Ingesta masiva de datos 2020 en mongoDB

# In[19]:


from pathlib import Path

path = (r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2020")
def ls3(path):
    return [obj.name for obj in Path(path).iterdir() if obj.is_file()]

archivos2 = ls3(path)
print (archivos2)


# In[20]:


archivos2[1]


# In[21]:


len(archivos2)


# In[22]:


import traceback
from pymongo import MongoClient
import json


client = MongoClient('localhost', 27017)
db = client['TFG']
collection_currency = db['Data2020']

for n in archivos2:
    nombre = n
    archivo = "/"+ nombre
    URL = ( r"D:\Álvaro Velázquez\Desktop\UNI\TFG\Datos\2020" + archivo)
    with open(URL) as f:
        file_data = json.load(f)
    try:
        result = collection_currency.insert_one(file_data) 
        print (result.inserted_id)
    except Exception:
        traceback.print_exc()
        client.close()


# ## Limpieza de datos 

# Primero hay que eliminar los datos que no se van a usar.
# 
# Para ello voy a quedarme con los datos útiles y estos los voy a guardar por separado para su posterior análisis
# 
# Cómo he planteado la prueba de limpieza de datos:
# 
#     Primero-> Hacemos una llamada a la BBDD para traernos un único bloque de datos.
#     
#     Segundo-> Ese primer bloque de datos lo convertimos a formato DataFrame de Pandas
#     
#     Tercero-> Una vez lo tenemos en formato DataFrame, eliminamos las filas que no nos sirven para el análisis de datos 
#         (_id, version, secret, type)
#         
#     Cuarto-> Aislamos la columna que queremos analizar (observations) la cual contiene toda la información.
#     
#     Quinto-> Convertimos todos los elementos de la columna en filas de elementos independientes
#     
#     sexto-> Cada fila se convierte en un diccionario de python y podemos sacar el valor de los datos.

# In[1]:


import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["TFG"]
mycol = mydb["Data2019"]

x = mycol.find_one()

Primeros_datos = []
Primeros_datos = x
Primeros_datos


# In[2]:


print(type(Primeros_datos))


# In[3]:


for n in Primeros_datos:
    print (n)


# El siguiente script me devuelve todos los datos de mongoDB de 2019.
# 
# Nota: no ejecutar, se peta
# 
# Para poder aumentar la salida de datos, debemos abrir un nuevo cmd y escribir:
# 
# jupyter notebook --NotebookApp.iopub_data_rate_limit=10000000
# 
# ---

# import pymongo
# 
# myclient = pymongo.MongoClient("mongodb://localhost:27017/")
# 
# 
# mydb = myclient["TFG"]
# 
# 
# mycol = mydb["Data2019"]
# 
# 
# for x in mycol.find():
# 
# 
#     print(x)
#     

# Creamos un data frame de pandas para gestionar los datos de la BBDD, en este ejemplo estamos trabajando con el primer bloque de datos de la BBDD de 2019

# In[4]:


import pandas as pd


# In[5]:


print(pd.DataFrame(list(Primeros_datos.items()),
                   columns=['Name', 'Data']))


# In[6]:


data = pd.DataFrame(list(Primeros_datos.items()),columns=['Name', 'Data'])
data


# In[7]:


print(type(data))


# A continuación con el método Drop, eliminamos las filas que no nos interesan para el análisis

# In[8]:


Only_data = data.drop([0,1,2,3], axis = 0)
Only_data


# In[9]:


DATA = Only_data.iloc[:, [1]] #columna Data
DATA


# In[10]:


print(type(DATA))


# In[11]:


DATA


# In[12]:


print(type(DATA))


# Al tener un data frame de pandas podemos aplicar el método explode para separar los campos

# In[13]:


NewDatas = DATA.explode("Data")
NewDatas


# En la siguiente líena convertimos el pandas Data Frame en Json file

# In[27]:


#DATA.to_json(r'dattaas.json')


# https://ichi.pro/es/extraccion-de-datos-analice-un-objeto-json-anidado-de-3-y-conviertalo-en-un-marco-de-datos-pandas-39357327828653

# In[15]:


import pandas as pd
df_final = (
    pd.DataFrame(DATA['Data'].apply(pd.Series))
)
df_final


# A continuación eliminamos las columnas que no necesitamos (apMac, apFloors, apTags)

# In[16]:


observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
observations


#  continuación nos quedamos con los valores que realmente necesitamos separados en filas

# In[17]:


NewDatas2 = observations.explode("observations")
NewDatas2


# In[18]:


print(type(NewDatas2))


# In[19]:


len(NewDatas2)


# Cada fila de mi data frame es un dic (diccionario de python)

# In[20]:


datos_1 = NewDatas2.iloc[0]['observations']
datos_1


# In[21]:


print (type(datos_1))


# Para acceder a un elemento de un diccionario se hace de la siguiente manera:

# In[22]:


clientMac = datos_1['clientMac']
clientMac


# ## recopilación de todos los datos útiles de 2019

# Conseguir crear un array de datos por tipo de datos:
#         
#         Array con todas las direcciones MAC
#         
#         Array con la señal rssi
#         
#         Array con seeTime
#         
#         Array con seeRpoch
#         

# creación de los arrays:

# In[23]:


Direcciones_Mac = []
señal_rssi = []
seenTime = []
seenEpoch = []


# En el siguiente bloque de código extraemos los datos que queremos para analizar

# In[24]:


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


# In[26]:


Direcciones_Mac


# In[27]:


len(Direcciones_Mac)


# In[149]:


señal_rssi


# In[150]:


seenTime


# In[151]:


seenEpoch


# A continuación exportamos los datos en csv, para tener los datos que vamos a analizar en un único documento

# In[156]:


import pandas as pd

data = {'Direcciones_Mac': Direcciones_Mac,
        'rssi_signal': señal_rssi,
        'seenTime': seenTime,
        'seenEpoch': seenEpoch}

df = pd.DataFrame(data, columns = ['Direcciones_Mac', 'rssi_signal', 'seenTime', 'seenEpoch'])
#df.to_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
df.to_csv(r"Datos_Para_Análisis.csv")


# In[160]:


import pandas as pd
 
datos = pd.read_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
datos


# ### Nota: hasta ahora los datos que hemos recopilado son los correspondientes al primer objeto de la BBDD de 
# ### Data2019

# ## Extracción de todos los objetos y datos de 2019

# En el siguiente script nos vamos a traer todos los datos de 2019

# In[1]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["TFG"] ["Data2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
df = pd.DataFrame (data) # leer la tabla completa (DataFrame)


# In[2]:


df


# In[31]:


datos = df.iloc[215]
datos


# In[32]:


data = pd.DataFrame(list(datos.items()),columns=['Name', 'Data'])
data


# In[33]:


Only_data = data.drop([0,1,2,3], axis = 0)
Only_data


# In[34]:


DATA = Only_data.iloc[:, [1]] #columna Data
DATA


# In[35]:


NewDatas = DATA.explode("Data")
NewDatas


# In[36]:


import pandas as pd
df_final = (pd.DataFrame(DATA['Data'].apply(pd.Series)))
df_final


# In[37]:


observations = df_final.drop(['apMac', 'apFloors', 'apTags'], axis=1)
observations


# In[38]:


NewDatas2 = observations.explode("observations")
NewDatas2


# In[39]:


datos_1 = NewDatas2.iloc[1000]['observations']
datos_1


# In[40]:


clientMac = datos_1['clientMac']
clientMac


# El siguiente script imprime sólo el primer elemento de cada objeto de la base de datos, hay que conseguir combinar este escript que recorre todos los objetos con el escript que recorre todas las observaciones.

# In[41]:


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

datos = pd.read_csv("Datos_Para_Análisis2.csv")
datos


# In[46]:


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


# In[47]:


Direcciones_Mac


# In[48]:


len(Direcciones_Mac)


# Con el siguiente script recorremos todos los elementos de todos los objetos

# In[3]:


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


# Con el siguiente script recorremos todos los elementos de todos los objetos y guardamos los datos que queremos en los arrays:
#     
#     - Direcciones_Mac
#     
#     - señal_rssi
#     
#     - seenTime
#     
#     - seenEpoch

# In[3]:


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


# In[4]:


len(Direcciones_Mac)


# In[5]:


Direcciones_Mac


# In[18]:


len(señal_rssi)


# In[57]:


señal_rssi


# In[20]:


len(seenTime)


# In[56]:


seenTime


# In[22]:


len(seenEpoch)


# In[55]:


seenEpoch


# In[13]:


data = {'Direcciones_Mac': Direcciones_Mac,
        'rssi_signal': señal_rssi,
        'seenTime': seenTime,
        'seenEpoch': seenEpoch}

df = pd.DataFrame(data, columns = ['Direcciones_Mac', 'rssi_signal', 'seenTime', 'seenEpoch'])
df.to_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
#df.to_csv(r"Datos_Para_Análisis2.csv")


# In[2]:


import pandas as pd
datos = pd.read_csv(r"C:\Users\alvar\OneDrive\Escritorio\Datos_Para_Análisis.csv")
datos


# ## Extracción de todos los objetos y datos de 2020

# In[1]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection2020 = cliente ["TFG"] ["Data2020"]
data2020 = collection2020.find()
data2020 = list (data2020) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
df2 = pd.DataFrame (data2020) # leer la tabla completa (DataFrame)


# In[2]:


df2


# In[3]:


len(df2)


# In[4]:


import pandas as pd

Direcciones_Mac_2020 = []
señal_rssi_2020 = []
seenTime_2020 = []
seenEpoch_2020 = []
lens = []

index = len(df2)
ids = -1
ids2 = -1

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

        Direcciones_Mac_2020.append(clientMac)
        señal_rssi_2020.append(rssi)
        seenTime_2020.append(seentime)
        seenEpoch_2020.append(seenepoch)
    
print(len(Direcciones_Mac_2020))
print(len(señal_rssi_2020))
print(len(seenTime_2020))
print(len(seenEpoch_2020))


# ## Ingesta de datos útiles en MongoDB

# ### Ingesta de los datos de 2019 en MongoDB 

# Al tener un tamaño tan grande cada array de datos, tenemos que dividirlos en 8 partes para poder ingestarlos en la BBDD. 
# 
# Una vez divididos los array se quedan en formato numpy.ndarray y para poder ingreserlos en la BBDD necesitamos convertir a tipo list

# In[6]:


print(type(Direcciones_Mac))


# En el siguiente scripr dividimos todos las listas en 8 arrays independientes

# In[7]:


import numpy as np
Direcciones_Mac_Partes = np.array_split(Direcciones_Mac, 8)
señal_rssi_Partes = np.array_split(señal_rssi, 8)
seenTime_Partes = np.array_split(seenTime, 8)
seenEpoch_Partes = np.array_split(seenEpoch, 8)


# In[8]:


Direcciones_Mac_Partes


# In[9]:


señal_rssi_Partes


# In[10]:


seenTime_Partes


# In[12]:


seenEpoch_Partes


# In[13]:


print (len(Direcciones_Mac_Partes[0]),len(Direcciones_Mac_Partes[1]),
       len(Direcciones_Mac_Partes[2]),len(Direcciones_Mac_Partes[3]),len(Direcciones_Mac_Partes[4]),
       len(Direcciones_Mac_Partes[5]),len(Direcciones_Mac_Partes[6]),len(Direcciones_Mac_Partes[7]))


# In[14]:


print (len(señal_rssi_Partes[0]),len(señal_rssi_Partes[1]),
       len(señal_rssi_Partes[2]),len(señal_rssi_Partes[3]),len(señal_rssi_Partes[4]),
       len(señal_rssi_Partes[5]),len(señal_rssi_Partes[6]),len(señal_rssi_Partes[7]))


# In[15]:


print (len(seenTime_Partes[0]),len(seenTime_Partes[1]),
       len(seenTime_Partes[2]),len(seenTime_Partes[3]),len(seenTime_Partes[4]),
       len(seenTime_Partes[5]),len(seenTime_Partes[6]),len(seenTime_Partes[7]))


# In[16]:


print (len(seenEpoch_Partes[0]),len(seenEpoch_Partes[1]),
       len(seenEpoch_Partes[2]),len(seenEpoch_Partes[3]),len(seenEpoch_Partes[4]),
       len(seenEpoch_Partes[5]),len(seenEpoch_Partes[6]),len(seenEpoch_Partes[7]))


# In[109]:


type(Direcciones_Mac_Partes[0])


# Convertimos los numpy.ndarray en class 'list'

# In[22]:


Direcciones_Mac_Parte_1 = Direcciones_Mac_Partes[0].tolist()
Direcciones_Mac_Parte_2 = Direcciones_Mac_Partes[1].tolist()
Direcciones_Mac_Parte_3 = Direcciones_Mac_Partes[2].tolist()
Direcciones_Mac_Parte_4 = Direcciones_Mac_Partes[3].tolist()
Direcciones_Mac_Parte_5 = Direcciones_Mac_Partes[4].tolist()
Direcciones_Mac_Parte_6 = Direcciones_Mac_Partes[5].tolist()
Direcciones_Mac_Parte_7 = Direcciones_Mac_Partes[6].tolist()
Direcciones_Mac_Parte_8 = Direcciones_Mac_Partes[7].tolist()


print (type(Direcciones_Mac_Parte_1),type(Direcciones_Mac_Parte_2),type(Direcciones_Mac_Parte_3),
       type(Direcciones_Mac_Parte_4),type(Direcciones_Mac_Parte_5),type(Direcciones_Mac_Parte_6),
       type(Direcciones_Mac_Parte_7),type(Direcciones_Mac_Parte_8))


# In[23]:


Direcciones_Macs = [Direcciones_Mac_Parte_1, Direcciones_Mac_Parte_2, Direcciones_Mac_Parte_3, Direcciones_Mac_Parte_4,
                   Direcciones_Mac_Parte_5, Direcciones_Mac_Parte_6, Direcciones_Mac_Parte_7, Direcciones_Mac_Parte_8]

len(Direcciones_Macs)


# In[24]:


Direcciones_Macs[0]


# Cargamos en la BBDD todas las direcciones MAC

# In[25]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["Direcciones_Macs"]
Datos_Análisis_2019 = collection.Datos_Análisis_2019

for i in range(len(Direcciones_Macs)):
    
    Direccion_Macs = Direcciones_Macs[i]
    direccion_macs = {
        "Direcciones_Macs": Direccion_Macs
    }
    
    id = Datos_Análisis_2019.insert_one(direccion_macs).inserted_id
    print(id)


# In[26]:


señal_rssi_Partes_1 = señal_rssi_Partes[0].tolist()
señal_rssi_Partes_2 = señal_rssi_Partes[1].tolist()
señal_rssi_Partes_3 = señal_rssi_Partes[2].tolist()
señal_rssi_Partes_4 = señal_rssi_Partes[3].tolist()
señal_rssi_Partes_5 = señal_rssi_Partes[4].tolist()
señal_rssi_Partes_6 = señal_rssi_Partes[5].tolist()
señal_rssi_Partes_7 = señal_rssi_Partes[6].tolist()
señal_rssi_Partes_8 = señal_rssi_Partes[7].tolist()


print (type(señal_rssi_Partes_1),type(señal_rssi_Partes_2),type(señal_rssi_Partes_3),
       type(señal_rssi_Partes_4),type(señal_rssi_Partes_5),type(señal_rssi_Partes_6),
       type(señal_rssi_Partes_7),type(señal_rssi_Partes_8))


# In[37]:


Señales_rssi = [señal_rssi_Partes_1, señal_rssi_Partes_2, señal_rssi_Partes_3, señal_rssi_Partes_4,
                   señal_rssi_Partes_5, señal_rssi_Partes_6, señal_rssi_Partes_7, señal_rssi_Partes_8]

len(Señales_rssi)


# Cargamos en la BBDD todas las señales rssi

# In[28]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["Señal_rssi"]
Datos_Análisis_2019 = collection.Datos_Análisis_2019

for i in range(len(Señales_rssi)):
    
    Señal_rssi = Señales_rssi[i]
    señal_rssi = {
        "señales_rssi": Señal_rssi
    }
    
    id = Datos_Análisis_2019.insert_one(señal_rssi).inserted_id
    print(id)


# In[32]:


seenEpoch_Partes_1 = seenEpoch_Partes[0].tolist()
seenEpoch_Partes_2 = seenEpoch_Partes[1].tolist()
seenEpoch_Partes_3 = seenEpoch_Partes[2].tolist()
seenEpoch_Partes_4 = seenEpoch_Partes[3].tolist()
seenEpoch_Partes_5 = seenEpoch_Partes[4].tolist()
seenEpoch_Partes_6 = seenEpoch_Partes[5].tolist()
seenEpoch_Partes_7 = seenEpoch_Partes[6].tolist()
seenEpoch_Partes_8 = seenEpoch_Partes[7].tolist()


print (type(seenEpoch_Partes_1),type(seenEpoch_Partes_2),type(seenEpoch_Partes_3),
       type(seenEpoch_Partes_4),type(seenEpoch_Partes_5),type(seenEpoch_Partes_6),
       type(seenEpoch_Partes_7),type(seenEpoch_Partes_8))


# In[38]:


SeenEpoch_Partes = [seenEpoch_Partes_1, seenEpoch_Partes_2, seenEpoch_Partes_3, seenEpoch_Partes_4,
                   seenEpoch_Partes_5, seenEpoch_Partes_6, seenEpoch_Partes_7, seenEpoch_Partes_8]

len(SeenEpoch_Partes)


# Cargamos en la BBDD todas las SeenEpoch

# In[47]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["SeenEpoch_Partes"]
Datos_Análisis_2019 = collection.Datos_Análisis_2019

for i in range(len(SeenEpoch_Partes)):
    
    SeenEpoch = SeenEpoch_Partes[i]
    seenEpoch_partes = {
        "SeenEpoch_Partes": SeenEpoch
    }
    
    id = Datos_Análisis_2019.insert_one(seenEpoch_partes).inserted_id
    print(id)


# In[51]:


seenTime_Partes_1 = seenTime_Partes[0].tolist()
seenTime_Partes_2 = seenTime_Partes[1].tolist()
seenTime_Partes_3 = seenTime_Partes[2].tolist()
seenTime_Partes_4 = seenTime_Partes[3].tolist()
seenTime_Partes_5 = seenTime_Partes[4].tolist()
seenTime_Partes_6 = seenTime_Partes[5].tolist()
seenTime_Partes_7 = seenTime_Partes[6].tolist()
seenTime_Partes_8 = seenTime_Partes[7].tolist()


print (type(seenTime_Partes_1),type(seenTime_Partes_2),type(seenTime_Partes_3),
       type(seenTime_Partes_4),type(seenTime_Partes_5),type(seenTime_Partes_6),
       type(seenTime_Partes_7),type(seenTime_Partes_8))


# In[52]:


SeenTime_Partes = [seenTime_Partes_1, seenTime_Partes_2, seenTime_Partes_3, seenTime_Partes_4,
                   seenTime_Partes_5, seenTime_Partes_6, seenTime_Partes_7, seenTime_Partes_8]

len(SeenTime_Partes)


# In[53]:


len(SeenTime_Partes[1])


# Cargamos en la BBDD todas las SeenTime

# In[46]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["SeenTime"]
Datos_Análisis_2019 = collection.Datos_Análisis_2019

for i in range(len(SeenTime_Partes)):
    
    SeenTime = SeenTime_Partes[i]
    seenTime_partes = {
        "SeenTime_Partes": SeenTime
    }
    
    id = Datos_Análisis_2019.insert_one(seenTime_partes).inserted_id
    print(id)


# ### Ingesta de los datos de 2020 en MongoDB 

# In[6]:


print(type(Direcciones_Mac_2020))


# In[65]:


import numpy as np
Direcciones_Mac_Partes_2020 = np.array_split(Direcciones_Mac_2020, 11)
señal_rssi_Partes_2020 = np.array_split(señal_rssi_2020, 11)
seenTime_Partes_2020 = np.array_split(seenTime_2020, 12)
seenEpoch_Partes_2020 = np.array_split(seenEpoch_2020, 11)


# In[69]:


print (len(Direcciones_Mac_Partes_2020[0]),len(Direcciones_Mac_Partes_2020[1]),
       len(Direcciones_Mac_Partes_2020[2]),len(Direcciones_Mac_Partes_2020[3]),len(Direcciones_Mac_Partes_2020[4]),
       len(Direcciones_Mac_Partes_2020[5]),len(Direcciones_Mac_Partes_2020[6]),len(Direcciones_Mac_Partes_2020[7]),
       len(Direcciones_Mac_Partes_2020[8]),len(Direcciones_Mac_Partes_2020[9]), len(Direcciones_Mac_Partes_2020[10]))


# In[41]:


print (len(señal_rssi_Partes_2020[0]),len(señal_rssi_Partes_2020[1]),
       len(señal_rssi_Partes_2020[2]),len(señal_rssi_Partes_2020[3]),len(señal_rssi_Partes_2020[4]),
       len(señal_rssi_Partes_2020[5]),len(señal_rssi_Partes_2020[6]),len(señal_rssi_Partes_2020[7]),
       len(señal_rssi_Partes_2020[8]),len(señal_rssi_Partes_2020[9]), len(señal_rssi_Partes_2020[10]))


# In[70]:


print (len(seenTime_Partes_2020[0]),len(seenTime_Partes_2020[1]),
       len(seenTime_Partes_2020[2]),len(seenTime_Partes_2020[3]),len(seenTime_Partes_2020[4]),
       len(seenTime_Partes_2020[5]),len(seenTime_Partes_2020[6]),len(seenTime_Partes_2020[7]),
       len(seenTime_Partes_2020[8]),len(seenTime_Partes_2020[9]),len(seenTime_Partes_2020[10]), 
       len(seenTime_Partes_2020[11]))


# In[44]:


print (len(seenEpoch_Partes_2020[0]),len(seenEpoch_Partes_2020[1]),
       len(seenEpoch_Partes_2020[2]),len(seenEpoch_Partes_2020[3]),len(seenEpoch_Partes_2020[4]),
       len(seenEpoch_Partes_2020[5]),len(seenEpoch_Partes_2020[6]),len(seenEpoch_Partes_2020[7]),
       len(seenEpoch_Partes_2020[8]),len(seenEpoch_Partes_2020[9]),len(seenEpoch_Partes_2020[10]))


# In[46]:


Direcciones_Mac_Parte_1 = Direcciones_Mac_Partes_2020[0].tolist()
Direcciones_Mac_Parte_2 = Direcciones_Mac_Partes_2020[1].tolist()
Direcciones_Mac_Parte_3 = Direcciones_Mac_Partes_2020[2].tolist()
Direcciones_Mac_Parte_4 = Direcciones_Mac_Partes_2020[3].tolist()
Direcciones_Mac_Parte_5 = Direcciones_Mac_Partes_2020[4].tolist()
Direcciones_Mac_Parte_6 = Direcciones_Mac_Partes_2020[5].tolist()
Direcciones_Mac_Parte_7 = Direcciones_Mac_Partes_2020[6].tolist()
Direcciones_Mac_Parte_8 = Direcciones_Mac_Partes_2020[7].tolist()
Direcciones_Mac_Parte_9 = Direcciones_Mac_Partes_2020[8].tolist()
Direcciones_Mac_Parte_10 = Direcciones_Mac_Partes_2020[9].tolist()
Direcciones_Mac_Parte_11 = Direcciones_Mac_Partes_2020[10].tolist()

Direcciones_Macs = [Direcciones_Mac_Parte_1, Direcciones_Mac_Parte_2, Direcciones_Mac_Parte_3, Direcciones_Mac_Parte_4,
                   Direcciones_Mac_Parte_5, Direcciones_Mac_Parte_6, Direcciones_Mac_Parte_7, Direcciones_Mac_Parte_8,
                   Direcciones_Mac_Parte_9, Direcciones_Mac_Parte_10, Direcciones_Mac_Parte_11]

print (type(Direcciones_Mac_Parte_1),type(Direcciones_Mac_Parte_2),type(Direcciones_Mac_Parte_3),
       type(Direcciones_Mac_Parte_4),type(Direcciones_Mac_Parte_5),type(Direcciones_Mac_Parte_6),
       type(Direcciones_Mac_Parte_7),type(Direcciones_Mac_Parte_8),
       type(Direcciones_Mac_Parte_9),type(Direcciones_Mac_Parte_10),type(Direcciones_Mac_Parte_11))

print(len(Direcciones_Macs))


# In[58]:


señal_rssi_Partes_1 = señal_rssi_Partes_2020[0].tolist()
señal_rssi_Partes_2 = señal_rssi_Partes_2020[1].tolist()
señal_rssi_Partes_3 = señal_rssi_Partes_2020[2].tolist()
señal_rssi_Partes_4 = señal_rssi_Partes_2020[3].tolist()
señal_rssi_Partes_5 = señal_rssi_Partes_2020[4].tolist()
señal_rssi_Partes_6 = señal_rssi_Partes_2020[5].tolist()
señal_rssi_Partes_7 = señal_rssi_Partes_2020[6].tolist()
señal_rssi_Partes_8 = señal_rssi_Partes_2020[7].tolist()
señal_rssi_Partes_9 = señal_rssi_Partes_2020[8].tolist()
señal_rssi_Partes_10 = señal_rssi_Partes_2020[9].tolist()
señal_rssi_Partes_11 = señal_rssi_Partes_2020[10].tolist()


Señales_rssi = [señal_rssi_Partes_1, señal_rssi_Partes_2, señal_rssi_Partes_3, señal_rssi_Partes_4,
                   señal_rssi_Partes_5, señal_rssi_Partes_6, señal_rssi_Partes_7, señal_rssi_Partes_8,
               señal_rssi_Partes_9,señal_rssi_Partes_10,señal_rssi_Partes_10]

print (type(señal_rssi_Partes_1),type(señal_rssi_Partes_2),type(señal_rssi_Partes_3),
       type(señal_rssi_Partes_4),type(señal_rssi_Partes_5),type(señal_rssi_Partes_6),
       type(señal_rssi_Partes_7),type(señal_rssi_Partes_8),type(señal_rssi_Partes_9),type(señal_rssi_Partes_10),
       type(señal_rssi_Partes_11))

print(len(Señales_rssi))


# In[59]:


seenEpoch_Partes_1 = seenEpoch_Partes_2020[0].tolist()
seenEpoch_Partes_2 = seenEpoch_Partes_2020[1].tolist()
seenEpoch_Partes_3 = seenEpoch_Partes_2020[2].tolist()
seenEpoch_Partes_4 = seenEpoch_Partes_2020[3].tolist()
seenEpoch_Partes_5 = seenEpoch_Partes_2020[4].tolist()
seenEpoch_Partes_6 = seenEpoch_Partes_2020[5].tolist()
seenEpoch_Partes_7 = seenEpoch_Partes_2020[6].tolist()
seenEpoch_Partes_8 = seenEpoch_Partes_2020[7].tolist()
seenEpoch_Partes_9 = seenEpoch_Partes_2020[8].tolist()
seenEpoch_Partes_10 = seenEpoch_Partes_2020[9].tolist()
seenEpoch_Partes_11 = seenEpoch_Partes_2020[1].tolist()


SeenEpoch_Partes = [seenEpoch_Partes_1, seenEpoch_Partes_2, seenEpoch_Partes_3, seenEpoch_Partes_4,
                   seenEpoch_Partes_5, seenEpoch_Partes_6, seenEpoch_Partes_7, seenEpoch_Partes_8, seenEpoch_Partes_9,
                   seenEpoch_Partes_10, seenEpoch_Partes_10]

print (type(seenEpoch_Partes_1),type(seenEpoch_Partes_2),type(seenEpoch_Partes_3),
       type(seenEpoch_Partes_4),type(seenEpoch_Partes_5),type(seenEpoch_Partes_6),
       type(seenEpoch_Partes_7),type(seenEpoch_Partes_8), type(seenEpoch_Partes_9),type(seenEpoch_Partes_10)
      ,type(seenEpoch_Partes_10))

print(len(SeenEpoch_Partes))


# In[66]:


seenTime_Partes_1 = seenTime_Partes_2020[0].tolist()
seenTime_Partes_2 = seenTime_Partes_2020[1].tolist()
seenTime_Partes_3 = seenTime_Partes_2020[2].tolist()
seenTime_Partes_4 = seenTime_Partes_2020[3].tolist()
seenTime_Partes_5 = seenTime_Partes_2020[4].tolist()
seenTime_Partes_6 = seenTime_Partes_2020[5].tolist()
seenTime_Partes_7 = seenTime_Partes_2020[6].tolist()
seenTime_Partes_8 = seenTime_Partes_2020[7].tolist()
seenTime_Partes_9 = seenTime_Partes_2020[8].tolist()
seenTime_Partes_10 = seenTime_Partes_2020[9].tolist()
seenTime_Partes_11 = seenTime_Partes_2020[10].tolist()
seenTime_Partes_12 = seenTime_Partes_2020[11].tolist()



SeenTime_Partes = [seenTime_Partes_1, seenTime_Partes_2, seenTime_Partes_3, seenTime_Partes_4,
                   seenTime_Partes_5, seenTime_Partes_6, seenTime_Partes_7, seenTime_Partes_8, seenTime_Partes_9,
                   seenTime_Partes_10, seenTime_Partes_11, seenTime_Partes_12]


print (type(seenTime_Partes_1),type(seenTime_Partes_2),type(seenTime_Partes_3),
       type(seenTime_Partes_4),type(seenTime_Partes_5),type(seenTime_Partes_6),
       type(seenTime_Partes_7),type(seenTime_Partes_8), type(seenTime_Partes_9),type(seenTime_Partes_10),
       type(seenTime_Partes_11), type(seenTime_Partes_11))

print(len(SeenTime_Partes))


# In[53]:


len(SeenTime_Partes[1])


# In[61]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["Direcciones_Macs"]
Datos_Análisis_2020 = collection.Datos_Análisis_2020

for i in range(len(Direcciones_Macs)):
    
    Direccion_Macs = Direcciones_Macs[i]
    direccion_macs = {
        "Direcciones_Macs": Direccion_Macs
    }
    
    id = Datos_Análisis_2020.insert_one(direccion_macs).inserted_id
    print(id)


# In[62]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["Señal_rssi"]
Datos_Análisis_2020 = collection.Datos_Análisis_2020

for i in range(len(Señales_rssi)):
    
    Señal_rssi = Señales_rssi[i]
    señal_rssi = {
        "señales_rssi": Señal_rssi
    }
    
    id = Datos_Análisis_2020.insert_one(señal_rssi).inserted_id
    print(id)


# In[63]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["SeenEpoch_Partes"]
Datos_Análisis_2020 = collection.Datos_Análisis_2020

for i in range(len(SeenEpoch_Partes)):
    
    SeenEpoch = SeenEpoch_Partes[i]
    seenEpoch_partes = {
        "SeenEpoch_Partes": SeenEpoch
    }
    
    id = Datos_Análisis_2020.insert_one(seenEpoch_partes).inserted_id
    print(id)


# In[67]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["SeenTime"]
Datos_Análisis_2020 = collection.Datos_Análisis_2020

for i in range(len(SeenTime_Partes)):
    
    SeenTime = SeenTime_Partes[i]
    seenTime_partes = {
        "SeenTime_Partes": SeenTime
    }
    
    id = Datos_Análisis_2020.insert_one(seenTime_partes).inserted_id
    print(id)

