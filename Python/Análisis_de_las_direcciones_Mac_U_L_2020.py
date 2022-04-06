#!/usr/bin/env python
# coding: utf-8

# # Análisis de las direcciones Mac

# # Índice

# 1 - Traernos los datos de MongoDB
# 
#     - Datos 2020
#     
# 2 - Limpieza de direcciones Mac, ver cuantas de ellas son falsas
# 
#     Para ello dividimos el array de direcciones Mac en 5 partes y separamos las Mac por universales o locales
# 
# 3 - Subimos a BBDD los diferentes arrays, uno con las direcciones locales y otro con las direcciones universales
# 
#     Hemos repetido el proceso de subida de datos 5 veces una por cada división

# # Direcciones Mac 2019

# In[1]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["Direcciones_Macs.Datos_Análisis_2020"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Direcciones_Mac_2020 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
Direcciones_Mac_2020


# In[2]:


Direcciones_Mac_2020_1 = Direcciones_Mac_2020.iloc[0]
Direcciones_Mac_2020_2 = Direcciones_Mac_2020.iloc[1]
Direcciones_Mac_2020_3 = Direcciones_Mac_2020.iloc[2]
Direcciones_Mac_2020_4 = Direcciones_Mac_2020.iloc[3]
Direcciones_Mac_2020_5 = Direcciones_Mac_2020.iloc[4]
Direcciones_Mac_2020_6 = Direcciones_Mac_2020.iloc[5]
Direcciones_Mac_2020_7 = Direcciones_Mac_2020.iloc[6]
Direcciones_Mac_2020_8 = Direcciones_Mac_2020.iloc[7]
Direcciones_Mac_2020_9 = Direcciones_Mac_2020.iloc[8]
Direcciones_Mac_2020_10 = Direcciones_Mac_2020.iloc[9]
Direcciones_Mac_2020_11 = Direcciones_Mac_2020.iloc[10]


Direcciones_Mac_2020_1[1]
Direcciones_Mac_2020_2[1]
Direcciones_Mac_2020_3[1]
Direcciones_Mac_2020_4[1]
Direcciones_Mac_2020_5[1]
Direcciones_Mac_2020_6[1]
Direcciones_Mac_2020_7[1]
Direcciones_Mac_2020_8[1]
Direcciones_Mac_2020_9[1]
Direcciones_Mac_2020_10[1]
Direcciones_Mac_2020_11[1]



Direcciones_MacS_2020 = (Direcciones_Mac_2020_1[1] + Direcciones_Mac_2020_2[1] + Direcciones_Mac_2020_3[1]
                         + Direcciones_Mac_2020_4[1] + Direcciones_Mac_2020_5[1] + Direcciones_Mac_2020_6[1]
                         + Direcciones_Mac_2020_7[1] + Direcciones_Mac_2020_8[1] + Direcciones_Mac_2020_9[1]
                         + Direcciones_Mac_2020_10[1] + Direcciones_Mac_2020_11[1])

Direcciones_MacS_2020


# In[3]:


len(Direcciones_MacS_2020)


# In[4]:


Direcciones_MacS_2020[0]


# In[5]:


type(Direcciones_MacS_2020)


# In[6]:


n=1000000
Lista_partida_direcciones_2020 = [Direcciones_MacS_2020[i:i + n] for i in range(0, len(Direcciones_MacS_2020), n)]
Lista_partida_direcciones_2020


# In[9]:


Lista_partida_direcciones_2020[5]


# In[10]:


len(Lista_partida_direcciones_2020[5])


# In[11]:


Parte_Macs_0 = Lista_partida_direcciones_2020[0]
Parte_Macs_1 = Lista_partida_direcciones_2020[1]
Parte_Macs_2 = Lista_partida_direcciones_2020[2]
Parte_Macs_3 = Lista_partida_direcciones_2020[3]
Parte_Macs_4 = Lista_partida_direcciones_2020[4]
Parte_Macs_5 = Lista_partida_direcciones_2020[5]


# In[52]:


import math
Direcciones_Mac_universales_2020_parte_5 = []
Direcciones_Mac_locales_2020_parte_5 = []

def binarizar(decimal):
    binario = ''
    while decimal // 2 != 0:
        binario = str(decimal % 2) + binario
        decimal = decimal // 2
    return str(decimal) + binario

for i in range(len(Parte_Macs_5)):
    
    direccion_i = Parte_Macs_5[i]
    primer_paquete = direccion_i[0:2]

    if (primer_paquete.isnumeric()):
        binario = binarizar(int(primer_paquete))
        binario_8 = binario.zfill(8)
        bit = int (binario_8[-2])
        print("numero decimal:", primer_paquete, "|numero en binario:",binario_8, "|bit U/L: ",bit )
        if (bit == 0):
            Direcciones_Mac_locales_2020_parte_5.append(direccion_i)
        elif(bit == 1):
            Direcciones_Mac_universales_2020_parte_5.append(direccion_i)


    else: 
        res = "{0:08b}".format(int(primer_paquete, 16))
        binary_8 = res.zfill(8)
        bit_res = binary_8[-2]
        print ("Numero Hexadecimal:", primer_paquete, "|numero en binario:", str(binary_8), "|bit U/L:  ", bit_res)
        if (bit_res == 0):
            Direcciones_Mac_locales_2020_parte_5.append(direccion_i)
        elif(bit_res == 1):
            Direcciones_Mac_universales_2020_parte_5.append(direccion_i)


# In[53]:


Direcciones_Mac_locales_2020_parte_5


# In[54]:


len(Direcciones_Mac_locales_2020_parte_5)


# In[47]:


len(Direcciones_Mac_locales_2020_parte_4)


# In[40]:


len(Direcciones_Mac_locales_2020_parte_3)


# In[33]:


len(Direcciones_Mac_locales_2020_parte_2)


# In[25]:


len(Direcciones_Mac_locales_2020_parte_1)


# In[15]:


len(Direcciones_Mac_locales_2020_parte_0)


# In[55]:


Direcciones_Mac_universales_2020_parte_5


# In[56]:


len(Direcciones_Mac_universales_2020_parte_5)


# In[49]:


len(Direcciones_Mac_universales_2020_parte_4)


# In[42]:


len(Direcciones_Mac_universales_2020_parte_3)


# In[35]:


len(Direcciones_Mac_universales_2020_parte_2)


# In[27]:


len(Direcciones_Mac_universales_2020_parte_1)


# In[17]:


len(Direcciones_Mac_universales_2020_parte_0)


# ## Insertamos en BBDD el array de Direcciones_Mac_Universales_2020

# In[57]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L_2020"] ["Parte_5_Universales"]
Direcciones_Mac_universales_2020 = collection.Datos_Análisis_2020

Parte_5 = Direcciones_Mac_universales_2020_parte_5
Direcciones_Mac_parte_5 = {
    "Direcciones_Mac_universales_2020_parte_5": Parte_5
}
    
id = Direcciones_Mac_universales_2020.insert_one(Direcciones_Mac_parte_5).inserted_id
print(id)


# In[19]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L_2020"] ["Parte_0_Universales.Datos_Análisis_2020"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
array = pd.DataFrame (data) # leer la tabla completa (DataFrame)
array


# ## Insertamos en BBDD el array de Direcciones_Mac_Locales_2020

# In[58]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L_2020"] ["Parte_5_Locales"]
Direcciones_Mac_locales_2020 = collection.Datos_Análisis_Locales_2020

Parte_5 = Direcciones_Mac_locales_2020_parte_5
Direcciones_Mac_L_parte_5 = {
    "Direcciones_Mac_locales_2020_parte_5": Parte_5
}
    
id = Direcciones_Mac_locales_2020.insert_one(Direcciones_Mac_L_parte_5).inserted_id
print(id)


# In[21]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L_2020"] ["Parte_0_Locales.Datos_Análisis_Locales_2020"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
array = pd.DataFrame (data) # leer la tabla completa (DataFrame)
array

