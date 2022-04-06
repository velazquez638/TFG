#!/usr/bin/env python
# coding: utf-8

# # Análisis de las direcciones Mac

# # Índice

# 1 - Traernos los datos de MongoDB
# 
#     - Datos 2019
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
collection = cliente ["Datos_Análisis_2019"] ["Direcciones_Macs.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Direcciones_Mac_2019 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
Direcciones_Mac_2019


# ## Diferenciar entre direcciones Universales y locales

# Las direcciones MAc poseen un bit que nos indican si la dirección Mac es Universal o Local:
# 
# Si una dirección mac tiene la siguiente estructura: ->  xxxxxx(U/L)x xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx 
# El segundo bit por la derecha del primer bloque nos indica si es una dirección universal o local.
# 
#      Bit a 0 -> Si el bit U/L está establecido en el valor 0, IEEE ha administrado la dirección a través de la designación    de un Id. de compañía.
#      
#      Bit a 1 -> Si el bit U/L está establecido en el valor 1, la dirección se administra localmente. El administrador de la red ha suplantado la dirección del fabricante y ha especificado otra dirección.
# 
# 
# A continuación vamos a separar las direcciones Mac de nuestra BBDD en direcciones Mac universales o locales.

# In[2]:


Direcciones_Mac_2019_1 = Direcciones_Mac_2019.iloc[0]
Direcciones_Mac_2019_2 = Direcciones_Mac_2019.iloc[1]
Direcciones_Mac_2019_3 = Direcciones_Mac_2019.iloc[2]
Direcciones_Mac_2019_4 = Direcciones_Mac_2019.iloc[3]
Direcciones_Mac_2019_5 = Direcciones_Mac_2019.iloc[4]
Direcciones_Mac_2019_6 = Direcciones_Mac_2019.iloc[5]
Direcciones_Mac_2019_7 = Direcciones_Mac_2019.iloc[6]
Direcciones_Mac_2019_8 = Direcciones_Mac_2019.iloc[7]

Direcciones_Mac_2019_1[1]
Direcciones_Mac_2019_2[1]
Direcciones_Mac_2019_3[1]
Direcciones_Mac_2019_4[1]
Direcciones_Mac_2019_5[1]
Direcciones_Mac_2019_6[1]
Direcciones_Mac_2019_7[1]
Direcciones_Mac_2019_8[1]



Direcciones_MacS_2019 = (Direcciones_Mac_2019_1[1] + Direcciones_Mac_2019_2[1] + Direcciones_Mac_2019_3[1]
                         + Direcciones_Mac_2019_4[1] + Direcciones_Mac_2019_5[1] + Direcciones_Mac_2019_6[1]
                         + Direcciones_Mac_2019_7[1] + Direcciones_Mac_2019_8[1])

Direcciones_MacS_2019


# In[3]:


len (Direcciones_MacS_2019)


# In[4]:


Direcciones_MacS_2019[0]


# In[5]:


type(Direcciones_MacS_2019)


# ### Scripts para pasar de decimal a binario y de hexadecimal a binario:

# In[11]:


def binarizar(decimal):
    binario = ''
    while decimal // 2 != 0:
        binario = str(decimal % 2) + binario
        decimal = decimal // 2
    return str(decimal) + binario

numero = 15
print("numero decimal:", numero)
print("numero en binario:",binarizar(numero))


# In[12]:


import math 
  
ini_string = "ba"
print ("NUmero Hexadecimal:", ini_string) 
res = "{0:08b}".format(int(ini_string, 16)) 
print ("numero en binario:", str(res)) 


# ## Aproximación a la automatización

# A continuación vamos a separa el primer paquete de números para analizar el bit (U/L)

# In[48]:


primera_dirección = Direcciones_MacS_2019[0]
primera_dirección


# In[49]:


primer_paquete = primera_dirección[0:2]
primer_paquete


# In[50]:


type(primer_paquete)


# In[51]:


import math


def binarizar(decimal):
    binario = ''
    while decimal // 2 != 0:
        binario = str(decimal % 2) + binario
        decimal = decimal // 2
    return str(decimal) + binario


if (primer_paquete.isnumeric()):
    binario = binarizar(int(primer_paquete))
    binario_8 = binario.zfill(8)
    bit = binario_8[-2]
    print("numero decimal:", primer_paquete, "|numero en binario:",binario_8, "|bit U/L: ",bit )

else: 
    res = "{0:08b}".format(int(primer_paquete, 16))
    binary_8 = res.zfill(8)
    bit_res = binary_8[-2]
    print ("Numero Hexadecimal:", primer_paquete, "|numero en binario:", str(binary_8), "|bit U/L:  ", bit_res) 

 


# ## Automatizado Bit U/L

# Debido a la densidad de datos vamos a dividir el array de direcciones Mac de 2019 (Direcciones_MacS_2019), en 4 arrays independientes de 1M de direcciones Mac cada uno.

# In[6]:


n=1000000
Lista_partida_direcciones_2019 = [Direcciones_MacS_2019[i:i + n] for i in range(0, len(Direcciones_MacS_2019), n)]
Lista_partida_direcciones_2019


# In[7]:


Parte_Macs_0 = Lista_partida_direcciones_2019[0]
Parte_Macs_1 = Lista_partida_direcciones_2019[1]
Parte_Macs_2 = Lista_partida_direcciones_2019[2]
Parte_Macs_3 = Lista_partida_direcciones_2019[3]
Parte_Macs_4 = Lista_partida_direcciones_2019[4]


# In[8]:


Parte_Macs_4


# In[9]:


len(Parte_Macs_4)


# In[43]:


Parte_Macs_0[0]


# En el siguiente script realizamos para cada parte la división entre Macs locales y Macs universales. Hemos tenido que repetir el siguiente scrit un total de 5 veces una vez por cada parte del array Direcciones_MacS_2019

# In[35]:


import math
Direcciones_Mac_universales_2019_parte_4 = []
Direcciones_Mac_locales_2019_parte_4 = []

def binarizar(decimal):
    binario = ''
    while decimal // 2 != 0:
        binario = str(decimal % 2) + binario
        decimal = decimal // 2
    return str(decimal) + binario

for i in range(len(Parte_Macs_4)):
    
    direccion_i = Parte_Macs_4[i]
    primer_paquete = direccion_i[0:2]

    if (primer_paquete.isnumeric()):
        binario = binarizar(int(primer_paquete))
        binario_8 = binario.zfill(8)
        bit = int (binario_8[-2])
        print("numero decimal:", primer_paquete, "|numero en binario:",binario_8, "|bit U/L: ",bit )
        if (bit == 0):
            Direcciones_Mac_locales_2019_parte_4.append(direccion_i)
        elif(bit == 1):
            Direcciones_Mac_universales_2019_parte_4.append(direccion_i)


    else: 
        res = "{0:08b}".format(int(primer_paquete, 16))
        binary_8 = res.zfill(8)
        bit_res = binary_8[-2]
        print ("Numero Hexadecimal:", primer_paquete, "|numero en binario:", str(binary_8), "|bit U/L:  ", bit_res)
        if (bit_res == 0):
            Direcciones_Mac_locales_2019_parte_4.append(direccion_i)
        elif(bit_res == 1):
            Direcciones_Mac_universales_2019_parte_4.append(direccion_i)


# In[36]:


Direcciones_Mac_locales_2019_parte_4


# In[37]:


len(Direcciones_Mac_locales_2019_parte_4)


# In[30]:


len(Direcciones_Mac_locales_2019_parte_3)


# In[22]:


len(Direcciones_Mac_locales_2019_parte_2)


# In[13]:


len(Direcciones_Mac_locales_2019_parte_1)


# In[62]:


len(Direcciones_Mac_locales_2019_parte_0)


# In[39]:


Direcciones_Mac_universales_2019_parte_4


# In[40]:


len(Direcciones_Mac_universales_2019_parte_4)


# In[32]:


len(Direcciones_Mac_universales_2019_parte_3)


# In[24]:


len(Direcciones_Mac_universales_2019_parte_2)


# In[16]:


len(Direcciones_Mac_universales_2019_parte_1)


# In[63]:


len(Direcciones_Mac_universales_2019_parte_0)


# ## Insertamos en BBDD el array de Direcciones_Mac_universales_2019

# In[41]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L"] ["Parte_4_Universales"]
Direcciones_Mac_universales_2019 = collection.Datos_Análisis_2019

Parte_4 = Direcciones_Mac_universales_2019_parte_4
Direcciones_Mac_parte_4 = {
    "Direcciones_Mac_universales_2019_parte_4": Parte_4
}
    
id = Direcciones_Mac_universales_2019.insert_one(Direcciones_Mac_parte_4).inserted_id
print(id)


# In[78]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L"] ["Parte_0_Locales.Datos_Análisis_Locales_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
array = pd.DataFrame (data) # leer la tabla completa (DataFrame)
array


# ## Insertamos en BBDD el array de Direcciones_Mac_locales_2019

# In[42]:


from pymongo import MongoClient

client = MongoClient('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L"] ["Parte_4_Locales"]
Direcciones_Mac_locales_2019 = collection.Datos_Análisis_Locales_2019

Parte_4 = Direcciones_Mac_locales_2019_parte_4
Direcciones_Mac_L_parte_4 = {
    "Direcciones_Mac_locales_2019_parte_3": Parte_4
}
    
id = Direcciones_Mac_locales_2019.insert_one(Direcciones_Mac_L_parte_4).inserted_id
print(id)


# In[79]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Direcciones_Mac_U_L"] ["Parte_0_Universales.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
array1 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
array1

