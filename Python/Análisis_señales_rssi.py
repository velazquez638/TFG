#!/usr/bin/env python
# coding: utf-8

# # Análisis de los datos

# # Índice

# 1 - Nos traemos los datos de la BBDD de MongoDB
# 
#     - Datos de 2019
#     - Datos de 2020
#     
# 2 - Análisis de las señales rssi  
# 

# ### Notas

# 1- estructura de este notebook:
# 
#     - Análisis de datos de 2019
#     - Análisis de datos de 2020
#     - Comparativa entre 2019 y 2020
#     
# 2- Para el estudio de las señales rssi, crear mapas tipo mapas de calor, para ello, juntar todos los valores de las señales que contengan el mismo valor y representarlo con un área de incidencia sobre una superficie acotada, para ver su distribución en el espacio.
# 

# ### Bibliografía

# 1- https://claudiovz.github.io/scipy-lecture-notes-ES/intro/matplotlib/matplotlib.html
# 
# 2- https://pythonbros.com/grafica-de-barras-con-matplotlib/

# ## Datos de 2019

# In[2]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["Señal_rssi.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Señal_rssi = pd.DataFrame (data) # leer la tabla completa (DataFrame)
Señal_rssi


# ## Datos 2020

# In[2]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["Señal_rssi.Datos_Análisis_2020"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Señal_rssi_2020 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
Señal_rssi_2020


# ## Análisis de las señales rssi de 2019

# El primer paso que vamos a realizar va a ser juntar todas las señales rssi de la BBDD en un único array.
# Después separaremos las señales por valores iguales. Para ello usaremos la biblioteca collections.

# In[2]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["Señal_rssi.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Señal_rssi = pd.DataFrame (data) # leer la tabla completa (DataFrame)
Señal_rssi


# In[48]:


#extraemos cada uno de los objetos de la BBDD
Señal_rssi_2019_1 = Señal_rssi.iloc[0]
Señal_rssi_2019_2 = Señal_rssi.iloc[1]
Señal_rssi_2019_3 = Señal_rssi.iloc[2]
Señal_rssi_2019_4 = Señal_rssi.iloc[3]
Señal_rssi_2019_5 = Señal_rssi.iloc[4]
Señal_rssi_2019_6 = Señal_rssi.iloc[5]
Señal_rssi_2019_7 = Señal_rssi.iloc[6]
Señal_rssi_2019_8 = Señal_rssi.iloc[7]

#de cada uno de los objetos con quedamos con los datos, suprimiendo el id y el type
Señal_rssi_2019_1[1]
Señal_rssi_2019_2[1]
Señal_rssi_2019_3[1]
Señal_rssi_2019_4[1]
Señal_rssi_2019_5[1]
Señal_rssi_2019_6[1]
Señal_rssi_2019_7[1]
Señal_rssi_2019_8[1]

#creamos arrays independientes con los datos.
Señal_rssi_1 = Señal_rssi_2019_1[1]
Señal_rssi_2 = Señal_rssi_2019_2[1]
Señal_rssi_3 = Señal_rssi_2019_3[1]
Señal_rssi_4 = Señal_rssi_2019_4[1]
Señal_rssi_5 = Señal_rssi_2019_5[1]
Señal_rssi_6 = Señal_rssi_2019_6[1]
Señal_rssi_7 = Señal_rssi_2019_7[1]
Señal_rssi_8 = Señal_rssi_2019_8[1]

#print(Señal_rssi_1, Señal_rssi_2, Señal_rssi_3, Señal_rssi_4, Señal_rssi_5, Señal_rssi_6, Señal_rssi_7, Señal_rssi_8)
print(Señal_rssi_1)


# In[49]:


len(Señal_rssi_1)


# In[56]:


señales_rssi_2019 = (Señal_rssi_1 + Señal_rssi_2 + Señal_rssi_3 + Señal_rssi_4 + Señal_rssi_5 + Señal_rssi_6 + 
                     Señal_rssi_7 + Señal_rssi_8)
    
len(señales_rssi_2019)


# En el siguiente script de python sacamos los elementos que se repiten y el número de veces que se repiten

# In[55]:


import collections
Resumen_Señal_rssi_2019 = collections.Counter(señales_rssi_2019)
Resumen_Señal_rssi_2019


# Proceso automatizado:

# In[123]:


from pymongo import MongoClient
import pandas as pd
import collections

 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["Señal_rssi.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Señal_rssi = pd.DataFrame (data) # leer la tabla completa (DataFrame)


#extraemos cada uno de los objetos de la BBDD
Señal_rssi_2019_1 = Señal_rssi.iloc[0]
Señal_rssi_2019_2 = Señal_rssi.iloc[1]
Señal_rssi_2019_3 = Señal_rssi.iloc[2]
Señal_rssi_2019_4 = Señal_rssi.iloc[3]
Señal_rssi_2019_5 = Señal_rssi.iloc[4]
Señal_rssi_2019_6 = Señal_rssi.iloc[5]
Señal_rssi_2019_7 = Señal_rssi.iloc[6]
Señal_rssi_2019_8 = Señal_rssi.iloc[7]

#de cada uno de los objetos con quedamos con los datos, suprimiendo el id y el type
Señal_rssi_2019_1[1]
Señal_rssi_2019_2[1]
Señal_rssi_2019_3[1]
Señal_rssi_2019_4[1]
Señal_rssi_2019_5[1]
Señal_rssi_2019_6[1]
Señal_rssi_2019_7[1]
Señal_rssi_2019_8[1]

#creamos arrays independientes con los datos.
Señal_rssi_1 = Señal_rssi_2019_1[1]
Señal_rssi_2 = Señal_rssi_2019_2[1]
Señal_rssi_3 = Señal_rssi_2019_3[1]
Señal_rssi_4 = Señal_rssi_2019_4[1]
Señal_rssi_5 = Señal_rssi_2019_5[1]
Señal_rssi_6 = Señal_rssi_2019_6[1]
Señal_rssi_7 = Señal_rssi_2019_7[1]
Señal_rssi_8 = Señal_rssi_2019_8[1]

señales_rssi_2019 = (Señal_rssi_1 + Señal_rssi_2 + Señal_rssi_3 + Señal_rssi_4 + Señal_rssi_5 + Señal_rssi_6 + 
                     Señal_rssi_7 + Señal_rssi_8)
    
Resumen_Señal_rssi_2019 = collections.Counter(señales_rssi_2019)
Resumen_Señal_rssi_2019


# In[124]:


print(type(Resumen_Señal_rssi_2019))


# Para poder manejar los datos más cómodamente, convertimos el <class 'collections.Counter'> en un diccionario de python, para separar la intensidad de la señal del número de veces que se repite.

# In[125]:


Dict_Señal_rssi_2019=dict(Resumen_Señal_rssi_2019) 
print("Dictionary is ",Dict_Señal_rssi_2019)


# In[126]:


valores_señal_2019 = list(Dict_Señal_rssi_2019.values())
print(valores_señal_2019)


# In[127]:


Intensidad_señal_2019 = [*Dict_Señal_rssi_2019]
print(Intensidad_señal_2019)


# In[137]:


from matplotlib import pyplot as plt

plt.figure(figsize=(11,11))

## Declaramos valores para el eje x
eje_x = Intensidad_señal_2019
 
## Declaramos valores para el eje y
eje_y = valores_señal_2019
 
## Creamos Gráfica
plt.bar(eje_x, eje_y)
 
## Leyenda en el eje y
plt.ylabel('Valores de la señal rssi')
 
## Legenda en el eje x
plt.xlabel('Cantidad de señales rssi')
 
## Título de Gráfica
plt.title('Variación de las señales rssi')
 
## Mostramos Gráfica
plt.show()


# ## Análisis de las señales rssi de 2020

# In[116]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["Señal_rssi.Datos_Análisis_2020"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
Señal_rssi_2020 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
#Señal_rssi_2020
#extraemos cada uno de los objetos de la BBDD
Señal_rssi_2020_1 = Señal_rssi_2020.iloc[0]
Señal_rssi_2020_2 = Señal_rssi_2020.iloc[1]
Señal_rssi_2020_3 = Señal_rssi_2020.iloc[2]
Señal_rssi_2020_4 = Señal_rssi_2020.iloc[3]
Señal_rssi_2020_5 = Señal_rssi_2020.iloc[4]
Señal_rssi_2020_6 = Señal_rssi_2020.iloc[5]
Señal_rssi_2020_7 = Señal_rssi_2020.iloc[6]
Señal_rssi_2020_8 = Señal_rssi_2020.iloc[7]
Señal_rssi_2020_9 = Señal_rssi_2020.iloc[8]
Señal_rssi_2020_10 = Señal_rssi_2020.iloc[9]
Señal_rssi_2020_11 = Señal_rssi_2020.iloc[10]

#de cada uno de los objetos con quedamos con los datos, suprimiendo el id y el type
Señal_rssi_2020_1[1]
Señal_rssi_2020_2[1]
Señal_rssi_2020_3[1]
Señal_rssi_2020_4[1]
Señal_rssi_2020_5[1]
Señal_rssi_2020_6[1]
Señal_rssi_2020_7[1]
Señal_rssi_2020_8[1]
Señal_rssi_2020_9[1]
Señal_rssi_2020_10[1]
Señal_rssi_2020_11[1]

#creamos arrays independientes con los datos.
Señal_rssi_1 = Señal_rssi_2020_1[1]
Señal_rssi_2 = Señal_rssi_2020_2[1]
Señal_rssi_3 = Señal_rssi_2020_3[1]
Señal_rssi_4 = Señal_rssi_2020_4[1]
Señal_rssi_5 = Señal_rssi_2020_5[1]
Señal_rssi_6 = Señal_rssi_2020_6[1]
Señal_rssi_7 = Señal_rssi_2020_7[1]
Señal_rssi_8 = Señal_rssi_2020_8[1]
Señal_rssi_9 = Señal_rssi_2020_9[1]
Señal_rssi_10 = Señal_rssi_2020_10[1]
Señal_rssi_11 = Señal_rssi_2020_11[1]

señales_rssi_2020 = (Señal_rssi_1 + Señal_rssi_2 + Señal_rssi_3 + Señal_rssi_4 + Señal_rssi_5 + Señal_rssi_6 + 
                     Señal_rssi_7 + Señal_rssi_8 + Señal_rssi_9+ Señal_rssi_10 + Señal_rssi_11)
    
Resumen_Señal_rssi_2020 = collections.Counter(señales_rssi_2020)
Resumen_Señal_rssi_2020


# In[117]:


Dict_Señal_rssi_2020 = dict(Resumen_Señal_rssi_2020) 
print("Dictionary is ",Dict_Señal_rssi_2020)


# In[120]:


valores_señal_2020 = list(Dict_Señal_rssi_2020.values())
print(valores_señal_2020)


# In[121]:


Intensidad_señal_2020 = [*Dict_Señal_rssi_2020]
print(Intensidad_señal_2020)


# In[141]:


#import matplotlib.pyplot as plt
from matplotlib import pyplot as plt

plt.figure(figsize=(11,11))

## Declaramos valores para el eje x
eje_x = Intensidad_señal_2020
 
## Declaramos valores para el eje y
eje_y = valores_señal_2020
 
## Creamos Gráfica
plt.bar(eje_x, eje_y)
 
## Leyenda en el eje y
plt.ylabel('Valores de la señal rssi')
 
## Legenda en el eje x
plt.xlabel('Cantidad de señales rssi')
 
## Título de Gráfica
plt.title('Variación de las señales rssi')
 
## Mostramos Gráfica
plt.show()


# ## Comparación señales rssi 2019/2020

# In[ ]:





# In[ ]:




