#!/usr/bin/env python
# coding: utf-8

# ## Análisis SeenTime

# ## Índice

# -Datos 2019
# 
# -Datos 2020
# 
# -Disponibilizar los datos de 2019 en un mismo array
# 
# -Disponibilizar los datos de 2020 en un mismo array
# 
# -Análisis gráfico
# 
#     Análisis gráfico 2019
#     Análisis gráfico 2020
#     
# -Comparativa datos 2019 vs 2020
# 

# ## Datos 2019

# In[1]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2019"] ["SeenTime.Datos_Análisis_2019"]
data = collection.find()
data = list (data) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
SeenTime_2019 = pd.DataFrame (data) # leer la tabla completa (DataFrame)
SeenTime_2019


# ## Datos 2020

# In[2]:


from pymongo import MongoClient
import pandas as pd
 
cliente = MongoClient ('localhost', 27017)
collection = cliente ["Datos_Análisis_2020"] ["SeenTime.Datos_Análisis_2020"]
data_1 = collection.find()
data_1 = list (data_1) # Al convertir a una lista, puede filtrar solo los datos que necesita según la situación. (para filtrado transversal)
SeenTime_2020 = pd.DataFrame (data_1) # leer la tabla completa (DataFrame)
SeenTime_2020


# ## Disponibilizar los datos de 2019 en un mismo array

# In[3]:


type(SeenTime_2019)


# In[4]:


type(SeenTime_2020)


# In[5]:


type(data)


# In[6]:


data_x = data[1]
data_x


# In[7]:


type(data_x)


# In[8]:


ID = data_x['SeenTime_Partes']
ID


# In[9]:


len(data)


# In[10]:


SeenTime_2019_2 = []
index = -1
for i in range(len(data)):
    index = index + 1
    #print (index)
    data_x = data[index]
    SeenTime_Partes = data_x['SeenTime_Partes']
    SeenTime_2019_2.append(SeenTime_Partes)
    
SeenTime_2019_2


# In[11]:


print(len(SeenTime_2019_2), type(SeenTime_2019_2))


# En el siguiente array eliminamos los símbolos que no necesitamos, únicamente dejamos la T pra posteriormente separar los días de las horas

# In[51]:


SeenTime_2019_3 = []

characters1 = "Z"
characters2 = ":"
characters3 = "-"


index_2 = -1

for i in range (len(SeenTime_2019_2)):
    index_2 = index_2 + 1
    New_Time = SeenTime_2019_2[index_2]
    for x in range(len(characters)):
        New_Time = New_Time.replace(characters[x],"")
        #SeenTime_2019_3.append(New_Time)
        for x in range(len(characters2)):
            New_Time = New_Time.replace(characters2[x],"")
            #SeenTime_2019_3.append(New_Time)
            for x in range(len(characters3)):
                New_Time = New_Time.replace(characters3[x],"")
                SeenTime_2019_3.append(New_Time)
        
SeenTime_2019_3


# In[52]:


len(SeenTime_2019_3)


# In[53]:


SeenTime_2019_3_2 = []
array_dias_2019 = []
array_horas_2019 = []

index_3 = -1
index_4 = -1
index_5 = -1

for i in range(len(SeenTime_2019_3)):
        index_3 = index_3 + 1 
        string = SeenTime_2019_3[index_3]
        x = string.split("T")
        SeenTime_2019_3_2.append(x)


# In[54]:


for x in range(len(SeenTime_2019_3_2)):
            index_4 = index_4 + 1
            dia = SeenTime_2019_3_2[index_4][0]
            hora = SeenTime_2019_3_2[index_4][1]
            array_dias_2019.append(dia)
            array_horas_2019.append(hora)


# In[63]:


array_dias_2019


# In[65]:


import collections
Resumen_array_dias_2019 = collections.Counter(array_dias_2019)
print (Resumen_array_dias_2019)


# In[69]:


Dict_array_dias_2019 = dict(Resumen_array_dias_2019) 
print("Dictionary is ",Dict_array_dias_2019)


# In[72]:


Repeticiones_dias_2019 = list(Dict_array_dias_2019.values())
print(Repeticiones_dias_2019)


# In[73]:


Dias_2019 = [*Dict_array_dias_2019]
print(Dias_2019)


# In[74]:


array_horas_2019


# In[68]:


import collections
Resumen_array_horas_2019 = collections.Counter(array_horas_2019)
print (Resumen_array_horas_2019)


# In[75]:


Dict_array_horas_2019 = dict(Resumen_array_horas_2019) 
print("Dictionary is ",Dict_array_horas_2019)


# In[83]:


repeticiones_horas_2019 = list(Dict_array_horas_2019.values())
print(repeticiones_horas_2019)


# In[79]:


Horas_2019 = [*Dict_array_horas_2019]
print(Horas_2019)


# In[107]:


nuevas_horas_2019 = []
index_i = -1

ini = 0 #posición inicial de la subcadena
fin = 2 #posición final de la subcadena (excluida)

for i in range (len(Horas_2019)):
    index_i = index_i + 1
    cadena = Horas_2019[index_i]
    subcadena = cadena[ini:fin]
    nuevas_horas_2019.append(subcadena)

nuevas_horas_2019


# In[109]:


import collections
Resumen_nuevas_horas_2019 = collections.Counter(nuevas_horas_2019)
print (Resumen_nuevas_horas_2019)


# In[110]:


Dict_nuevas_horas_2019 = dict(Resumen_nuevas_horas_2019) 
print("Dictionary is ",Dict_nuevas_horas_2019)


# In[111]:


repeticiones_nuevas_horas_2019 = list(Dict_nuevas_horas_2019.values())
print(repeticiones_nuevas_horas_2019)


# In[112]:


Horas_2019_2 = [*Dict_nuevas_horas_2019]
print(Horas_2019_2)


# Arrays: 
# 
#         Repeticiones_dias_2019
#         Dias_2019
#         repeticiones_nuevas_horas_2019
#         Horas_2019_2
#         

# ## Disponibilizar los datos de 2020 en un mismo array

# In[14]:


fila_0 = SeenTime_2020.iloc[0]
fila_1 = SeenTime_2020.iloc[1]
fila_2 = SeenTime_2020.iloc[2]
fila_3 = SeenTime_2020.iloc[3]
fila_4 = SeenTime_2020.iloc[4]
fila_5 = SeenTime_2020.iloc[5]
fila_6 = SeenTime_2020.iloc[6]
fila_7 = SeenTime_2020.iloc[7]
fila_8 = SeenTime_2020.iloc[8]
fila_9 = SeenTime_2020.iloc[9]
fila_10 = SeenTime_2020.iloc[10]
fila_11 = SeenTime_2020.iloc[11]



Todos_los_tiempos_2020 = (fila_0[1] + fila_1[1] + fila_2[1] + fila_3[1] + fila_4[1] + fila_5[1] + fila_6[1] + fila_7[1] +
                            fila_8[1] + fila_9[1] + fila_10[1] + fila_11[1])
Todos_los_tiempos_2020


# In[15]:


len(Todos_los_tiempos_2020)


# In[22]:


SeenTime_2020_3 = []

characters = "T"
characters2 = "Z"
characters3 = ":"
characters4 = "-"


index_2 = -1

for i in range (len(Todos_los_tiempos_2020)):
    index_2 = index_2 + 1
    New_Time = Todos_los_tiempos_2020[index_2]
    for x in range(len(characters)):
        New_Time = New_Time.replace(characters[x],"")
        #SeenTime_2019_3.append(New_Time)
        for x in range(len(characters2)):
            New_Time = New_Time.replace(characters2[x],"")
            #SeenTime_2019_3.append(New_Time)
            for x in range(len(characters3)):
                New_Time = New_Time.replace(characters3[x],"")
                #SeenTime_2019_3.append(New_Time)
                for x in range(len(characters4)):
                    New_Time = New_Time.replace(characters4[x],"")
                    SeenTime_2020_3.append(New_Time)
        
SeenTime_2020_3


# In[17]:


len(SeenTime_2020_3)


# Próximos pasos:
# 
#     1 - Separar por un lado la fecha y la hora
# 
#     2 - Contar los días repetidos para saber el pico máximo para un dia 
#         (eje x = el día, eje y = la cantidad de repeticiones sobre ese dia)
# 
#     3 - Lo mismo para las horas de los días
# 
#     4 - Luego fusionar las gráficas para ver que día y qué hora son las más frecuentadas

# In[19]:


txt = "20191231T220025"

x = txt.split("T")

print(x)


# In[23]:


SeenTime_2020_3_1 = []

characters = "Z"
characters2 = ":"
characters3 = "-"


index_2 = -1

for i in range (len(Todos_los_tiempos_2020)):
    index_2 = index_2 + 1
    New_Time = Todos_los_tiempos_2020[index_2]
    for x in range(len(characters)):
        New_Time = New_Time.replace(characters[x],"")
        #SeenTime_2019_3.append(New_Time)
        for x in range(len(characters2)):
            New_Time = New_Time.replace(characters2[x],"")
            #SeenTime_2019_3.append(New_Time)
            for x in range(len(characters3)):
                New_Time = New_Time.replace(characters3[x],"")
                SeenTime_2020_3_1.append(New_Time)
        
SeenTime_2020_3_1


# In[26]:


SeenTime_2020_3_1[1]


# In[59]:


SeenTime_2020_3_2 = []
array_dias_2020 = []
array_horas_2020 = []

index_3 = -1
index_4 = -1
index_5 = -1

for i in range(len(SeenTime_2020_3_1)):
        index_3 = index_3 + 1 
        string = SeenTime_2020_3_1[index_3]
        x = string.split("T")
        SeenTime_2020_3_2.append(x)


# In[60]:


for x in range(len(SeenTime_2020_3_2)):
            index_4 = index_4 + 1
            dia = SeenTime_2020_3_2[index_4][0]
            hora = SeenTime_2020_3_2[index_4][1]
            array_dias_2020.append(dia)
            array_horas_2020.append(hora)


# In[61]:


array_dias_2020


# In[80]:


import collections
Resumen_array_dias_2020 = collections.Counter(array_dias_2020)
print (Resumen_array_dias_2020)


# In[81]:


Dict_array_dias_2020 = dict(Resumen_array_dias_2020) 
print("Dictionary is ",Dict_array_dias_2020)


# In[82]:


repeticiones_dias_2020 = list(Dict_array_dias_2020.values())
print(repeticiones_dias_2020)


# In[84]:


dias_2020 = [*Dict_array_dias_2020]
print(dias_2020)


# In[62]:


array_horas_2020


# In[85]:


import collections
Resumen_array_horas_2020 = collections.Counter(array_horas_2020)
print (Resumen_array_horas_2020)


# In[86]:


Dict_array_horas_2020 = dict(Resumen_array_horas_2020) 
print("Dictionary is ",Dict_array_horas_2020)


# In[118]:


repeticiones_horas_2020 = list(Dict_array_horas_2020.values())
print(repeticiones_horas_2020)


# In[90]:


horas_2020 = [*Dict_array_horas_2020]
print(horas_2020)


# In[114]:


nuevas_horas_2020 = []
index_ii = -1

ini = 0 #posición inicial de la subcadena
fin = 2 #posición final de la subcadena (excluida)

for i in range (len(horas_2020)):
    index_ii = index_ii + 1
    cadena = horas_2020[index_ii]
    subcadena = cadena[ini:fin]
    nuevas_horas_2020.append(subcadena)

nuevas_horas_2020


# In[115]:


import collections
Resumen_nuevas_horas_2020 = collections.Counter(nuevas_horas_2020)
print (Resumen_nuevas_horas_2020)


# In[116]:


Dict_array_nuevas_horas_2020 = dict(Resumen_nuevas_horas_2020) 
print("Dictionary is ",Dict_array_nuevas_horas_2020)


# In[119]:


repeticiones_nuevas_horas_2020 = list(Dict_array_nuevas_horas_2020.values())
print(repeticiones_nuevas_horas_2020)


# In[120]:


horas_2020_2 = [*Dict_array_nuevas_horas_2020]
print(horas_2020_2)


# Arrays: 
# 
#     repeticiones_dias_2020
#     dias_2020
#     repeticiones_nuevas_horas_2020
#     horas_2020_2

# # Análisis gráfico

# ## Análisis datos 2019

# Análisis de los días

# In[97]:


import matplotlib.pyplot as plt
 
plt.figure(figsize=(12,12))

## Declaramos valores para el eje y, en este caso son categorias
eje_x = Dias_2019
 
## Declaramos valores para el eje x, ahora son los valores
eje_y = Repeticiones_dias_2019
 
## Creamos Gráfica y ponesmos las barras de color verde
plt.barh(eje_x, eje_y, color="green")
plt.ylabel('Días 2019')
plt.xlabel('número de días')
plt.title('Días más repetidos')
plt.show()


# Análisis de las horas

# In[113]:


import matplotlib.pyplot as plt
 
plt.figure(figsize=(12,12))

## Declaramos valores para el eje y, en este caso son categorias
eje_x = Horas_2019_2
 
## Declaramos valores para el eje x, ahora son los valores
eje_y = repeticiones_nuevas_horas_2019
 
## Creamos Gráfica y ponesmos las barras de color verde
plt.barh(eje_x, eje_y, color="green")
plt.ylabel('Horas 2019')
plt.xlabel('número de Horas')
plt.title('Horas de Afluencia')
plt.show()


# ## Análisis datos 2020

# Análisis de los días

# In[104]:


import matplotlib.pyplot as plt
 
plt.figure(figsize=(50,50))

## Declaramos valores para el eje y, en este caso son categorias
eje_x = dias_2020
 
## Declaramos valores para el eje x, ahora son los valores
eje_y = repeticiones_dias_2020
 
## Creamos Gráfica y ponesmos las barras de color verde
plt.barh(eje_x, eje_y, color="green")
plt.ylabel('Días 2020')
plt.xlabel('número de días')
plt.title('Días más repetidos')
plt.show()


# In[121]:


import matplotlib.pyplot as plt
 
plt.figure(figsize=(12,12))

## Declaramos valores para el eje y, en este caso son categorias
eje_x = horas_2020_2
 
## Declaramos valores para el eje x, ahora son los valores
eje_y = repeticiones_nuevas_horas_2020
 
## Creamos Gráfica y ponesmos las barras de color verde
plt.barh(eje_x, eje_y, color="green")
plt.ylabel('Días 2019')
plt.xlabel('número de días')
plt.title('Días más repetidos')
plt.show()
    
    


# # Comparativa datos 2019 vs 2020

# In[130]:


import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(15,15))

x1 = Horas_2019_2
y1 = repeticiones_nuevas_horas_2019

x = horas_2020_2
y = repeticiones_nuevas_horas_2020

plt.bar(x, y, align = "center")
plt.bar(x1, y1, color = "g", align = "center")
plt.legend(["Horas 2019", "Horas 2020"])
plt.show()


# In[137]:


import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(50,50))

x1 = Dias_2019
y1 = Repeticiones_dias_2019

x = dias_2020
y = repeticiones_dias_2020

plt.bar(x, y, align = "center")
plt.bar(x1, y1, color = "g", align = "center")
plt.legend(["Días 2019", "Días 2020"])
plt.show()

 


# In[ ]:




