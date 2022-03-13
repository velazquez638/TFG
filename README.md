# TFG
El objetivo de este trabajo es el poder realizar un análisis predictivo de datos.

Para ello analizaremos los datos recogidos por un sensor colocado en un punto de la Gran Vía de Madrid.

Este trabajo se divide en 3 partes:
## Primera parte (Limpieza y cargado de datos en BBDD):
1 -> Cargado de archivos json en BBDD (MongoDB) tenemos un total de 216 json de datos de 2019 y un total de 237 archivos json de 2020.
2 -> Limpieza de datos, cada archivo json contiene datos en orden de millones, de los cuales no todos los datos son útiles, nos quedaremos con los siguientes datos:
     - Direcciones Mac
     - señales rssi
     - Seen Epoch
     - Seen Time
3 -> Cargado de los datos útiles en BBDD
              
## Segunda parte (Análisis):
Para ello vamos a realizar un análisis de tódos los datos útiles, sacaremos conclusiones en espacio y tiempo de los datos así como datos estadísticos de toda la 
información útil recopilada.
    
## Tercera parte (Redes Neuronales):
Una vez realizado el estudio estadístico de los datos, crearemos un modelo predictivo basado en redes neuronales.
