# Semana 1
- 
- se usa python 3.11 porque es la mejor version para luego usar en databricks y la que usa pyspark en local.

## Fuentes tier 1
Este funte no funciona des de el 4 de junio. Se van a completar los datos de manera sintetica
VBB_GTFS_RT_URL=https://production.gtfsrt.vbb.de/data
fuente de las bicis
estos son donde estan todos los links. Dentro de este link estan los links de bicis libres y el estado de las estaciones

NEXTBIKE_GBFS_DISCOVERY_URL=https://gbfs.nextbike.net/maps/gbfs/v2/nextbike_bn/gbfs.json

VIZ_DISRUPTIONS_URL=

## lakehosue in azure
rutas lakehouse en azure. probar primero como hacerlo en unitz catalog volume porque asi no hay
que poner las credenciales. si uso adls gen2 tengo que poner la credencial. 

## lakehouse formato
las tablas del lakehouse son delta
las particiones en bronze van por la fecha de ingesta porque tendremos archivos cada dia. 
Por ejemplo la infomacion de las bicis si se actualiza cada 5/10 minutos ya da un volumen importante
De momento todas las fuentes se particionan por fecha.

## http.py va a descargar bytes no solo el json. 
en algunas fuentes no recibo json, recibo protobuf. asi descargo y luego parseo. 

## lakehouse folder
ahora se decide la ruta en config.py. asi no da error al ejecutar desde la consola
alternativa poner el lakehouse_root en .env

## gtfs_static para calcular el delay de cada viaje. Se parsea solo las estaciones cerca del recorrido no todo berlin. 

## Explorar bvg los delays ya estan ahi. 

# Semana 2

- Los datos se localizaran con latitud y longitud en una malla que divide el terreno en hexágonos, el sistema H3. 
Este sistema presenta ventajas frente a otros con cuadricula rectangular que distorsionan la resolucion dependiendo 
de la latitud o soluciones más complejas que necesitan de bases de datos especiales. En nuestro caso H3 con latitud 
longitud obtenemos una malla de lado 174m que asegura la anonimizacion de los datos. El sistema esta preparado para 
cambiar la densidad de la malla.

- silver y gold se sobreescriben cada vez, ya que el volumen de datos no es importante en este momento
- La malla de H3 sera el nivel 9 de 174m de arista. esto ayuda a anonimizar y da suficiente
resolucion para modelar donde esta la gente y las estaciones 

# en una primera version se metian directamente las coordenadas que limitaban un rectangulo
alrededor del recorrido. He mejorado el codigo para que cada año se puedan meter los puntos
de interes del recorrido y las coordenadas se calculen automaticamente