# Lista de decisiones tomadas por semana y categoría

***

## Semana 1

### Entorno y forma de trabajar

- PyCharm es el IDE  Databricks no se usa para desarrollar.
- Solo dataframe API y SQL. 
- Funciones encadenadas. Claridad a la hora de escribir código sin clases y funciones definidas por el usuario.
- Nombres de funciones en inglés para facilitar la distribución.
- Se usa python 3.11 porque es la mejor version para luego usar en databricks y la que usa pyspark en local.
- rutas lakehouse en azure. probar primero como hacerlo en unitz catalog volume porque asi no hay que poner las credenciales. si uso adls gen2 tengo que poner la credencial. 
- las tablas del lakehouse son delta
- las particiones en bronze van por la fecha de ingesta porque tendremos archivos cada dia. Por ejemplo la infomacion de las bicis si se actualiza cada 5/10 minutos ya da un volumen importante.De momento todas las fuentes se particionan por fecha.
- prepare_windows_hadoop() con winutils.exe y hadoop.dll. Spark necesita la libreria nativa en Windows para leer y escribir Delta en local.

### Configuracion y parametros

- lakehouse folder. la ruta se decide en config.py. asi no da error al ejecutar desde la consola alternativa poner el lakehouse_root en .env
- En Databricks, LAKEHOUSE_ROOT se inyecta como variable de entorno del cluster y se lanza RuntimeError si falta
- Ruta absoluta. Spark en windows tiene problemas al coger la ruta relativa.  D:/..., no D:\
- prints para el log con nombre del modulo y mensaje. ej: f"[gbfs] {len(filas)} estaciones (informacion estatica) descargadas"

### Lakehouse

- Arquitectura Medallion Bronze, Silver, Gold en Delta Lake
- Bronze guarda el payload sin aplanar y con columnas de metadatos
- Bronze se particiona por ingest_date. no se particiona mas. de momento los datos no son suficientes.
- Modulo storage.py es el unico que escribe en lakehouse
- Bronze es append-only

### Ingesta etapa 1

- http.py va a descargar bytes no solo el json. en algunas fuentes no recibo json, recibo protobuf. asi descargo y luego parseo.
- Este fuente no funciona desde el 4 de junio. VBB_GTFS_RT_URL=https://production.gtfsrt.vbb.de/data. Datos pueden venir incompletos
- gtfs_static para calcular el delay de cada viaje. Se parsea solo las estaciones cerca del recorrido no todo berlin.
- Explorar bvg los delays ya estan ahi. Descartado
- http.py ahora sse llama http_request.py para no causar problemas
- VBB bloquea las peticiones sin User-Agent. Cabecera User-Agent de navegador
- Reintentos: 2 intentos, timeout 60 s, espera de 30 s entre intentos
- gtfs-rt envia un protobuf que se decodifica a json y no se aplana. 
- GBFS necesita de station_status y station_information para tener coordenadas. Añadido station_information.
- GTFS_static se filtra a la zona del evento. El archivo es muy grande y mucha informacion no es necesaria. 

### Otros
- Test no usan http. usan datos de ejemplo predescargados o dataframes declarados en ejecucion
- Lakehosue en azure sera unity catalog. ADLS hay que poner credenciales y el generador sintetico no funciona 

___

## Semana 2

### configuración y parámetros

- .config("spark.sql.shuffle.partitions", "4") se baja a 4 para reducir tiempo de arranque en local.
- Los puntos de interes del recorrido calculan automaticamente las coordenadas de la caja. no se cambia manual.

### Lakehouse

- Silver y Gold se reconstruyen con overwrite en cada ejecucion. el evento dura pocas horas. La evolucion incremental es linea futura, no requisito
- funciones build reciben un dataframe y devuelven un dataframe


### Ingesta etapa 1

- para viz se busca un punto representativo de la incidencia.
- viz se cogen las incidencias activas el dia del evento. 
- malla hexagonal H3 con resolucion 9. aprox 174m de arista. Asi se asegura anonimizacion. Podria ser mas exacto.
 
___

## Semana 3

### Entorno y forma de trabajar

- Databricks Jobs como orquestador en la nube. Airflow anhade otra dependencia y otro aprovisionamiento en databricks
- makefile para ejecutar/orquestar en local
- ruff para probar el estilo de código. 
- Github ejecuta ruff y pytest en cada push. CI. 
 
### configuración y parámetros
 
- el dia del CSD es el pasado para que las agregaciones funcionen se usa otro dia y se pone el dia de referencia como variable.
- config.py tendra las variables del evento. esto hace el proyecto reutilizable a otros eventos.

 
### Ingesta etapa 1
 
- gtfs_static pasa a ingestarse una vez y no sigue la cadencia de descarga. La oferta es la misma para todo el dia. Se ahorra una descarga. write_bronze_snapshot

### Procesamiento

- H3 se añade en silver
- El umbral de retraso es 60 segundos
- Contrato de datos. Los esquemas de las tablas se definen en structtype
- Gold agrega por celda H3 y franja horaria. Por minuto de momento no tiene sentido con lo que tarda en descargar. 
 
### Descartes

- etapa 3 gestor de multitudes con simulacion peatonal queda fuera. 
- con la etapa 3 el generador sintetico de posiciones de apps de citas queda fuera
- el recomendador en tiempo real con una fastapi se descarta el caer la etapa 3

___

## Semana 4

### Ingesta etapa 1
- INGESTAR_GTFS_ESTATICO = False. variable ingesta de gtfs_static. En azure se hace la ingesta a mano la primera vez

### ingesta etapa 2

- se descarta kafka como ingesta. Spark Structured Streaming. Kafka demo. 
- Spark Structured streaming. Corre identico en local y en la nube. lectura incremental, esquema explicito, checkpointing y semantica de append
- Un generador escribe en la capa landing del lakehouse y structured streaming coge las menciones de ahi. 
- El consumidor no se queda escuchando.
- Generador es sintético pero reproducible para evitar errores en el pase de Azure. Estoy seguro que los joins funcionan.
- perfil horario que simula cuando se publican mas menciones geolocalizadas. 
- las menciones se generan alrededor de los puntos de interes declarados del recorrido


### Procesamiento

- gold_mobilitz_vs_activity hace join de las dos etapas.
- en gold no hay info de menciones en celdas que tienen menos de 5 menciones. Anonimización.

### Lakehouse

- gold_pipeline_metrics. inventario de tablas del lakehouse como tabla gold.

___

## Semana 5

### Local

- webapp streamlit para explorar las tablas visualmente. Simulacion de capa de consumo

### ML

- añadido un modelo de machine learning para predecir el retraso en la proxima hora con datos de VBB
- no MLflow
- Minimo de filas para entrenar (MINIMO_FILAS_ENTRENAMIENTO = 50)

### Azure 

- DAG de 5 tareas con ingest_bronze y stream_mentions en paralelo
- Cada tarea es un python_wheel_task contra un entry point del wheel
- Se cambia a python 3.12 que es el que usa el cluster de azure

## Semana 6

### Memoria y entrega

- Kafka aparece en la memoria solo como alternativa considerada y descartada. se menciona la demo
- Excepcion: gtfs_static.py conserva los nombres de funcion en espanol