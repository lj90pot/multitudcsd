# Demo KAFKA para menciones en Streaming

En esta carpeta se expone un ejemplo del despliegue de kafka para procesar las menciones
al CSD en las redes sociales. El Objetivo es mostrar que configuracion hace falta en local
para levantar el cluster.

El Plan original del proyecto contemplaba el despliege de un cluster Kafka para procesar
las menciones al CSD en redes sociales. 

Debido a que la cuenta de Confluence expiró y a que el despliegue en azure 
hubiera necesitado el aprovisionamiendo de un eventhubs como recurso adicional. Kafka fue
descartado para usar Structured Streaming.