# Introducción a Kafka #

_Apache Kafka_ es una plataforma de streaming distribuida de código abierto capaz de manejar trillones 
de eventos al día. 

Inicialmente concebido como una cola de mensajes, Kafka se basa en una abstracción
de un registro de confirmación distribuido. 

Desde que fue creado y abierto por LinkedIn en 2011,
Kafka ha evolucionado rápidamente de la cola de mensajes a convertirse en una plataforma de 
streaming completa.

> ### _Apache Kafka es una plataforma de streaming distribuida. ¿Qué significa eso?_ ###

Una plataforma de streaming tiene tres capacidades clave:

- Publicar y suscribirse a flujos de registros, similar a una cola de mensajes o un sistema
de mensajería empresarial.
- Almacenar flujos de registros de forma duradera y tolerante a fallos.
- Procesar flujos de registros a medida que se producen.

> ### _¿Por qué usar Kafka?_ ###

Porque es una herramienta que nos permite desacoplar los flujos de datos y los sistemas.

Kafka se usa generalmente para dos amplias clases de aplicaciones:

- Construír flujos de datos en tiempo real para obtener datos de manera confiable entre
sistemas o aplicaciones.
- Creación de aplicaciones de streaming en tiempo real que transforman o reaccionan a los flujos
de datos.

> ### _Un poco más sobre Kafka_ ###

Creado por LinkedIn, es un proyector Open Source mantenido principalmente por Confluent.

Posee las siguientes _características_:

- Distribuido de arquitectura resistente.
- Tolerante a fallos.
- Escalabilidad horizontal:
    - Puede escalar hasta cientos de brokers.
    - Puede escalar a millones de mensajes por segundo.
- Alto rendimiento (latencia < 10 ms), considerado por tanto real time.

_Casos de uso_:

- Sistema de mensajería.
- Seguimiento de actividad.
- Recopilar métricas de muchos lugares diferentes.
- Recopilar logs de aplicación.
- Desacoplamiento de sistemas.
- Integración con Spark, Hadoop y muchas otras tecnologías Big Data frecuentemente relacionadas
con el almacenamiento o procesamiento de datos vía stream.

_Ejemplos de uso de Kafka_:

- Netlix usa Kafka para aplicar recomendaciones en tiempo real mientras estamos viendo la TV.
- UBER usa Kafka para recopilar datos de usuarios, taxi y el viaje en sí en tiempo real para
estimar la demanda y calcular el aumento de precios en tiempo real.
- LinkedIn usa Kafka para prevenir spam, recolectar interacciones de usuarios y realizar mejores
recomendaciones de conexiones en tiempo real.

> ### _Conceptos fundamentales de Kafka_ ###

Kafka se ejecuta como un clúster en uno o más servidores que pueden abarcar varios centros de 
datos. El clúster Kafka almacena flujos de registros llamadas "topics". Cada registro consta
de una clave, un valor y una marca de tiempo.

1. Topics

Son un particular flujo de datos, similar a una tabla en una base de datos (sin todas las restricciones).
Se pueden tener tantos topics como se desee, y cada uno es identificado por su nombre.

Los topics están divididos en particiones, siendo cada una de ella una secuencia ordenada e 
inmutable de registros que se agrega continuamente a un registro de confirmación estructurado.
A los registros de las particiones se les asigna un número de identificación secuencial (incrementa)
llamado _offset_ que identifica de manera única a cada registro dentro de la partición.

> El offset sólo tiene significado para una partición en específico. Por ejemplo, offset 3 en la
partición 0 no representa la misma data que el offset 3 de la partición 1.

El orden está garantizado solamente dentro de una partición y los datos son mantenidos solamente
por un período de tiempo limitado (por defecto una semana). Una vez el dato ha sido escrito en
una partición, este no puede ser modificado. 

Los topics deben tener un factor de replicación >1 (usualmente entre 2 y 3). La replicación es
crítica porque con esta Kafka garantiza l disponibilidad y durabilidad cuando los nodos
individuales fallan inevitablemente.

2. Brokers

Son múltiples servidores que componen un cluster Kafka. Cada broker está identificado por un 
ID (entero) y contiene cierto número de particiones de topics. 

Después de conectado con cualquier broker (llamado bootstrap broker), se estará conectado
al cluster en su totalidad.

Un buen número de brokers para empezar es 3, pero algunos clusters han llegado a tener 100.

En cuanto a líderes y particiones:

- En cualquier momento solamente 1 broker puede ser el líder para una partición dada.
- Solamente el líder puede recibir y servir data para una partición.
- El resto de brokers sincronizarán los datos, por lo tanto cada partición tiene un líder y 
múltiples ISR (in-sync replica).

3. Producer

Son aquellos encargados de escribir datos en los topics. 

Conocen automáticamente a cual broker y partición escribir. En caso de falla de un broker, los
productores se recuperarán automáticamente.

Pueden escoger enviar una clave con el mensaje (número, string, etc...):
- Si la clave es _null_, los datos son enviados en round bind (broker 101, luego broker 102
y así sucesivamente).
- Si una clave es enviada, entonces todos los mensajes para esa clave siempre irán en la misma
partición.
- Una clave es básicamente enviada si necesitamos ordenamiento de mensajes para un campo
específico.

4. Consumer

Son aquellos que leen datos desde un topic.




