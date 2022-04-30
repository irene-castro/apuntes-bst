# EJERCICIOS PROPUESTOS DE SPARK
## Módulo 1: Utilizando el Shell de Spark

El propósito de este ejercicio es trabajar con el Shell de Spark en Scala para leer un fichero en 
un RDD.

Tareas a realizar:

1. Arrancar el Shell de Spark para scala y familiarizarse con la información que aparece por 
pantalla (Infos, Warnings, versión de Scala y Spark, etc…). Tarda un poco en arrancar.
```
spark-shell
```
2. Comprobar que se ha creado un contexto “sc” tal y como vimos en la documentación:
```
sc
```
*Nota: La salida es del tipo-> res0:org.apache.spark.SparkContext= org.apache.spark.SparkContext@”alfanum”*

3. Usando el comando de autocompletado sobre el SparkContext, podéis ver los métodos 
disponibles. La función de autocompletado consiste en presionar el tabulador después 
de escribir el objeto SparkContext seguido de un punto.
```
Spark.Context().
```
4. Para salir del Shell, se puede escribir “exit” o presionar Cntrl+C

## Módulo 2: Comenzando con los RDDs
### Exploración de fichero plano 1

1. Inicia el Shell de Spark si te saliste en el paso anterior.
```
spark-shell
```

2. Para este ejercicio vamos a trabajar con datos en local
*Nota: Para acceder al fichero en local, se coloca delante de la ruta la palabra “file:”*

3. Crea una carpeta llamada BIT en “/home” de forma que se cree la ruta “/home/BIT” y 
copia dentro de ella todos los ficheros de datos necesarios para el curso. Copiar la carpeta data_spark a la máquina virtual como en otras ocasiones y 
familiarizaos con su contenido.
```
En mi caso, yo creé /home/cloudera/BIT porque me daba error intentando hacerlo directamente en /home. Es por ello que trabajé en ese directorio
a partir de aquí.
```
4. Dentro de data_spark se encuentra el fichero “relato.txt”. Copia este fichero en la 
máquina virtual en la siguiente ruta: “/home/BIT/data/relato.txtexit”.

5. Visualiza el fichero con un editor de texto como “gedit” o “vi” o a través de la Shell con 
el comando “cat”.
```
Abrimos una nueva terminal e introducimos:
cat /home/cloudera/BIT/data/relato.txtexit/relato.txt
```
6. Crea un RDD llamado “relato” que contenga el contenido del fichero utilizando el 
método “textFile”.
```
val relato=sc.textFile("file:/home/BIT/data/relato.txtexit/relato.txt")
```

7. Una vez hecho esto, observa que aún no se ha creado el RDD. Esto ocurrirá cuando 
ejecutemos una acción sobre el RDD.

8. Cuenta el número de líneas del RDD y observa el resultado. Si el resultado es 23 es 
correcto.
```
relato.count()
```
9. Ejecuta el método “collect()” sobre el RDD y observa el resultado. Recuerda lo que 
comentamos durante el curso sobre cuándo es recomendable el uso de este método.
```
relato.collect()
```

10. Observa el resto de métodos aplicables sobre el RDD como vimos en el ejercicio anterior.
```
relato.countByValue()
relato.take(2)
```

11. Si tienes tiempo, investiga cómo usar la función “foreach” para visualizar el contenido 
del RDD de una forma más cómoda de entender.
```
relato.foreach(println)
```

### Exploración de fichero plano 2

1. Copia la carpeta weblogs contenida en la carpeta de ejercicios de Spark a 
“/home/BIT/data/weblogs/” y revisa su contenido.
2. Escoge uno de los ficheros, ábrelo, y estudia cómo está estructurada cada una de sus 
líneas (datos que contiene, separadores (espacio), etc).
```
Tiene la pinta siguiente:
- 128 [15/Sep/2013:23:59:53 +0100] "GET /KBDOC-00031.html HTTP/1.0" 
200 1388 "http://www.loudacre.com" "Loudacre CSR 116.180.70.237
Browser"
donde 116.180.70.237 es la IP, 128 el número de usuario y GET /KBDOC-00031.html HTTP/1.0
el artículo sobre el que recae la acción.
```
4. Crea una variable que contenga la ruta del fichero, por ejemplo 
file:/home/BIT/data/weblogs/2013-09-15.log.
```
val ruta="file:/home/cloudera/BIT/data/weblogs/2013-09-15"
```
5. Crea un RDD con el contenido del fichero llamada logs
```
val logs= sc.textFile(log)
```
6. Crea un nuevo RDD, jpglogs, que contenga solo las líneas del RDD que contienen la 
cadena de caracteres “.jpg”
```
val jpglogs=logs.filter(x=>x.contains(".jpg"))
```
7. Imprime en pantalla las 5 primeras líneas de jpglogs
```
jpglogs.take(5)
```
8. Es posible anidar varios métodos en la misma línea. Crea una variable jpglogs2 que 
devuelva el número de líneas que contienen la cadena de caracteres “.jpg”.
```
val jpglogs2=logs.filter(x=>x.contains(".jpg")).count()
```
9. Ahora vamos a comenzar a usar una de las funciones más importantes de Spark, la 
función “map()”. Para ello, coge el RDD logs y calcula la longitud de las 5 primeras líneas. 
Puedes usar la función “size()” o “length()” Recordad que la función map ejecuta una 
función sobre cada línea del RDD, no sobre el conjunto total del RDD.
```
logs.map(x=>x.length).take(5)
```
10. Imprime por pantalla cada una de las palabras que contiene cada una de las 5 primeras 
líneas del RDD logs. Puedes usar la función “split()”.
```
sc.textFile(logs).filter(line=> line.contains(".jpg")).count()
```
11. Mapea el contenido de logs a un RDD “logwords” de arrays de palabras de cada línea.
```
var logwords = logs.map(line => line.split(' '))
```
12. Crea un nuevo RDD llamado “ips” a partir del RDD logs que contenga solamente las ips 
de cada línea (primer elemento de cada fila)
```
var ips = logs.map(line => line.split(' ')(0))
```
*Nota: cuando un elemento está en la columna n tendremos que escribir n-1 en los comandos para referirnos a ella.*

13. Imprime por pantalla las 5 primeras líneas de ips
```
ips.take(5)
```
14. Visualiza el contenido de ips con la función “collect()”. Verás que no es demasiado 
intuitivo. Prueba a usar el comando “foreach”.
```
ips.collect()
ips.foreach(println)
```
15. Crea un bucle “for” para visualizar el contenido de las 10 primeras líneas de ips.
```
for(x<-ips.take(10){print(x)}
```
16. Guarda el resultado del bucle anterior en un fichero de texto usando el método 
saveAsTextFile en la ruta “/home/cloudera/iplist” y observa su contenido.
```
ips.saveAsTextFile("file:/home/cloudera/iplist")
```

### Exploración conjunto de ficheros planos en una carpeta

1. Crea un RDD que contenga solo las ips de todos los documentos contenidos en la ruta 
“/home/BIT/data/weblogs”. Guarda su contenido en la ruta “/home/cloudera/iplistw” 
y observa su contenido.
```
val ips2=sc.textFile("file:/home/cloudera/BIT/data/weblogs").map(linea=>linea.split(" ")(0))
```
2. A partir del RDD logs, crea un RDD llamado “htmllogs” que contenga solo la ip seguida 
de cada ID de usuario de cada fichero html. El ID de usuario es el tercer campo de cada 
línea de cada log. Después imprime las 5 primeras líneas.
```
val htmllogs=logs.map(linea=>linea.split(" ")(0)).map(linea=>linea.split(" ")(2))
htmllogs.take(5)
```

