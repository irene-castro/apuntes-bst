# EJERCICIOS PROPUESTOS DE SPARK (Módulo 6)

### Spark Streaming I

1. Visita en la web la documentación de Spark 
https://spark.apache.org/docs/1.5.2/streaming-programming-guide.html y 
familiarízate con el ejercicio. El objetivo es hacer lo mismo que pone en la web en el 
apartado “A Quick Example”
2. Tomate un tiempo para navegar por la web y explorar todo lo que puede ofrecerte. 
Cuando lo consideres, comienza el ejercicio:
3. Abre un terminal nuevo y escribe el siguiente comando: “nc -lkv 4444”, que hace que 
todo lo que escribas se envíe al puerto 4444.
```
nc -lkv 4444
```
4. Inicia un nuevo terminal y arranca el Shell de Spark en modo local con al menos 2 
threads, necesarios para ejecutar este ejercicio: “spark-shell --master local[2]”
```
spark-shell --master local[2]
```
5. Por otro lado, accede al fichero “/usr/lib/spark/conf/log4j.properties”, y edítalo para 
poner el nivel de log a ERROR, de modo que en tu Shell puedas ver con claridad el 
streaming de palabras contadas devuelto por tu script.
6. Importa las clases necesarias para trabajar con Spark Streaming
```
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
```
7. Crea un SparkContext con una duración de 5 segundos
```
var ssc = new StreamingContext(sc,Seconds(5))
```
8. Crea un DStream para leer texto del puerto que pusiste en el comando “nc”, 
especificando el hostname de nuestra máquina, que es “quickstart.cloudera”
```
var mystream = ssc.socketTextStream("quickstart.cloudera",4444)
```
9. Crea un MapReduce, como vimos en los apuntes, para contar el número de palabras 
que aparecen en cada Stream
```
var palabras = mystream.flatMap(line => line.split("\\W"))
var wordcounts = palabras.map(x => (x, 1)).reduceByKey((x,y) => x+y)
```
10. Imprime por pantalla los resultados de cada batch
```
wordcounts.print()
```
11. Arranca el Streaming Context y llama a awaitTermination para esperar a que la tarea 
termine
```
ssc.start()
ssc.awaitTermination()
```
12. Una vez hayas acabado, sal del Shell y del terminal donde ejecutaste el comando “nc” 
haciendo un CNTRL+C
13. Para ejecutarlo desde un script: spark-shell --master local[2] -i prueba.scala

### Spark Streaming II

Primeramente, copiamos el script de Python en /home/cloudera/BIT/examples/streamtest.py. Leerá los contenidos de los ficheros de weblogs y simulará un streaming de ellos.

1. Abre un nuevo terminal, sitúate en la ruta “/home/BIT/examples” y ejecuta el script 
Python mencionado arriba de la siguiente manera:
```
cd /home/cloudera/BIT/examples
python streamtest.py quickstart.cloudera 4444 5 /home/cloudera/BIT/data/weblogs/*
```
2. Copia el fichero “StreamingLogs.scalaspark” situado en la carpeta de ejercicios de Spark 
en “/home/BIT/stubs/StreamingLogs.scalaspark” y familiarízate con el contenido. El 
objetivo es ejecutar en el Shell cada una de las líneas que contiene más las que vamos a 
programar para este ejercicio.
3. Abre un nuevo terminal y arranca el Shell de Spark con al menos dos threads, como en 
el caso anterior (tal y como pone en el fichero). A continuación ejecuta los imports y 
crea un nuevo StreamingContext con intervalos de un segundo.
```
spark-shell –master local[2]
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
var ssc=new StreamingContext(sc, Seconds(5))
```
4. Crea un DStream de logs cuyo host es “quickstart.cloudera” (también podéis usar 
“localhost”) y cuyo puerto es “4444” (lo mismo que hemos indicado por parámetro al 
script Python anterior)
```
val dstream=ssc.socketTextStream("quickstart.cloudera",4444)
```
5. Filtra las líneas del Stream que contengan la cadena de caracteres “KBDOC”
```
val filli=dstream.filter(x=>x.contains("KBDOC")
```
6. Para cada RDD, imprime el número de líneas que contienen la cadena de caracteres 
indicada. Para ello, puedes usar la función “foreachRDD()"
```
val num=foreachRDD(filli,1)
```
7. Guarda el resultado del filtrado en un fichero de texto en sistema de archivos local 
(créate una ruta en /home/cloudera/…)de la máquina virtual, no en hdfs y revisa el 
resultado al acabar el ejercicio.
```
Creo la ruta: /home/cloudera/resultej
val numero=ssc.saveAsTextFile("/home/cloudera/resultej")
```
8. Para arrancar el ejercicio ejecuta los comandos start() y awaitTermination()
```
ssc.start()
ssc.awaitTermination()
```


