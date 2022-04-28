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
### Exploración de fichero plano

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




