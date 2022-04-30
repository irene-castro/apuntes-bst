# EJERCICIOS PROPUESTOS DE SPARK (MÓDULO 5)

### Módulo 5.1
1. Crea un nuevo contexto SQLContext
```
var ssc = new org.apache.spark.sql.SQLContext(sc)
```
2. Importa los implicits que permiten convertir RDDs en DataFrames
```
import sqlContext.implicits._
```
3. Carga el dataset “zips.json” que se encuentra en la carpeta de ejercicios de Spark y que 
contiene datos de códigos postales de Estados Unidos. Puedes usar el comando 
“ssc.load(“ruta_fichero”, “formato”)”.
```
var zips = ssc.load("file:/home/BIT/data/zips.json", "json")
```
4. Visualiza los datos con el comando “show()”. Tienes que ver una tabla con 5 columnas 
con un subconjunto de los datos del fichero. Puedes ver que el código postal es “_id”, la 
ciudad es “city”, la ubicación “loc”, la población “pop” y el estado “state”.
```
zips.show()
```
5. Obtén las filas cuyos códigos postales cuya población es superior a 10000 usando el api 
de DataFrames.
```
val over1=zips.filter(zips("pop") > 10000).collect()
```
6. Guarda esta tabla en un fichero temporal para poder ejecutar SQL contra ella.
```
zips.registerTempTable("zips")
```
7. Realiza la misma consulta que en el punto 5, pero esta vez usando SQL.
```
ssc.sql("select * from zips where pop > 10000").collect()
```
8. Usando SQL, obtén la ciudad con más de 100 códigos postales
```
ssc.sql("select city from zips group by city having count(*)>100").show()
```
9. Usando SQL, obtén la población del estado de Wisconsin (WI).
```
ssc.sql("select SUM(pop) as POBLACION from zips where state='WI'").show()
```
10. Usando SQL, obtén los 5 estados más poblados
```
ssc.sql("select state from zips GROUP BY state ORDER BY sum(pop) DESC").take(5)
```

###  Módulo 5.2: Hive

1. Abrir una terminal y ejecutad este comando: “sudo cp /usr/lib/hive/conf/hive-site.xml 
/usr/lib/spark/conf/”. Para más información podéis consultar esta web “ 
https://community.cloudera.com/t5/Advanced-Analytics-Apache-Spark/how-to-access-the-hive-tables-from-spark-shell/td-p/36609 ”
2. Reiniciar el Shell de Spark.
3. En un terminal, arrancar el Shell de Hive y echar un vistazo a las bases de datos y tablas 
que hay en cada una de ellas.
```
hive
show databases
```
4. En otro terminal aparte, arrancar el Shell de Spark, y a través de SparkSQL crear una 
base de datos y una tabla con dos o tres columnas. Si no creamocon Spark a través de 
Hive.\
bbdd: hivespark\
tabla: empleados\
columnas: id INT, name STRING, age INT\
config table: FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'.
```
import org.apache.spark.sql.hive.HiveContext
var sscc=new.HiveContext(sc)
sscc.sql("CREATE DATABASE hivespark IF NOT EXISTS")
sscc.sql("USE hivespark")
sscc.sql("CREATE TABLE hivespark.empleados(id AS INT, name AS STRING, age AS INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'")
```
5. Crear un fichero “/home/cloudera/empleado.txt” que contenga los siguientes datos de 
esta manera dispuestos:\
1201, nombre1, 25\
1202, nombre2, 28\
1203, nombre3, 39\
1204, nombre4, 23\
1205, nombre5, 23
6. Aprovechando la estructura de la tabla que hemos creado antes, usando SparkSQL, 
subid los datos del fichero “/home/cloudera/empleado.txt” a la tabla hive, usando como 
la sintaxis de HiveQL como vimos en el curso de Hive (LOAD DATA LOCAL INPATH).
```
sscc.sql("LOAD DATA LOCAL INPATH '/home/cloudera/empleado.txt' INTO TABLE hivespark.empleados")
```
7. Ejecutad cualquier consulta en los terminales de Hive y Spark para comprobar que todo 
funciona y se devuelven los mismos datos. En el terminal de Spark usad el comando 
“show()” para mostrar los datos.
```
En terminal de Hive: SELECT pop FROM empleados
En terminal de Spark: sscc.sql("SELECT pop FROM EMPLEADOS").show()
```

### Módulo 3: DataFrames

1. Creamos un contexto SQL
```
var ssce = new org.apache.spark.sql.SQLContext(sc)
```
2. Importa los implicits que permiten convertir RDDs en DataFrames y Row.
```
import sqlContext.implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType}
```
3. Creamos una variable con la ruta al fichero 
“/home/cloudera/Desktop/DataSetPartidos.txt”. Será necesario copiar el dataset 
“DataSetPartidos.txt” al escritorio de la máquina virtual. Las líneas tienen el siguiente 
formato:\
idPartido::temporada::jornada::EquipoLocal::EquipoVisitant
e::golesLocal::golesVisitante::fecha::timestamp
```
val ruta="file:/home/cloudera/Desktop/DataSetPartidos.txt"
```
4. Leemos el contenido del archivo en una variable
```
val rutale=sq.textFile(ruta)
```
5. Creamos una variable que contenga el esquema de los datos
```
val schemaString="idPartido::temporada::jornada::EquipoLocal::EquipoVisitant
e::golesLocal::golesVisitante::fecha::timestamp"
```
6. Generamos el esquema basado en la variable que contiene el esquema de los datos 
que acabamos de crear.
```
val schema=StructType(schemaString.split("::").map(a=>StructField(a,StringType,true)))
```
7.Convertimos las filas de nuestro RDD a Rows
```
val rowrdd=rutale.map(._split("::")).map(p=>Row(p(0),p(1),p(2),p(3),p(4),p(5),p(6),p(7),p(8).trim))
```
8.Aplicamos el Schema al RDD
```
val partidosf=sqlContext.createDatFrame(rowrdd,schema)
```
9. Registramos el DataFrame como una Tabla
```
partidosdf.registerTempTable("partidos")
```
10. Ya estamos listos para hacer consultas sobre el DF con el siguiente formato:
```
ssce.sql("SELECT * FROM partidos").show()
```
11. Los resulados de las queries son DF y soportan las operaciones como los RDDs 
normales. Las columnas en el Row de resultados son accesibles por índice o nombre de 
campo.\
12. Ejercicio: ¿Cuál es el record de goles como visitante en una temporada del Oviedo?
```
ssce.sql("SELECT sum(golesVisitante) FROM partidos WHERE equipoVisitante='Real Oviedo' GROUP BY temporada ORDER BY goles DESC").take(1)
```
13. Ejercicio: ¿Quién ha estado más temporadas en 1 Division Sporting u Oviedo?
```
ssce.sql("SELECT count(distinct(temporada)) FROM partidos WHERE equipoVisitante='Real Oviedo' OR equipoLocal='Real Oviedo'").show()
ssce.sql("SELECT count(distinct(temporada)) FROM partidos WHERE equipoVisitante='Sporting de Gijon" OR equipoLocal='Sporting de Gijon'").show()
```














