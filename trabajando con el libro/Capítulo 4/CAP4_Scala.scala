// Databricks notebook source
// MAGIC %md
// MAGIC En este script resolveremos los ejemplos dados del capítulo 4 en Scala.
// MAGIC 
// MAGIC #1. Ejemplos de query básica#

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession
  .builder
  .appName("SparkSQLExampleApp")
  .getOrCreate()

// COMMAND ----------

val csvFile="/FileStore/tables/departuredelays.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una vista temporal:

// COMMAND ----------

val df = spark.read.format("csv")
  .option("inferSchema","true")
  .option("header","true")
  .load(csvFile)

df.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC Encontremos primeramente todos los vuelos cuya distancia sea mayor a 1000 millas.

// COMMAND ----------

spark.sql("SELECT distance,origin,destination FROM us_delay_flights_tbl WHERE distance>1000 ORDER BY distance DESC").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC A la vista de los resultados, todos los vuelos más largos fueron entre Honolulu y Nueva York. Ahora, encontraremos todos los vuelos entre San Francisco (SFO) y Chicago (ORD) con al menos dos horas de retraso:

// COMMAND ----------

spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE delay>120 AND origin='SFO' AND destination='ORD' ORDER BY delay DESC").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Podemos convertir la columna date en un formato legible y averiguar qué días o meses fueron más comunes los retrasos (pero lo haremos más tarde).
// MAGIC Ahora, queremos etiquetar todos los vuelos de US, sin importar el origen y destino, con una indicación de los retrasos que experimentaron: Retrasos muy largos (>6h), Retrasos largos (2-6h), etc. Todo esto lo añadiremos a una nueva columna Retrasos_Vuelos.

// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
               CASE
                  WHEN delay > 360 THEN "Retraso muy largo"
                  WHEN delay > 120 AND delay < 360 THEN "Retraso largo"
                  WHEN delay > 60 AND delay < 120 THEN "Pequeño retraso"
                  WHEN delay > 0 AND delay < 60 THEN "Retraso tolerable"
                  WHEN delay = 0 THEN "Sin retrasos"
                  ELSE "Temprano"
               END AS Retrasos_Vuelos
               FROM us_delay_flights_tbl
               ORDER BY origin, delay DESC""").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Tratemos de convertir las queries de SQL para usar DataFrame API.

// COMMAND ----------

df.select("date", "delay", "origin", "destination").where("delay>120").where("origin='SFO'").where("destination='ORD'").orderBy("delay").show(10)
/*
df.select(col("*"), when(("delay" > 360), "Retraso muy largo")\
          .when(("delay > 120") && ("delay < 360"), "Retraso largo")\
          .when(("delay > 60") && ("delay < 120"), "Pequeño retraso") \
          .when(("delay > 0") && ("delay < 60"), "Retraso tolerable") \
          .when("delay = 0", "Sin retrasos") \
          .otherwise("Temprano")
          .alias("Retrasos_Vuelos"))
.orderBy("origin","delay").show(10)
*/

// COMMAND ----------

// MAGIC %md
// MAGIC #2. Creando tablas y bases de datos SQL#

// COMMAND ----------

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creando una tabla managed ###

// COMMAND ----------

spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)")

/* Usando el DataFrame API:
val csv_file="ruta_archivo"
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
val flights_df=spark.read.csv(csv_file,schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creando una tabla unmanaged###

// COMMAND ----------

spark.sql("CREATE TABLE us_delay_flights(date STRING, delay INT, distance INT, origin STRING, destination STRING) USING csv OPTIONS (PATH '/FileStore/tables/departuredelays.csv')")

/* Con el DataFrame API:
(flights_df.write.option("/FileStore/tables/departuredelays.csv","/tmp/data/us_flights_delay").saveAsTable("us_delay_flights_tbl"))

// COMMAND ----------

// MAGIC %md
// MAGIC # 3. Creando vistas#

// COMMAND ----------

// MAGIC %md
// MAGIC Las vistas pueden ser globales (visibles a través de todas las SparkSession de un cluster dado) o que puedan ser vistas en una sóla sesión, y son temporales: desaparecen después de terminar la aplicación de Spark.
// MAGIC 
// MAGIC La diferencia entre crear una tabla o crear una vista, es que una vista no mantiene los datos y desaparece, pero la tabla sí persiste.
// MAGIC 
// MAGIC Veamos a continuación como crear una vista de una tabla existente usando SQL.

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin='SFO';
// MAGIC 
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_JFK_global_tmp_view AS SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin='JFK'
// MAGIC 
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC Utilizando DF API:

// COMMAND ----------

val df_sfo=spark.sql("SELECT date, origin, destination FROM us_delay_flights_tbl WHERE origin='SFO'")
val df_jfk=spark.sql("SELECT date, origin, destination FROM us_delay_flights_tbl WHERE origin='JFK'")

// Creamos una vista temporal y global

df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

// Accedemos
spark.read.table("us_origin_airport_JFK_tmp_view")
spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(10)

// Dropeamos la vista como lo haríamos con una tabla:
spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC En el caso de SQL, para dropear una view:
// MAGIC ```
// MAGIC DROP VIEW IF EXISTS us_origin_aiport_SFO_global_tmp_view
// MAGIC ```

// COMMAND ----------

// MAGIC %md
// MAGIC # 4. Viewing the Metadata #
// MAGIC 
// MAGIC En una aplicación Spark podemos, por ejemplo, tras crear una variable SparkSession, acceder a todo el metadata almacenado por métodos como el siguiente:

// COMMAND ----------

spark.catalog.listDatabases();
spark.catalog.listTables();
spark.catalog.listColumns("us_delay_flights_tbl");

// COMMAND ----------

// MAGIC %md
// MAGIC # 5. Leyendo tablas en DataFrames #
// MAGIC 
// MAGIC Asumamos que tenemos una base de datos, learn_spark_db, y una tabla, us_delay_flights_tbl, listos para usar. En lugar de leer de un archivo JSON externo, podemos usar una consulta SQL para consultar la tabla y asignar el resultado devuelto a un dataframe:

// COMMAND ----------

val usFlightsDF=spark.sql("SELECT * FROM us_delay_flights_tbl")
val usFlightsDF2=spark.table("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # 6. Data Sources para DataFrames y tablas SQL #
// MAGIC 
// MAGIC Primeramente, echaremos un vistazo a dos constructos Data Source API de alto nivel que dictan la manera en la cual interaccionamos con los diferentes data sources: DataFrameReader y DataFrameWriter.
// MAGIC 
// MAGIC ### DataFrameReader ###
// MAGIC 
// MAGIC Es el núcleo de construcción para leer data de un data source en un DataFrame. Tiene un formato definido y un patrón recomendado de uso:
// MAGIC 
// MAGIC ```
// MAGIC DataFrameReader.format(args).option("key","value").schema(args).load()
// MAGIC ```
// MAGIC Veamos algunos ejemplos:

// COMMAND ----------

// Usando Parquet
val file="/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz.parquet"
val df=spark.read.format("parquet").load(file)
// Podemos omitir el formato "parquet" pues es el que está por defecto.

// Usando CSV
val df3=spark.read.format("csv")
  .option("inferSchema","true")
  .option("header","true")
  .option("mode","PERMISSIVE")
  .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")

// Usando JSON
val df4=spark.read.format("json")
  .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

// COMMAND ----------

// MAGIC %md
// MAGIC ### DataFrameWriter ###
// MAGIC 
// MAGIC Guarda o escribe data a un built-in data source específico. Tiene unos cuantos patrones de uso recomendados:
// MAGIC ```
// MAGIC DataFrameWriter.format(args).option(args).bucketBy(args).partitionBy(args).save(path)
// MAGIC 
// MAGIC DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
// MAGIC ```
// MAGIC 
// MAGIC También se puede usar:
// MAGIC ```
// MAGIC DataFrame.write
// MAGIC // ó
// MAGIC DataFrame.writeStream
// MAGIC ```
// MAGIC 
// MAGIC Veamos un pequeño ejemplo de como usar algunos argumetos y métodos:

// COMMAND ----------

/*
val location=...
df.write.format("json").mode("overwrite").save(location)
*/

// COMMAND ----------

// MAGIC %md
// MAGIC ### Parquet ###
// MAGIC 
// MAGIC Es el data source de Spark por defecto. Por su eficacia y sus optimizaciones, se recomienda que tras transformar y limpiar nuestro data, guardemos los DataFrames en formato Parquet para el consumo posterior.
// MAGIC 
// MAGIC #### Leyendo archivos Parquet en un DataFrame ####
// MAGIC 
// MAGIC Para leer archivos Parquet en un DataFrame, tenemos que especificar el formato y la ruta, y a menos que estemos leyendo de un streaming data source no tenemos la necesidad de aportar el esquema.

// COMMAND ----------

val df=spark.read.format("parquet").load(file)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Leyendo archivos Parquet a una tabla Spark SQL ####
// MAGIC 
// MAGIC Usando directamente SQL podemos crear una tabla o vista Spark SQL unmanaged, y una vez creada podemos leer la data en un DataFrame usando SQL como vimos anteriormente.

// COMMAND ----------

/* En SQL:
CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
  USING parquet
  OPTIONS (
    path "ruta")
    
spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Escribiendo DataFrames a archivos Parquet ####

// COMMAND ----------

df.write.format("parquet").mode("overwrite").option("compression","snappy").save("/tmp/data/parquet/df_parquet")
// Creará un set de archivos Parquet compactos y comprimidos en la ruta especificada.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Escribiendo DataFrames en tablas Spark SQL ####

// COMMAND ----------

df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")

// COMMAND ----------

// MAGIC %md
// MAGIC ### JSON ###
// MAGIC 
// MAGIC JSON: JavaScript Object Notation.
// MAGIC 
// MAGIC #### Leyendo un archivo JSON a un Dataframe ####

// COMMAND ----------

val file="/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df4=spark.read.format("json")
  .load(file)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Leyendo un archivo JSON en una tabla Spark SQL y escribiendo DataFrames a archivos JSON ####
// MAGIC Se hace igual que en el caso de Parquet.
// MAGIC 
// MAGIC ### CSV ###
// MAGIC 
// MAGIC #### Leyendo un archivo CSV a un DataFrame ####

// COMMAND ----------

val file="/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val schema="DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df=spark.read.format("csv")
  .schema(schema)
  .option("header","true")
  .option("mode","FAILFAST") // Sale si hay muchos errores
  .option("nullValue","")
  .load(file)
// En Python, tendríamos que poner antes del schema la opción del header.

// COMMAND ----------

// MAGIC %md
// MAGIC ### Avro ###
// MAGIC 
// MAGIC #### Leyendo un archivo AVRO en un DataFrame ####
// MAGIC Veamos un ejemplo y omitimos el resto pues es similar al resto de apartados en otros formatos. De la misma manera también para el formato ORC.

// COMMAND ----------

val df=spark.read.format("avro").load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
df.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Imágenes ###
// MAGIC 
// MAGIC #### Leyendo un archivo imagen a un DataFrame ####

// COMMAND ----------

import org.apache.spark.ml.source.image

val imageDir="/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val imagesDF=spark.read.format("image").load(imageDir)

imagesDF.printSchema

imagesDF.select("image.height","image.width","image.nChannels","image.mode","label").show(5,false)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Archivos binarios ###
// MAGIC 
// MAGIC El DataFrameReader convierte cada archivo binario en una única fila de DataFrame que contiene el contenido raw y metadata del archivo. El data source del archivo binario produce un DataFrame con las siguientes columnas:
// MAGIC - path: StringType
// MAGIC - modificationTime: TimestampType
// MAGIC - length: LongType
// MAGIC - content: BinaryType
// MAGIC 
// MAGIC #### Leyendo un archivo binario a un DataFrame ####
// MAGIC 
// MAGIC Visualicemos con un ejemplo:

// COMMAND ----------

val path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val binaryFilesDF=spark.read.format("binaryFile")
  .option("pathGlobFilter","*.jpg")
  .load(path)

binaryFilesDF.show(5)

// Para ignorar el partitioning data discovery en un directorio, podemos fijar recursiveFileLookUp a "true" (con .option). Así desaparece la columna label.
