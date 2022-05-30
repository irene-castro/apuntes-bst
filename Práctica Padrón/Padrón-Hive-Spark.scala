// Databricks notebook source
// MAGIC %md
// MAGIC # ¿Y si juntamos Hive y Spark?
// MAGIC 
// MAGIC Prueba a hacer los ejercicios sugeridos en la parte de Hive con el csv "Datos 
// MAGIC Padrón" (incluyendo la importación con Regex) utilizando desde Spark EXCLUSIVAMENTE 
// MAGIC sentencias spark.sql, es decir, importar los archivos desde local directamente como tablas
// MAGIC de Hive y haciendo todas las consultas sobre estas tablas sin transformarlas en ningún 
// MAGIC momento en DataFrames ni DataSets.

// COMMAND ----------

import spark.implicits._ 
import org.apache.spark.sql.functions._ 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7.1. Creación de tablas en formato texto ##
// MAGIC 
// MAGIC 7.1.1) Crear base de datos "datos_padron" 

// COMMAND ----------

val db= spark.sql("CREATE DATABASE IF NOT EXISTS datos_padron")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 7.1.2) Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los datos mediante el 
// MAGIC comando LOAD DATA LOCAL INPATH. La tabla tendrá formato texto y tendrá como delimitador de campo el caracter ';'
// MAGIC y los campos que en el documento original están encerrados en comillas dobles '"' no deben estar 
// MAGIC envueltos en estos caracteres en la tabla de Hive (es importante indicar esto 
// MAGIC utilizando el serde de OpenCSV, si no la importación de las variables que hemos 
// MAGIC indicado como numéricas fracasará ya que al estar envueltos en comillas los toma 
// MAGIC como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.
// MAGIC 
// MAGIC > _Nota: En Databricks no funciona bien el OpenCSV, así que lo hacemos de forma diferente al anterior apartado_

// COMMAND ----------

spark.sql("USE datos_padron")

spark.sql(""" CREATE TABLE IF NOT EXISTS padron_txt
USING CSV OPTIONS (
header="true",
delimiter=";",
inferSchema="true",
path="/FileStore/tables/Rango_Edades_Seccion_202205.csv")""")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 7.1.3) Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla con una sentencia CTAS.)

// COMMAND ----------

spark.sql("""CREATE TABLE padron_txt_2 AS SELECT 
 cod_distrito AS cod_distrito, 
 trim(desc_distrito) AS desc_distrito, 
 cod_dist_barrio AS cod_dist_barrio, 
 trim(desc_barrio) AS desc_barrio, 
 cod_barrio AS cod_barrio, 
 cod_dist_seccion AS cod_dist_seccion, 
 cod_seccion AS cod_seccion, 
 cod_edad_int AS cod_edad_int, 
 EspanolesHombres AS espanoleshombres, 
 EspanolesMujeres AS espanolesmujeres, 
 ExtranjerosHombres AS extranjeroshombres, 
 ExtranjerosMujeres AS extranjerosmujeres 
 FROM padron_txt;""")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 7.1.5) En este momento te habrás dado cuenta de un aspecto importante, los datos nulos de nuestras tablas vienen representados por un espacio vacío y no por un identificador de nulos comprensible para la tabla. Esto puede ser un problema para el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para esto primero comprobaremos que solo hay espacios en blanco en las variables numéricas correspondientes a las últimas 4 variables de nuestra tabla (podemos hacerlo con alguna sentencia de HiveQL) y luego aplicaremos las sentencias case when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla padron_txt.

// COMMAND ----------

spark.sql("""SELECT length(EspanolesHombres), length(EspanolesMujeres), length(ExtranjerosHombres), length(ExtranjerosMujeres) FROM padron_txt""")

spark.sql("""ALTER TABLE padron_txt RENAME TO padron_viej""")

spark.sql("""CREATE TABLE padron_txt AS SELECT cod_distrito, 
                                  desc_distrito,
                                  cod_dist_barrio,
                                  desc_barrio,
                                  cod_dist_seccion,
                                  cod_seccion,
                                  cod_edad_int, 
                                  CASE WHEN length(EspanolesHombres)=0 then 0 ELSE EspanolesHombres END AS espanoleshombres,
                                  CASE WHEN length(EspanolesMujeres)=0 then 0 ELSE EspanolesMujeres END AS espanolesmujeres,
                                  CASE WHEN length(ExtranjerosHombres)=0 then 0 ELSE ExtranjerosHombres END AS extranjeroshombres,
                                  CASE WHEN length(ExtranjerosMujeres)=0 then 0 ELSE ExtranjerosMujeres END AS extranjerosmujeres
FROM padron_viej""")

// COMMAND ----------

// MAGIC %md
// MAGIC 7.1.6) Una manera tremendamente potente de solucionar todos los problemas previos (tanto las comillas como los campos vacíos que no son catalogados como null y los espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona OpenCSV. Para ello utilizamos : ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES ('input.regex'='XXXXXXX') Donde XXXXXX representa una expresión regular que debes completar y que identifique el formato exacto con el que debemos interpretar cada una de las filas de nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método para crear de nuevo la tabla padron_txt_2

// COMMAND ----------

import org.apache.hadoop.hive.serde2.RegexSerDe


spark.sql(""" CREATE TABLE IF NOT EXISTS padron_txt_2
USING CSV OPTIONS (
header="true",
input.regex='"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"([A-Za-z]*) *"\073"(.*)
"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"(.*?)"\073"(.*?)"\073"(.*?)"',
inferSchema="true",
path="/FileStore/tables/Rango_Edades_Seccion_202205.csv")""")


// COMMAND ----------

// MAGIC %md
// MAGIC ## 7.2.  Investigamos el formato columnar parquet ##
// MAGIC 
// MAGIC 7.2.2) Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato columnar parquet) a través de la tabla padron_txt mediante un CTAS

// COMMAND ----------

val padparq= spark.sql("""CREATE TABLE IF NOT EXISTS padron_parquet STORED AS PARQUET AS SELECT * FROM padron_txt;""")


// COMMAND ----------

// MAGIC %md
// MAGIC 7.2.3) Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios innecesarios) y otras dos tablas en formato parquet (padron_parquet y padron_parquet_2, la primera con espacios y la segunda sin ellos).

// COMMAND ----------

spark.sql("""CREATE TABLE padron_parquet_2 STORED AS PARQUET AS SELECT * FROM padron_txt_2;""")

// COMMAND ----------

// MAGIC %md
// MAGIC ## 7.4. Sobre tablas particionadas ##
// MAGIC 
// MAGIC 7.4.1) Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y DESC_BARRIO cuyos datos estén en formato parquet.

// COMMAND ----------

spark.sql("""CREATE TABLE padron_particionado ( cod_distrito INT,
                                   cod_dist_barrio INT,
                                   cod_barrio INT,
                                   cod_dist_seccion INT,
                                   cod_seccion INT,
                                   cod_edad_int INT,
                                   espanoleshombres INT,
                                   espanolesmujeres INT,
                                   extranjeroshombres INT,
                                   extranjerosmujeres INT)
PARTITIONED BY (desc_distrito STRING, desc_barrio STRING)   
STORED AS parquet""")

// COMMAND ----------

// MAGIC %md
// MAGIC 7.4.2) Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién creada a partir de un select de la tabla padron_parquet_2

// COMMAND ----------

spark.sql("""
SET hive.exec.dynamic.partition=true
""")
spark.sql(""" SET hive.exec.dynamic.partition.mode=non-strict;
""")
spark.sql("""
SET hive.exec.max.dynamic.partitions=10000;
""")
spark.sql("""
SET hive.exec.max.dynamic.partitions.pernode=10000
""")

spark.sql("""FROM datos_padron.padron_parquet_2 INSERT OVERWRITE TABLE
datos_padron.padron_particionado partition (desc_distrito, desc_barrio)
SELECT 
 CAST(cod_distrito AS INT),
 CAST(cod_dist_barrio AS INT),
 CAST(cod_barrio AS INT),
 CAST(cod_dist_seccion AS INT),
 CAST(cod_seccion AS INT),
 CAST(cod_edad_int AS INT),
 CAST(espanoleshombres AS INT),
 CAST(espanolesmujeres AS INT),
 CAST(extranjeroshombres AS INT),
 CAST(extranjerosmujeres AS INT),
 desc_distrito,
 desc_barrio;""")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 7.4.4) Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_txt_2
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 7.4.5) Llevar a cabo la consulta en Hive en las tablas padron_parquet y padron_particionado. ¿Alguna conclusión?
// MAGIC 
// MAGIC > _Conclusión: En parquet le lleva 1.35 segundos y en particionado 0.73 segundos_.

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_parquet
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_particionado
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")

// COMMAND ----------

// MAGIC %md
// MAGIC 7.4.7) Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y comparar rendimientos y sacar conclusiones.
// MAGIC 
// MAGIC > _Conclusión, de más a menos rápido: padron_parquet_2, padron_particionado, padron_txt_2. Pero todos con tiempos de ejecución de menos de 1 segundo._

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       max(espanoleshombres) AS maxespanoleshombres,
       avg(extranjerosmujeres) AS mediaextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_txt_2
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       max(espanoleshombres) AS maxespanoleshombres,
       avg(extranjerosmujeres) AS mediaextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_parquet_2
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")

// COMMAND ----------

spark.sql("""SELECT count(espanoleshombres) AS numespanoleshombres,
       count(espanolesmujeres) AS numespanolesmujeres,
       count(extranjeroshombres) AS numextranjeroshombres,
       count(extranjerosmujeres) AS numextranjerosmujeres,
       max(espanoleshombres) AS maxespanoleshombres,
       avg(extranjerosmujeres) AS mediaextranjerosmujeres,
       desc_distrito,
       desc_barrio
FROM padron_particionado
WHERE desc_distrito IN ("CENTRO","LATINA","CHAMARTIN","TETUAN","VICALVARO","BARAJAS")
GROUP BY desc_distrito, desc_barrio""")
