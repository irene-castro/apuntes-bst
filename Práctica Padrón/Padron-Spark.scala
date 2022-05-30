// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC 6.1) Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema

// COMMAND ----------

import spark.implicits._ 
import org.apache.spark.sql.functions._ 

val fich= "/FileStore/tables/Rango_Edades_Seccion_202205.csv"

val padron=spark.read.format("csv").option("header","true").option("inferschema","true").option("emptyValue",0).option("delimiter",";").load(fich)

val padron_bien=padron.na.fill(value=0).withColumn("DESC_DISTRITO",trim(col("desc_distrito"))) .withColumn("DESC_BARRIO",trim(col("desc_barrio")))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 6.3) Enumera todos los barrios diferentes

// COMMAND ----------

padron_bien.select(countDistinct("desc_barrio")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 6.4) Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.

// COMMAND ----------

padron_bien.createOrReplaceTempView("tabla")
spark.sql("""SELECT count(distinct(DESC_BARRIO)) FROM tabla""").show()

// COMMAND ----------

// MAGIC %md
// MAGIC 6.5) Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".

// COMMAND ----------

val padron2=padron_bien.withColumn("longitud", length(col("DESC_DISTRITO")))

// COMMAND ----------

// MAGIC %md
// MAGIC 6.6) Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.

// COMMAND ----------

val padron3=padron2.withColumn("valor5",lit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC 6.7) Borra esta columna.

// COMMAND ----------

val padron4=padron3.drop(col("valor5"))

// COMMAND ----------

// MAGIC %md
// MAGIC 6.8) Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO

// COMMAND ----------

val padron_particionado=padron4.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))

// COMMAND ----------

// MAGIC %md
// MAGIC 6.9) Almacénalo en caché. 

// COMMAND ----------

padron_particionado.cache()

// COMMAND ----------

// MAGIC %md
// MAGIC 6.10) Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

// COMMAND ----------

padron_particionado.groupBy(col("DESC_BARRIO"),col("DESC_DISTRITO")).agg(count(col("espanoleshombres")).alias("numespanoleshombres"), count(col("espanolesmujeres")).alias("numespanolesmujeres"), count(col("extranjeroshombres")).alias("numextranjeroshombres"), count(col("extranjerosmujeres")).alias("numextranjerosmujeres")).orderBy(desc("numextranjerosmujeres"),desc("numextranjeroshombres"))show(10)


// COMMAND ----------

// MAGIC %md
// MAGIC 6.11) Elimina el registro en caché.

// COMMAND ----------

spark.catalog.clearCache()
