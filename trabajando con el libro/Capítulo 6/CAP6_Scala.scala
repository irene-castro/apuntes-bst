// Databricks notebook source
// MAGIC %md
// MAGIC # 1. Scala Case Classes and JavaBeans for Datasets #
// MAGIC 
// MAGIC Para crear Dataset[T] donde T es nuestro objeto tipado en Scala, necesitamos una case class que defina el objeto. Usamos el ejemplo del capítulo 3 donde tenemos el archivo JSON *bloggers*.
// MAGIC 
// MAGIC 
// MAGIC Para crear el dataset[bloggers] distribuido, deberemos primero definir la case class scala que defina cada campo individual que contiene el objeto scala. Esta case class sirve como blueprint o schema para el objeto tipado bloggers.

// COMMAND ----------

case class Bloggers(id:BigInt, first:String, last:String, url:String, published:String,
hits: BigInt, campaigns:Array[String])

// COMMAND ----------

val bloggers = "/FileStore/tables/blogs.json"
val bloggersDS = spark
 .read
 .format("json")
 .option("path", bloggers)
 .load()
 .as[Bloggers]

// COMMAND ----------

// MAGIC %md
// MAGIC # Creating Sample Data #

// COMMAND ----------

import scala.util.Random._

// Nuestra clase para el dataset

case class Usage(uid:Int, uname:String, usage: Int)

// Random de 42 caracteres (creo)
val r = new scala.util.Random(42)

// Creamos 1000 casos de la clase scala Usage

val data = for (i <- 0 to 1000)
 yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
 r.nextInt(1000)))

//  Creamos el dataset de dato tipado de Usage
val dsUsage = spark.createDataset(data)
dsUsage.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC # Transforming Sample Data #

// COMMAND ----------

import org.apache.spark.sql.functions._
dsUsage
 .filter(d => d.usage > 900)
 .orderBy(desc("usage"))
 .show(5, false)

// O podemos usar una función definida por nosotros.

def filterWithUsage(u: Usage) = u.usage > 900
dsUsage.filter(filterWithUsage(_)).orderBy(desc("usage")).show(5)

// COMMAND ----------

//Usamos una if-then-else expresión lambda y computamos un valor.
dsUsage.map(u => {if (u.usage > 750) u.usage * .15 else u.usage * .50 })
 .show(5, false)

// Defino una función para computar usage
def computeCostUsage(usage: Int): Double = {
 if (usage > 750) usage * 0.15 else usage * 0.50
}
// Uso la función como un argumento de map.
dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)


// COMMAND ----------

// Creamos una nueva case class con un campo adicional, cost.

case class UsageCost(uid: Int, uname:String, usage: Int, cost: Double)

// Computamos el usage cost con Usage como parámetro y devolviendo un nuevo objeto, UsageCost

def computeUserCostUsage(u: Usage): UsageCost = {
 val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50
 UsageCost(u.uid, u.uname, u.usage, v)
}

// Usamos map() a nuestro DataSet original.
dsUsage.map(u => {computeUserCostUsage(u)}).show(5)
