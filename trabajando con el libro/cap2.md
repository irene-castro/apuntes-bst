# EJERCICIOS CAPÍTULO 2 DE LEARNING SPARK 2ND ED.

Todo el siguiente código fue implementado en Databricks.

## Ejemplos de libro.

### M&M count en Pyspark.
1. Counting and aggregating M&M's (Python version)
2. Import the necessary libraries
3. Since we are using Python, import the SparkSession and related funcionts from PySpark module.
```
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
```
4. Build a SparkSession using the SparkSession APIs.
5. If one does not exist, then create an instance. There can only be one SparkSession per JVM.
```
spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())
```
6. Get the M&M data set filename from the command-line arguments
```
mnm_file=sys.argv[1]
```
7. Read the file into a Spark DF using the CSV format by inferring the schema and specifying that the file contains a header, which provides 
column names for comma-separated fields.
```
mnm_df = (spark.read.format("csv")
          .option("header","true")
          .option("inferSchema","true")
          .csv("/FileStore/shared_uploads/irene.castro@bosonit.com/mnm_dataset.csv"))
```
8. We use the DF high-level APIs. Note that we don't use RDDs at all. Because some of Spark's functions return the same object, we can chain 
function calls.
9. Select from the DF the fiels "State", "Color" and "Count".
10. Since we want to group each state and its M&M color count, we use groupBy()
11. Aggregate counts of all colors and groupBy() State and Color.
12. orderBy() in descending order
```
count_mnm_df=(mnm_df
              .select("State","Color","Count")
              .groupBy("State","Color")
              .agg(count("Count")
              .alias("Total"))
              .orderBy("Total", ascending=False))
```
13. Show the resulting aggregations for all the states and colors; a total count of each color per state.
 Note show() is an action, which will trigger the above query to be executed.
```
count_mnm_df.show(n=60,truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))
```
While the above code aggregated and counter for all the states, what if we just want to see the data for a single state, e.g., CA?\
14. Select from all rows in the DF\
15. Filter only CA state\
16. groupBy() State and Color as we did above.\
17. Aggregate the counts for each color\
18. orderBy() in descending order\
19. Find the aggregate count for California by filtering.
```
ca_count_mnm_df=(mnm_df
                .select("State","Color","Count")
                .where(mnm_df.State=="CA")
                .groupBy("State","Color")
                .agg(count("Count")
                .alias("Total"))
                .orderBy("Total",ascending=False))
```
20. Show the resulting aggregation for California. As above, show() is an action that will trigger the execution of the entire computation.
```
ca_count_mnm_df.show(n=10, truncate=False)
```
21. Stop the SparkSession
```
spark.stop()
```

### Ejemplo 2.2: Scala
```
package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Usage: MnMcount <mnm_file_dataset>

object MnMcount {
    def main(args: Array[String]){
        val spark = SparkSession.builder.appName("MnMCount").getOrCreate()
        
        if (args.length<1){
            print("Usage: MnMcount <mnm_file_dataset>")
            sys.exit(1)
        }
// Get the M&M data set filename
      val mnmFile=args(0)

// Read the file into a Spark DataFrame
      val mnmDF=spark.read.format("csv")
                                  .option("header","true")
                                  .option("inferSchema","true")
                                  .csv("/FileStore/shared_uploads/irene.castro@bosonit.com/mnm_dataset.csv")

// Aggregate counts of all colors and groupBy() State and Color
// orderBy() in descending order
      val countMnMDF=mnmDF
                          .select("State","Color","Count")
                          .groupBy("State","Color")
                          .agg(count("Count").alias("Total"))
                          .orderBy(desc("Total"))
      
// Show the resulting aggregations for all the state and colors
      countMnMDF.show(60)
      println("Total Rows=${countMnMDF.count()}")
      println

// Find the aggregate counts for California by filtering 
      val caCountMnMDF=mnmDF
                            .select("State","Color","Count")
                            .where(col("State")==="CA")
                            .groupBy("State","Color")
                            .agg(count("Count").alias("Total"))
                            .orderBy(desc("Total"))
      
// Show the resulting aggregations for California
      caCountMnMDF.show(10)
      
// Stop the SparkSession
      spark.stop()
    }
}
```

## EJERCICIOS A MAYORES

### Ejercicio del Quijote

Descargamos El Quijote de: https://gist.github.com/jsdario/6d6c69398cb0c73111e49f1218960f79

Aplicaremos distintos comandos como count para obtener el número de líneas, sobrecargas del método show y los métodos head, first, take. Trabajaremos en Scala.

```
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._
import sqlContext.implicits._
```

1. Creo la RDD quijote
```
val quijote=sc.textFile("/FileStore/tables/el_quijote.txt")
```
2. Hacemos el count(), de forma que el resultado es de 2186 líneas.
```
quijote.count()
```
3. Ejecutamos un show(), que se usa para mostrar los contenidos de un DF en una tabla de formato fila-columna.
```
val quijotedf=spark.read.text("/FileStore/tables/el_quijote.txt")
quijotedf.show()
```
Como podemos observar, por defecto solo se muestran las 20 primeras filas y los valores de columna se cortan en los 20 caracteres.

4. Veamos otras opciones:
```
quijotedf.show(10,false) 
```
Así mostramos todo el contenido de la columna y 10 filas.
```
quijotedf.show(10,25,true)
```
Vemos las 10 primeras líneas con el número de línea (record) correspondiente, y mostramos solo los 25 primeros caracteres de la columna por comodidad.

Cogemos las primeras 10 líneas y las imprimimos.
```
quijotedf.take(10).foreach(println)
```
5. Vemos el encabezado:
```
quijotedf.head()
```
6. Mostramos la primera línea:
```
quijotedf.first()
```

### Continuación de M&M (PySpark)

Se trata de aplicar al M&M otras operaciones.

```
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import col

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
```

```
spark = (SparkSession.builder.appName("PythonMnMCount").getOrCreate())
mnm_file=sys.argv[1]
# Leemos como un dataframe
mnm_df2 = (spark.read.format("csv")
                                  .option("header","true")
                                  .option("inferSchema","true")
                                  .csv("/FileStore/shared_uploads/irene.castro@bosonit.com/mnm_dataset.csv"))
```
1. Otras operaciones agg:
```
count_mnm_df=(mnm_df2
                    .select("State","Color","Count")
                    .groupBy("State","Color")
                    .agg({'Count': 'sum'})
                    .orderBy("sum(Count)",ascending=True))
count_mnm_df.show(n=60,truncate=False)

count_mnm_df=(mnm_df2
                    .select("State","Color","Count")
                    .groupBy("State","Color")
                    .agg({'Count': 'avg'})
                    .orderBy("State",ascending=True))
count_mnm_df.show(n=60,truncate=False)
```
2. Filtrar por varios estados:
```
filmnm_df=(mnm_df2
           .select("State","Color","Count")
           .where((mnm_df2.State == "CA") | (mnm_df2.State == "CO"))
           .groupBy("State","Color").agg(count("Count").alias("Total"))
           .orderBy("State"))

filmnm_df.show()
```

