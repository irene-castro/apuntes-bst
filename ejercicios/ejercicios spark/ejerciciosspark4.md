# EJERCICIOS PROPUESTOS SPARK: MÓDULO 4.

### Trabajo con todos los datos de la carpeta de logs: “/home/BIT/data/weblogs/*”

1. Usando MapReduce, cuenta el número de peticiones de cada usuario, es decir, las veces 
que cada usuario aparece en una línea de un log. Para ello:\
  a. Usa un Map para crear un RDD que contenga el par (ID, 1), siendo la clave el ID y 
el Value el número 1. Recordad que el campo ID es el tercer elemento de cada 
línea.\
  b. Usa un Reduce para sumar los valores correspondientes a cada userid. Los datos 
tendría que mostrarse de la siguiente manera:
```
a. val rdd=sc.textFile("file:/home/cloudera/BIT/data/weblogs/*").map(line=>line.split(" ")).map(a=>(a(2),1))
b. val red=rdd.reduceByKey((v1,v2)=>v1+v2)
```
*Nota: para ver un resultado habría que ejecutar una acción como collect(), show(), take(5)...*

2. Muestra los id de usuario y el número de accesos para los 10 usuarios con mayor número 
de accesos. Para ello:\
  a. Utiliza un map() para intercambiar la Clave por el Valor, de forma que quede algo 
así: (Si no se te ocurre cómo hacerlo, investiga la función “swap()”)\
  b. Utiliza la función vista en teoría para ordenar un RDD. Ten en cuenta que 
queremos mostrar los datos en orden descendiente (De mayor a menor número 
de peticiones). Recuerda que el RDD debe estar en la misma forma que al inicio, 
es decir, con clave: userid y valor: nº de peticiones.\
```
a. val newRdd=red.map(campo=>campo.swap())
b. val newRdd2=newRdd.sortByKey(false).map(campo=>campo.swap()).take(10).foreach(println)

```
3. Crea un RDD donde la clave sea el userid y el valor sea una lista de ips a las que el userid 
se ha conectado (es decir, agrupar las IPs por userID). Ayúdate de la función groupByKey() 
para conseguirlo.
```
val logs=sc.textAsFile("file:/home/cloudera/BIT/data/weblogs/*")
val rddus=logs.map(line=>line.split(" ")).map(x=>(x(2),x(0)).groupByKey().take(10)
```

### Trabajo con todos los datos de la carpeta de logs: “/home/BIT/data/accounts.cvs”
1. Abre el fichero accounts.cvs con el editor de texto que prefieras y estudia su contenido. 
Verás que el primer campo es el id del usuario, que corresponde con el id del usua1rio 
de los logs del servidor web. El resto de campos corresponden con fecha, nombre, 
apellido, dirección, etc.

2. Haz un JOIN entre los datos de logs del ejercicio pasado y los datos de accounts.csv, de 
manera que se obtenga un conjunto de datos en el que la clave sea el userid y como valor 
tenga la información del usuario seguido del número de visitas de cada usuario. Los pasos 
a ejecutar son:\
  a. Haz un map() de los datos de accounts.cvs de forma que la Clave sea el userid y 
el Valor sea toda la línea, incluido el userid\
  b. Haz un JOIN del RDD que acabas de crear con el que creaste en el paso anterior 
que contenía (userid, nº visitas).\
  c. Crea un RDD a partir del RDD anterior, que contenga el userid, número de visitas, 
nombre y apellido de las 5 primeras líneas.
```
val red1=sc.textFile("file:/home/cloudera/BIT/data/accounts.cvs")
a. val red2=red1.map(x=>(x(0),x))
b. val redjo=red2.join(red)
c. val redjo2=redjo.take(5)
   for(x<-redjo2){println(x_.1,x._2._2,x._2._1(3),x._2._1(4))}
``` 

### Trabajo con más métodos sobre pares RDD

1. Usa keyBy para crear un RDD con los datos de las cuentas, pero con el código postal como 
clave (noveno campo del fichero accounts.CSV). Puedes buscar información sobre este 
método en la API online de Spark.
```
var red3=red1.keyBy(word=>(word(8),word))
```

2. Crea un RDD de pares con el código postal como la clave y una lista de nombres (Apellido, 
Nombre) de ese código postal como el valor. Sus lugares son el 5º y el 4º 
respectivamente.\
  a. Si tienes tiempo, estudia la función “mapValues()” e intenta utilizarla para 
cumplir con el propósito de este ejercicio
```
var red4=red1.map(a=>(a(8),(a(4),a(3)))
a. var red5=red1.mapValues(a=>a(4)+','+a(3)).groupByKey()
```

3. Ordena los datos por código postal y luego, para los primeros 5 códigos postales, muestra 
el código y la lista de nombres cuyas cuentas están en ese código postal
```
var red6=red5.sortByKey.take(5).foreach(println)
ó
var red6=red5.sortByKey.take(5).foreach{|case (x,y)=>println("---"+x) |y.foreach(println)}
```






