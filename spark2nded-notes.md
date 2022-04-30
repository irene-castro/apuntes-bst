### Instalación de Java, Anaconda y Spark

El proceso se sigue del Manual Apache Spark Beta.

#### Java JDK

En primer lugar, descargamos Java JDK, concretamente jdk-15.0.1. A continuación, creamos la ruta C:/java y descomprimimos la carpeta descargada ahí,
de modo que el contenido estará en la ruta C:/java/jdk-15.0.1
Después crearemos una variable de entorno llamada JAVA_HOME, que nos ayudará acceder a su ruta de una forma más sencilla. Para ello:

- Accedemos a Editar las variables de entorno del sistema. \
- Accedemos a Variables de entorno... Y creamos una nueva en Variables del Sistema, en donde su nombre de variable será JAVA_HOME y su valor C:\java\jdk-15.0.1
- Luego accedemos a Path y lo editamos, añadiendo C:\java\jdk-15.0.1\bin

Para comprobar que todo está bien, escribimos cmd en el Inicio y abrimos una terminal en modo administrador. En ella, escribimos
```
java -version
```
#### Anaconda (*+Python*)

Empezamos por acceder a https://www.anaconda.com, y seleccionamos la versión compatible con nuestro sistema Windows 10 de 64 bits. Una vez descargado,
asumimos toda la configuración por defecto.

Una vez instalado, en Inicio escribimos *prompt* y abrimos Anaconda en modo administrador. Para asegurarnos de que Python (versión 3.9.7) se instaló correctamente, escribimos:
```
python --version
```

#### Apache Spark

Accedemos a https:/spark.apache.org/ y descargamos la versión 3.0.3 (Jun 23 2021) y Pre-Built for Apache Hadoop 2.7.

Creamos la ruta C:/Spark y descomprimimos la descarga en ella. Ahora, en el GitHub https://github.com/steveloughran/winutils descargamos la carpeta hadoop-2.7.1
y la llevaremos a la ruta C:/Hadoop.

Creamos dos variables de entorno nuevas, SPARK_HOME y HADOOP_HOME, con los path a sus bin. 

#### Scala y Python desde la consola

En la consola de Anaconda, 
```
- pyspark 
- spark-shell
- pip install pyspark
```

También añadimos las variables de entorno (esta vez de usuario) PYSPARK_DRIVER_PYTHON (val: jupyter), PYSPARK_DRIVER_PYTHON_OPTS (val:notebook) y haciendo pyspark en Anaconda nos abrirá un notebook de jupyter.

#### Scala desde Jupyter Notebook

Abrimos la consola de Anaconda y ejecutamos *pip install spylon-kernel*.

