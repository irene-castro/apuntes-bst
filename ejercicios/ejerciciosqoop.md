# EJERCICIOS EN SQOOP

### Creación de tabla MySQL

1. Probamos la conexión con MySQL.
 
      `mysql -u root -p`
  
      `Password: cloudera`
  
 2. Vemos las bbdd que contiene.

      `show databases`
      
 3. Creamos en MySQL la base de datos que queremos importar a Hive.

      `create database pruebadb`
      
 4. Creamos en MySQL una tabla de datos que luego importaremos a Hive mediante Sqoop.

      `use pruebadb`
      `create table tabla_prueba (nombre varchar(30), edad);`
      
 5. Comprobamos que se ha creado.
 
      `show tables;`
      
 6. Importamos algunas filas.
 
      `INSERT INTO tabla_prueba VALUES ("Irene", 23);`
      
 7. Comprobamos que los datos se han insertado en la tabla.

      `SELECT* FROM tabla_prueba;`
      

### Creación de la tabla en Hive

Creamos la tabla en hive donde se importarán los datos que acabamos de crear

1. Accedemos a hive.

   `hive`
   
2. Creamos una base de datos para esta prueba y accedemos a ella.

   `CREATE DATABASE pruebahsq;`
   
   `USE pruebahsq;`
   
3. Comprobamos que está en el warehouse de hive (NOTA: Para ello debemos abrir otra terminal en la MV)

   `hadoop fs -ls /user/hive/warehouse`

4. Creamos la estructura de la table que contendrá los datos importados desde mysql con sqoop.

   `CREATE TABLE tabhsq (nombre string, edad int) ROW FORMAT DELIMITED STORED AS TEXTFILE;`

5. Comprobamos que se ha creado con éxito.

   `SHOW TABLES`
   
   
### Importamos la tabla a Sqoop

1. Dado que la “bbdd” Accumulo no está configurada, abrimos un Shell y ejecutamos los siguientes comandos para evitar warnings molestos.

   `sudo mkdir /var/lib/accumulo`
   
   `ACCUMULO_HOME= '/var/lib/accumulo'`
   
   `export ACCUMULO_HOME` 
   
2. En un Shell escribimos lo siguiente para ver que sqoop está conectado con nuestro mysql:

   `sqoop list-databases --connect jdbc:mysql://localhost --username root --password cloudera`
   
3. Ahora listamos la tabla “table_prueba” de la bbdd “pruebadb” que hemos creado en MySQL.

   `sqoop list-tables --connect jdbc:mysql://localhost/pruebadb --username root --password cloudera`
   
4. Usando los argumentos de importación hive mostrados en las slides del curso, importar la tabla creada en Mysql en la estructura creada en hive. Usar como conector (jdbc:mysql://localhost/bbddMysql) y un solo mapper.

   `sqoop import --connect jdbc:mysql://localhost/pruebadb --table tabla_prueba --username root --password cloudera -m 1 --hive-import --hive-overwrite --hive-table pruebahsq.tabhsq`
  
