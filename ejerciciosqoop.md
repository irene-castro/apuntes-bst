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
  
