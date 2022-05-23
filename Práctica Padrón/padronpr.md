# Práctica con Hive-Impala-HDFS-Spark #

A partir de los datos (CSV) de Padrón de Madrid llevamos acabo los siguientes ejercicios.

## _Hive_ ##

_Nota: Para abrir Hive, iniciamos la máquina virtual de Cloudera. Una vez hecho, abrimos Hue en el navegador, en el cual ya 
podremos iniciar la consulta Hive que queramos._

## 1. Creación de tablas en formato texto ##

1.1) Crear base de datos "datos_padron" 
```
CREATE DATABASE datos_padron
```

1.2) Crear la tabla de datos padron_txt con todos los campos del fichero CSV y cargar los datos mediante el 
comando LOAD DATA LOCAL INPATH. La tabla tendrá formato texto y tendrá como delimitador de campo el caracter ';'
y los campos que en el documento original están encerrados en comillas dobles '"' no deben estar 
envueltos en estos caracteres en la tabla de Hive (es importante indicar esto 
utilizando el serde de OpenCSV, si no la importación de las variables que hemos 
indicado como numéricas fracasará ya que al estar envueltos en comillas los toma 
como strings) y se deberá omitir la cabecera del fichero de datos al crear la tabla.
```
CREATE EXTERNAL TABLE IF NOT EXISTS padron_txt (
 cod_distrito INT,
 desc_distrito STRING,
 cod_dist_barrio INT,
 desc_barrio STRING,
 cod_barrio INT,
 cod_dist_seccion INT,
 cod_seccion INT,
 cod_edad_int INT,
 EspanolesHombres INT,
 EspanolesMujeres INT,
 ExtranjerosHombres INT,
 ExtranjerosMujeres INT )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = '\073', 'quoteChar' = '"',)
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH "/user/hive/warehouse/datos_padron.db/padron_txt/Rango_Edades_Seccion_202204.csv" INTO TABLE padron_txt;

```
_Nota: Usé LOAD DATA INPATH porque el LOAD DATA LOCAL INPATH con la ruta mnt/hgfs no funcionaba, no tengo ni idea de por qué._

1.3) Hacer trim sobre los datos para eliminar los espacios innecesarios guardando la 
tabla resultado como padron_txt_2. (Este apartado se puede hacer creando la tabla 
con una sentencia CTAS.)
_Nota: La función trim elimina los espacios innecesarios en los datos string_
```
CREATE TABLE padron_txt_2 AS SELECT 
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
 FROM padron_txt;

```
_Nota: Tarda muchísimo tiempo._

1.4) Investigar y entender la diferencia de incluir la palabra LOCAL en el comando LOAD 
DATA.
```
Al insertar datos en Hive, se pueden hacer de dos formas: por el sistema de archivos local, y por HDFS.
Con LOCAL, lo que estamos haciendo es cargar datos de nuestro sistema local, y si no lo especificamos, entonces estaremos cargando en HDFS.
```

1.5) En este momento te habrás dado cuenta de un aspecto importante, los datos nulos 
de nuestras tablas vienen representados por un espacio vacío y no por un 
identificador de nulos comprensible para la tabla. Esto puede ser un problema para 
el tratamiento posterior de los datos. Podrías solucionar esto creando una nueva 
tabla utiliando sentencias case when que sustituyan espacios en blanco por 0. Para 
esto primero comprobaremos que solo hay espacios en blanco en las variables 
numéricas correspondientes a las últimas 4 variables de nuestra tabla (podemos 
hacerlo con alguna sentencia de HiveQL) y luego aplicaremos las sentencias case 
when para sustituir por 0 los espacios en blanco. (Pista: es útil darse cuenta de que 
un espacio vacío es un campo con longitud 0). Haz esto solo para la tabla 
padron_txt.
```
- Primero, comprobamos si hay espacios en blanco en las variables numéricas correspondientes a las 4 últimas
variables de nuestra tabla. Como un espacio vacío es un campo de longitud 0, podemos seleccionar las longitudes
de las variables numéricas que nos atañen:

SELECT length(EspanolesHombres), length(EspanolesMujeres), length(ExtranjerosHombres), length(ExtranjerosMujeres) FROM padron_txt

-Vemos que, por ejemplo, en la columna de ExtranjerosHombres (primera fila) nos aparece el campo de longitud 0.

- Ahora creamos la tabla nueva:

ALTER TABLE padron_txt RENAME TO padron_viej

- Finalmente, sustituímos los espacios en blanco por 0 haciendo uso de la sentencia CASE WHEN

CREATE TABLE padron_txt AS SELECT cod_distrito, 
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
FROM padron_viej

                              
```

1.6) Una manera tremendamente potente de solucionar todos los problemas previos 
(tanto las comillas como los campos vacíos que no son catalogados como null y los 
espacios innecesarios) es utilizar expresiones regulares (regex) que nos proporciona 
OpenCSV.
Para ello utilizamos :
 ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
 WITH SERDEPROPERTIES ('input.regex'='XXXXXXX')
 Donde XXXXXX representa una expresión regular que debes completar y que 
identifique el formato exacto con el que debemos interpretar cada una de las filas de 
nuestro CSV de entrada. Para ello puede ser útil el portal "regex101". Utiliza este método 
para crear de nuevo la tabla padron_txt_2.

```
CREATE EXTERNAL TABLE IF NOT EXISTS padron_txt_2 (
 cod_distrito INT,
 desc_distrito STRING,
 cod_dist_barrio INT,
 desc_barrio STRING,
 cod_barrio INT,
 cod_dist_seccion INT,
 cod_seccion INT,
 cod_edad_int INT,
 EspanolesHombres INT,
 EspanolesMujeres INT,
 ExtranjerosHombres INT,
 ExtranjerosMujeres INT )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex'='"(.*)"\073"([A-Za-z]*) *"\073"(.*)"\073"([A-Za-z]*) *"\073"(.*)
"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"(.*?)"\073"(.*?)"\073"(.*?)"')
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH "/user/hive/warehouse/padron_txt/Rango_Edades_Seccion_202205.csv" INTO TABLE padron_txt_2;

```


_Una vez finalizados todos estos apartados deberíamos tener una tabla padron_txt que 
conserve los espacios innecesarios, no tenga comillas envolviendo los campos y los campos 
nulos sean tratados como valor 0 y otra tabla padron_txt_2 sin espacios innecesarios, sin 
comillas envolviendo los campos y con los campos nulos como valor 0. Idealmente esta 
tabla ha sido creada con las regex de OpenCSV._

## 2. Investigamos el formato columnar parquet ##

2.1) ¿Qué es CTAS?
```
CTAS: Create Table As Select. Es una forma de crear una tabla en Hive a partir de la sentencia SELECT de otra tabla.
```

2.2) Crear tabla Hive padron_parquet (cuyos datos serán almacenados en el formato 
columnar parquet) a través de la tabla padron_txt mediante un CTAS
```
CREATE TABLE padron_parquet STORED AS PARQUET AS SELECT * FROM padron_txt;
```

2.3) Crear tabla Hive padron_parquet_2 a través de la tabla padron_txt_2 mediante un 
CTAS. En este punto deberíamos tener 4 tablas, 2 en txt (padron_txt y 
padron_txt_2, la primera con espacios innecesarios y la segunda sin espacios 
innecesarios) y otras dos tablas en formato parquet (padron_parquet y 
padron_parquet_2, la primera con espacios y la segunda sin ellos).
```
CREATE TABLE padron_parquet_2 STORED AS PARQUET AS SELECT * FROM padron_txt_2;
```

2.4) Opcionalmente también se pueden crear las tablas directamente desde 0 (en lugar 
de mediante CTAS) en formato parquet igual que lo hicimos para el formato txt 
incluyendo la sentencia STORED AS PARQUET. Es importante para comparaciones 
posteriores que la tabla padron_parquet conserve los espacios innecesarios y la 
tabla padron_parquet_2 no los tenga. Dejo a tu elección cómo hacerlo.
```
```

2.5) Investigar en qué consiste el formato columnar parquet y las ventajas de trabajar 
con este tipo de formatos.
```
En primer lugar, Apache Parquet es un formato de archivo diseñado para soportar el procesamiento rápido de datos complejos,
con varias características notables. Entre ellas, está el del formato columnar: A diferencia de los formatos basados en filas,
como CSV, Parquet está orientado a columnas, lo que significa que los valores de cada columna de la tabla se almacenan uno al 
lado del otro, en lugar de los de cada registro. Es de código abierto. 

Ventajas del almacenamiento columnar:
- Compresión.
- Rendimiento: Al ejecutar consultas en su sistema de archivos basado en Parquet, puede centrarse sólo en los datos 
relevantes muy rápidamente.
- Los usuarios pueden empezar con un esquema sencillo, e ir añadiendo gradualmente más columnas al esquema según sea necesario. 
De esta manera, los usuarios pueden terminar con múltiples archivos Parquet con esquemas diferentes pero compatibles entre sí. 
```

2.6) Comparar el tamaño de los ficheros de los datos de las tablas padron_txt (txt), 
padron_txt_2 (txt pero no incluye los espacios innecesarios), padron_parquet y 
padron_parquet_2 (alojados en hdfs cuya ruta se puede obtener de la propiedad 
location de cada tabla por ejemplo haciendo "show create table").
```
```

## _Impala_ ##

## 3. Juguemos con Impala ##

3.1) ¿Qué es Impala?
```
Es un motor SQL pensado para administrar y analizar sobre grandes volúmenes de datos almacenados en Hadoop. Su motor es MPP –procesamiento masivo en paralelo-. Latencias de milisegundos.
```

3.2) ¿En qué se diferencia de Hive?
```
- Impala no soporta ficheros con tipos de datos a medida. 
- No soporta tipo de datos DATE 
- Ni funciones XML y JSON, ni otras de agregación como percentile, percentile_approx,… 
- No soporta sampling (ejecutar queries sobre un subconjunto de una tabla). 
- Vistas laterales 
- Múltiples cláusulas DISTINCT por query. 
- Hive se basa en MapReduce, Impala no, pues implementa una arquitectura distribuida basada en procesos daemon. 
- Hive materializa todos los resultados intermedios para mejorar la escalabilidad y tolerancia a fallos. Impala realiza
streaming de resultados intermedios entre ejecutores. 
- No admite tipos complejos
```

3.3) Comando INVALIDATE METADATA, ¿en qué consiste?
```
Hace que los metadatos de la base de datos/tabla especificada queden obsoletos, de forma que aplicado ese comando,
si Impala quiere ejecutar una consulta en la tabla cuyo metadata está invalidado, Impala recarga el 
metadata asociado antes de que se realice la consulta. Es una operación muy costosa comparada con REFRESH, que actualiza 
incrementalmente el metadata.

```

3.4)  Hacer invalidate metadata en Impala de la base de datos datos_padron
```
- Si ejecuto el siguiente comando me da error porque no es una tabla. ¿Se puede aplicar a bases de datos?
INVALIDATE METADATA datos_padron
```

3.5) Calcular el total de EspanolesHombres, espanolesMujeres, ExtranjerosHombres y 
ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO.
```
SELECT count(espanoleshombres), 
       count(espanolesmujeres), 
       count(extranjeroshombres), 
       count(extranjerosmujeres),
       desc_distrito,
       desc_barrio
FROM padron_txt
GROUP BY desc_distrito, desc_barrio
```

3.6)  Llevar a cabo las consultas en Hive en las tablas padron_txt_2 y padron_parquet_2 
(No deberían incluir espacios innecesarios). ¿Alguna conclusión?
```
SELECT count(espanoleshombres), 
       count(espanolesmujeres), 
       count(extranjeroshombres), 
       count(extranjerosmujeres),
       desc_distrito,
       desc_barrio
FROM padron_txt_2
GROUP BY desc_distrito, desc_barrio


SELECT count(espanoleshombres), 
       count(espanolesmujeres), 
       count(extranjeroshombres), 
       count(extranjerosmujeres),
       desc_distrito,
       desc_barrio
FROM padron_parquet_2
GROUP BY desc_distrito, desc_barrio

Conclusión: 

```

3.7) Llevar a cabo la misma consulta sobre las mismas tablas en Impala. ¿Alguna 
conclusión?
```
SELECT count(espanoleshombres), 
       count(espanolesmujeres), 
       count(extranjeroshombres), 
       count(extranjerosmujeres),
       desc_distrito,
       desc_barrio
FROM padron_txt_2
GROUP BY desc_distrito, desc_barrio


SELECT count(espanoleshombres), 
       count(espanolesmujeres), 
       count(extranjeroshombres), 
       count(extranjerosmujeres),
       desc_distrito,
       desc_barrio
FROM padron_parquet_2
GROUP BY desc_distrito, desc_barrio

Conclusión:

```

3.8) ¿Se percibe alguna diferencia de rendimiento entre Hive e Impala?
```
Impala es hasta 5 veces más rapido que Hive, tiene una latencia mucho menor.
```


## 4. Sobre tablas particionadas. (_Hive + Impala_) ##

4.1) Crear tabla (Hive) padron_particionado particionada por campos DESC_DISTRITO y 
DESC_BARRIO cuyos datos estén en formato parquet.
```
CREATE TABLE padron_particionado ( cod_distrito INT,
                                   desc_distrito STRING,
                                   cod_barrio INT,
                                   desc_barrio STRING, 
                                   cod_barrio INT,
                                   cod_dist_seccion INT,
                                   cod_seccion INT,
                                   cod_edad_int INT,
                                   espanoleshombres INT,
                                   espanolesmujeres INT,
                                   extranjeroshombres INT,
                                   extranjerosmujeres INT)
PARTITIONED BY (desc_distrito, desc_barrio)                                  
                                   
```

4.2)  Insertar datos (en cada partición) dinámicamente (con Hive) en la tabla recién 
creada a partir de un select de la tabla padron_parquet_2
```
```

4.3) Hacer invalidate metadata en Impala de la base de datos padron_particionado
```
```

4.4) Calcular el total de EspanolesHombres, EspanolesMujeres, ExtranjerosHombres y 
ExtranjerosMujeres agrupado por DESC_DISTRITO y DESC_BARRIO para los distritos 
CENTRO, LATINA, CHAMARTIN, TETUAN, VICALVARO y BARAJAS.
```
```

4.5) Llevar a cabo la consulta en Hive en las tablas padron_parquet y 
padron_partitionado. ¿Alguna conclusión?
```
```

4.6) Llevar a cabo la consulta en Impala en las tablas padron_parquet y 
padron_particionado. ¿Alguna conclusión?
```
```

4.7) Hacer consultas de agregación (Max, Min, Avg, Count) tal cual el ejemplo anterior 
con las 3 tablas (padron_txt_2, padron_parquet_2 y padron_particionado) y 
comparar rendimientos tanto en Hive como en Impala y sacar conclusiones.
```
```























