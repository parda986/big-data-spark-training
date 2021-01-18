/**
  * SPARK SQL
  *
  * A pesar de la creciente popularidad de Spark, SQL ha sido y sigue siendo la lengua franca para hacer analytics
  * Pero es muy dificil conectar las Big Data Processing Pipelines como Spark o Hadoop a una SQL database
  * Generalmente en apps de BDD uno tiene que escoger entre SQL, Spark o Hadoop
  *
  * ¿No estaria guay si pudieramos mezclar SQL queries con Scala?
  * ¿Para obtener todas la optimizaciones que estabamos usando en las comunidades de BDD sobre los Spark Jobs?
  *
  * Spark SQL ofrece ambas bondades
  * OBJETIVOS:
  * - Apoyar el procesamiento relacional en Spark, mezclando codigo relacional como sintaxis declarativa de SQL con API
  *     de manera funcional dentro de programas Spark y sobre Datasources externas con una friendly API como la Spark RDD API
  *   A veces es mejor expresar una computacion como sintaxis declarativa SQL y otras con APIs funcionales
  *   Por ejemplo seleccionar los valores de una columna con SQL se hace en una linea; de la otra forma habria que hacer
  *   un map o un filter.
  *
  * - Alto rendimiento, logrado usando tecnicas de investigacion en BDDs
  *
  * - Soporte facil para obtener nuevas fuentes de datos tales como semi-structured data y BDDs externas
  *   ¿No molaria si pudieramos leer datos semi-estructurados como JSON?
  *   Spark SQL busca agregar procesamiento relacional a Spark, traer un rendimiento super alto de las optimizaciones en
  *   el mundo de las BDDs y admitir la lectura en datos de conjuntos de datos estructurados y semi-estructurados
  *
  *   ¿Entonces que es Spark SQL, otro sistema que tenemos que aprender?
  *   No, es solo un componente de la pila de Spark
  *   -Es un modulo de Spark para procesar datos estructurados, o hacer consultas relacionales
  *   -Como una biblioteca en lo mas alto de Spark
  *
  *   Entonces podemos pensar en ello como agregar nuevas API a las API que ya conocemos y no tenemos que aprender ningun
  *   sistema ni nada.
  *
  * Lo que Spark SQL agrega:
  *
  *   Hay 3 APIs principales:
  *   - SQL literal syntax
  *   - Dataframes
  *   - Datasets
  *
  *   Hay 2 componentes especializados en backend:
  *   - Catalyst: un optimizador de queries
  *   - Tungsten: off-heap serializer, serializador fuera del monton,
  *       establecera otra forma de sistema que codificara los objetos Scala en una representacion muy eficiente
  *       fuera del monton del recolector de basura
  *
  * Queries relacionales:
  * Everything sobre SQL es estructurado, tiene un conjunto de tipos fijo
  * tiene un conjunto de opearaciones fijo: SELECT, WHERE, GROUP BY, etc.
  * La industria se ha esforzado en explotar la rigidez de las BDDs relacionales para obtener mejora
  * de la velocidad del performance
  *
  * DataFrame:
  * es la abstraccion core de SparkSQL, que es conceptualmente equivalente a una tabla de BDDs
  * Los DataFrame son colecciones conceptualmente distribuidas de registros,
  * que tienen un esquema conocido, cosa que los diferencia de los RDD, ya que requieren algun tipo de info de esquema
  * sobre los datos que contiene
  * Los DataFrame no tienen tipo, no tiene algun tipo de parametro de tipo que el compilador de Scala compruebe estaticamente
  * Los RDD sin embargo si tienen un parametro de tipo RDD[T], con DataFrames Scala no comprueba el tipo en su esquema
  * Las transformaciones en DataFrames se conocen como transformaciones sin tipo
  *
  * SparkSession: el objeto que necesitamos para empezar con Spark SQL
  * Si recordamos el SparkContext del resto del curso, el SparkSession es el contexto de Spark para SparkSQL
  * Asi que ahora se usa el SparkSession en vez del SparkContext
  * Solo hay que importar el objeto SparkSession y utilizarlo parecido a SparkContext, se pueden añadir configuraciones
  * personalizadas pero en general hay que empezar con un constructor, nombrar la app y se pueden poner en medio configs
  * y llamar a getOrCreate(). Esto es lo minimo para iniciar una sesion de Spark SQL
  * El objeto SparkSession esta destinado a reemplazar a los contactos de Spark en linea, asi que es bueno investigar esto
  *
  * DataFrame: Creacion
  * Dos formas:
  *
  * 1. De un RDD existente
  *   a. Deduciendo el esquema
  *   b. Proporcionando explicitamente el esquema que debe estar contenido en nuestro DataFrame
  *
  * 2. Leyendo en algun tipo de fuente de datos especifica del archivo
  * Leyendo en algun tipo de estructura comun o formato semi-estructurado como JSON
  *
  * Ejemplos:
  *
  * 1. Crear DF a partir de RDD existente por inferencia de esquema reflexiva:
  *   se llama a toDF(...) sobre un PairRDD con parametros nombres de las columnas
  *   si no pasamos params Spark asignara numeros a las columnas del estilo _1 _2 _3...
  *
  * 2. Crear DF a partir de un RDD de elementos de algun tipo de case class, lo que es mejor.
  *   Si tenemos un RDD[Persona] lo que tenemos que hacer es llamar a toDF() sobre el RDD de Personas
  *   Los nombres de columna se infieren automaticamente como los nombres de los campos de la case class
  *   y ello se hace con cada reflexion
  *
  * 3. Crear DF a partir de RDD especificando explicitamente un esquema. Esto es util cuando no es posible aplicar una case class
  * que se pueda usar como su esquema. Esto entonces toma 3 pasos a realizar:
  *     - Crear un RDD de filas a partir del RDD original
  *     - Crear un esquema por separado que coincide con la estructura de las filas
  *     - Aplicar el esquema al RDD de filas usando el metodo createDataFrame que nos da el SparkSession
  *
  *     Ejemplo: creacion explicita y manual de un esquema y un DataFrame a partir del esquema
  *     Tenemos un RDD de Personas para el ejemplo. Podriamos hacerlo con los dos metodos pero es ejemplo para mostrar lo
  *     anterior
  *     - Codificar el esquema de la cadena manualmente por ejemplo "name age"
  *     - Crear el esquema con StructType y StructField que coincidan con la forma de los datos para los que estoy trantando
  *       de construir un esquema. Lo conseguimos por ejemplo rompiendo la cadena con split y mapeando en ella creando StructFields
  *       con nombre y un StringType.
  *       y finalmente el esquema con StructType(fields)
  *     - Luego convertimos el RDD[Persona] a un RowRDD dividiendolo con split y mapeando a los objetos Row con los atributos que
  *     necesitamos. Finalmente le pasamos el objeto RowRDD y el esquema al SparkSession con el metodo createDataFrame(rowRDD, schema)
  *     Es la manera mas involucrada pero a veces necesaria.
  *
  * 4. La mejor manera probablemente sea leyendo los datos de un archivo fuente. Usando el propio objeto SparkSession
  *    se pueden leer varios tipos de datos estructurados o semi-estructurados usando el metodo read. Por ejemplo para leer
  *    datos e inferir el esquema de un fichero JSON seria: spark.read.json("jsonFilePath")
  *    Es comno leer un archivo de texto pero mejor. Leera los datos estructurados y desestructurados y creara el DF
  *    con esos datos.
  *    Hay un puñado de formatos que Spark SQL puede leer automaticamente para nosotros, no puede leer arbitrariamente
  *    Los mas comunes son: JSON Parquet CSV y JDBC. Hay metodos para la lectura directa en datos estructurados y semi-estructurados
  *    Se puede visitar la API de ellos para ver la lista de metodos disponible.
  */


/**
  * Una vez que tenemos el DF podemos escribir sintaxis SQL familiar para operar con el dataset
  *
  * Si tenemos un DF de Personas lo que tenemos que hacer es registrarlo como una vista SQL temporal con
  * createOrReplaceTempView("people"), con esto damos un nombre al DF en SQL, podemos referirnos a el con la sentencia
  * SQL FROM, es decir lo registramos como tabla para operar con el con SQL.
  * Una vez registrado podemos hacer consultas con el objeto SparkSession:
  * spark.sql("SELECT * FROM people WHERE age > 17") usando literales SQL
  *
  * Ahora sabemos que estamos escribiendo SQL Queries sobre Spark que no es una BDDs SQL, ¿qué sentencias tenemos disponibles?
  * Las que estan disponibles son las mismas que estan en una consulta Hive, sentencias estandar:
  * SELECT, FROM, WHERE, COUNT
  * HAVING, GROUP BY, ORDER BY, SORT BY
  * DISTINCT, JOIN, LEFT/RIGTH/FULL OUTER JOIN
  * Subqueries
  *
  * Hay paginas sobre las sentencias soportadas por Spark SQL, cheatsheet sobre trucos HiveQL, listas de features de Hive
  * soportadas por Spark SQL
  */


/**
  * Una consulta SQL interesante:
  * Hasta ahora hemos creado un DF y hecho una consulta muy simple
  * Imaginamos que tenemos un DF que representa un conjunto de datos de empleados, ¿como creamos el conjunto de datos?
  * Tenemos una case class Empleado con una serie de campos: id, nombre, apellido...
  * Para crear el DF simplemente creamos un RDD y llamamos a toDF en el. Queremos los id y apellidos de los empleados
  * que trabajan en una ciudad especifica y despues ordenar los resultados en orden asc. de id
  *
  * spark.sql("""SELECT id, lname FROM employees WHERE city = "SYDNEY" ORDER BY id""")
  *
  */

