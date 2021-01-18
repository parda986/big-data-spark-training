/**
  * Los tipos de datos de Spark SQL son los tipicos de BDD
  *
  * Hay algunos tipos de datos complejos, con algunas diferencias respecto a los equivalentes de Scala:
  *
  * - Array[T]   => ArrayType(elementType, containsNull)
  * Son Array de un solo tipo de elemento. containsNull es true si el Array puede contener null values
  *
  * - Map[K, V]  => MapType(keyType, valueType, valueContainsNull)
  * Son Map que tambien contienen el tipo de la clave, el tipo del valor, y si los value pueden ser null, containsNull
  *
  * - case class => StructType(List[StructFields])
  * Las Struct son un poco menos rigidas que las matrices y los mapas.
  * Contienen una lista de posibles campos de diferentes tipos
  * Las matrices y mapas podian ser de un tipo o dos tipos posibles, las estructuras pueden ser cualquier numero
  * arbitrario de tipos.
  * Pero dentro del Struct vamos a tener que tener una lista de campos StructField que contiene un nombre para el campo, tipo
  * del campo y si puede ser null.
  * Se pueden construir tipos de datos mas interesantes asi, por ejemplo podemos tomar una case class y mapearla a
  * algo que SQL pueda entender. Ejemplo:
  * // Scala
  * case class Person(name: String, age: Int)
  *
  * // Spark SQL
  * StructType(List(StructField("name", StringType, true),
  * StructField("age", IntegerType, true)))
  *
  * Para usar los SQL types tenemos que importarlos con la sentencia: import org.apache.spark.sql.types._
  */


/**
  * OPERACIONES SOBRE DATAFRAMES:
  *
  * Hemos visto que los DF solo admiten operaciones relacionales, los que mas se usan:
  * SELECT WHERE LIMIT ORDERBY GROUPBY JOIN
  *
  * Los DF ya no admiten las operaciones map... porque se quisieron restringir el numero de operaciones
  * y de esa manera aprovechar la optimizacion de SQL y las BDDs que ya existia
  *
  * Un truco para poder ver lo que hacemos con ls operaciones relacionales son 2 metodos Spark:
  * - show(): imprime el DF en forma tabular para mostrar como son los datos. Muestra primeros 20 elementos
  *
  * - printSchema(): imprime el esquema del DF en formato de arbol, hojas en formato StructField
  *
  */


/**
  * Transformaciones sobre los DF: operaciones que devuelven otro DF y son lazy evaluated
  *
  * - def select(col: String, cols: String*) : Dataframe
  * Selecciona una o varias columnas del DF y devuelve un DF con esas columnas como resultado
  *
  * - def agg(expr: Column, exprs: Column*) : DataFrame
  * Hace agregaciones sobre una serie de columnas y devuelve un DF con el resultado calculado
  *
  * - def groupBy(col1: String, cols: String*) : Dataframe
  * Agrupa el DF usando las columnas especificadas. Para usar antes de hacer un agregado
  * No devuelve exactamente un DF porque es para ser usado justo antes de la agregacion
  *
  * - def join(right: DataFrame) : DataFrame
  * Inner join con otro DF y devuelve un DF
  *
  * Otras transformaciones:
  * filter, limit, orderBy, where, as, sort, union, drop...
  */


/**
  * Como podemos ver en los metodos la mayoria toman un parametro Column o String, que se refiere a algun atributo
  * o columna del dataset.
  * La mayoria de los metodos sobre DFs tienden a ser bien entendidos, una operacion predefinida sobre una columna del
  * dataset.
  *
  * Se pueden seleccionar y trabajar con columnas de 3 maneras:
  *
  * -Con la notacion $, por ejemplo: df.filter($"age" > 18) requiere import spark..implicits._
  *
  * -Refieriendose al DF, por ejemplo_ df.filter(df("age" > 18))
  *
  * -Usando un SQL querie String, por ejemplo: df.filter("age > 18")
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

val conf = new SparkConf().setAppName("DF").setMaster("local")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)

val empRDD = List(
  Employee(1, "Jose", "Martinez", 31, "Oviedo"),
  Employee(2, "Jorge", "Pardavila", 15, "Gijon"),
  Employee(3, "Luis", "Martinez", 68, "Aviles"),
  Employee(4, "Monica", "Alvarez", 19, "Gijon"))

val empDF = sc.parallelize(empRDD).toDF

/**
  * Empleados de Gijon
  */
val gijonEmployeesDF = empDF.select("id", "lname")
  .where("city = 'Gijon'")
  .orderBy("id").show()

/**
  * Empleados de Gijon: filter y where hacen lo mismo
  */
val gijonEmployeesDF2 = empDF.filter("city = 'Gijon'").show()
val gijonEmployeesDF3 = empDF.where("city = 'Gijon'").show()

/**
  * Filtros complejos:
  */
val gijonOver18EmployeesDF = empDF.filter(($"age" > 18) && ($"city" === "Gijon")).show()


/**
  * GROUPING AND AGGREGATING ON DFs
  *
  * Una de las cosas mas comunes en tablas es agrupar los registros por algun atributo
  * y despues hacer una operacion de agregacion sobre ello como por ejemplo una cuenta
  *
  * Para ello Spark nos da la funcion groupBy, que devuelve un RelationalGroupedDataset,
  * el cual tiene varias operaciones de agregacion standard definidas sobre el, como:
  * count, sum, max, min, y avg
  *
  * Para agrupar y agregar debemos llamar a groupBy sobre un atributo o columna especifico del DF,
  * Seguido de alguna llamada a algun metodo de RelationalGroupedDataset, como count, max, o agg
  * Para agg(...) debemos especificar tambien que atributo/columna deben ser llamado
  * las spark.sql.functions como count, max, sum etc...
  */

val aggDFSum = empDF.groupBy($"city")
  .agg(sum($"age")).show()

val aggDFCount = empDF.groupBy($"city").count().show()

/**
  * Los mas viejos y jovenes por ciudad
  */
val oldestByCity = empDF.groupBy($"city").max("age").show()
val youngestByCity = empDF.groupBy($"city").min("age").show()


/**
  * Un ejemplo mas chungo
  */
case class Post(authorID: String, subforum: String, likes: Int, date: String)

val postRDD = List(
  Post("1", "Tech", 31, "2020-01-01"),
  Post("1", "Music", 6, "2020-01-01"),
  Post("2", "Music", 15, "2020-01-01"),
  Post("3", "Space", 68, "2020-01-01"),
  Post("3", "Space", 68, "2020-01-01"),
  Post("4", "Sea", 19, "2020-01-01"),
  Post("4", "Space", 19, "2020-01-01"),
  Post("4", "Space", 19, "2020-01-01"),
  Post("4", "Space", 19, "2020-01-01"))

val postDF = sc.parallelize(postRDD).toDF

/**
  * Queremos ver que personas escriben en cada subforo
  * y despues ver la persona que mas escribio en cada subforo
  *
  * Agrupamos por autor y dentro de cada autor por foro, porque pudo
  * escribir varias veces en los subforos
  * Como resultado mostrar el numero de veces que escribio cada autor
  * en cada foro
  */
val mostBussy = postDF.groupBy($"authorID", $"subforum")
  .agg(count($"authorID"))
  .orderBy($"subforum", $"count(authorID)".desc).show()


/**
  * TENEMOS DOS API
  *
  * -RelationalGroupedDataset: methods following a groupBy(...)
  *
  * -Metodos dentro de agg: min, max, sum, mean, stddev, count, avg, first,
  * last... Para ver toda la lista podemos verlos en org.apache.spark.sql.functions
  */

