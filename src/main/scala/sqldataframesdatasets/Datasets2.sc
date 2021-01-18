import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

val conf = new SparkConf().setAppName("Datasets").setMaster("local")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

val keyValuesList = List((3, "Me"), (1, "Thi"), (2, "Se"), (3, "ssa"), (1, "sIsA"), (3, "ge:"),
  (3, "-)"), (2, "cre"), (2, "t"))
val keyValuesRDD = sc.parallelize(keyValuesList)
val keyValuesDF = keyValuesRDD.toDF

val reduceRDD = keyValuesRDD.reduceByKey(_ + _)

val keyValuesDS = keyValuesList.toDS
val reduceDS = keyValuesDS.groupByKey(p => p._1)
  .mapGroups((k, vs) => (k, vs.foldLeft("")((acc, p) => acc + p._2)))
  .sort($"_1")
reduceDS.show()

val reduceGroupsDS = keyValuesDS.groupByKey(p => p._1)
  .mapValues(p => p._2) // nos quedamos solo con las cadenas individuales porque reduceGroups no puede trabajar con pares
  .reduceGroups((acc, str) => acc + str).sort($"value") // va sumando las cadenas de cada grupo en acc
reduceGroupsDS.show()

val strConcat = new Aggregator[(Int, String), String, String] {
  def zero: String = ""

  def reduce(b: String, a: (Int, String)): String = b + a._2

  def merge(b1: String, b2: String): String = b1 + b2

  def finish(reduction: String): String = reduction

  def bufferEncoder: Encoder[String] = Encoders.STRING

  def outputEncoder: Encoder[String] = Encoders.STRING
}.toColumn

val reduceWithAggregator = keyValuesDS.groupByKey(p => p._1)
  .agg(strConcat.as[String]).sort($"value")
reduceWithAggregator.show()

/**
  * En la creacion del Aggregator nos daba un error de compilacion
  * porque faltaba por implementar dos metodos bufferEncoder y outputEncoder
  * ¿Que son los Encoder?
  * Son las cosas que convierten los datos entre:
  * JVM Objects y Spark SQL internal (tabular) representation
  * Son requeridos por todos los DS!!!
  * Estan muy especializados, son generadores de codigo optimizados que generan bytecode
  * custom para la serializacion y deserializacion de nuestros datos
  *
  * Los datos serializados se guardan usando internamente el formato binario Tungsten
  * que permite operaciones sobre datos serializados y mejora de la utilizacion de memoria
  *
  * Ya que los Encoder se centran en serializar y deserializar, ¿Porque no vale Java o Kryo?
  * Por dos razones:
  * - tenemos un conjunto muy limitado de tipos de datos
  *     Esta limitado y es optimo para primitivos y case classes, Spark SQL data types,
  *     que ya conocemos bien
  * - Contienen info de esquema lo que hace que sean generadores de codigo lo mas optimos
  *     posible, permite optimizacion basada en la forma de los datos. Spark entiende
  *     la estructura de los datos en los DS, puede crear un diseño mas optimo, en memoria
  *     cuando se cachean los DS
  * - Usa significativamente menos memoria que la serializacion Java o Kryo
  * - 10x faster que la serializacion de Kryo (en Java aun mas lento)
  */

/**
  * Hay un Encoder para cada conjunto de datos, cada DS tiene codificadores para sus tipos
  * Hemos estado creando DS por todas partes y no hemos visto los Encoder
  * Hay dos formas de introducirlos:
  * - Automaticamente con import spark.sql.implicits._ sobre SparkSession
  * - A veces estamos obligados a pasar un Encoder explicitamente:
  *   org.apache.spark.sql.Encoder que contiene una gran seleccion de metodos para crear
  *   Encoders desde:
  *   -scala primitive types: scalaInt, scalaLong, scalaByte
  *   -INT, LONG, STRING: para primitivos nullables
  *   -product/tuple: para los Scala Product y Tuple (cuando tenemos tipos complejos
  *   de case classes)
  *   Ejemplos de creacion de Encoders
  */
Encoders.scalaInt
Encoders.STRING

case class Person(name: String, age: BigInt) extends Product
val personEncoder = Encoders.product[Person]
personEncoder.schema.printTreeString()

/**
  * Acciones comunes sobre DSs:
  * SON LAS MISMAS QUE SOBRE DFs Y RDDs
  *
  * collect(): Array[T]
  * count(): Long
  * first(): T/head(): T
  * foreach(f: T => Unit): Unit
  * reduce(f: (T, T) => T): T
  * show(): Unit
  * take(n: Int): Array[T]
  */

/**
  * DSs vs. DFs vs. RDDs
  *
  * Usar DSs cuando:
  * - tenemos datos estructurados y semi-estructurados
  * - queremos typesafety
  * - necesitamos trabajar con APIs funcionales
  * - necesitamos buena performance pero tampoco la mejor
  *
  * Usar DFs cuando:
  * - tenemos datos estructurados y semi-estructurados
  * - queremos la mejor performance posible, optimizada automaticamente por nosotos
  *
  * Usar RDDs cuando:
  * - tenemos datos sin estructura
  * - necesitamos tunear bien y controlar detalles de bajo nivel sobre los calculos sobre el RDD
  * - tenemos tipos de datos complejos que no pueden ser serializados con Encoders
  */


/**
  * LIMITACIONES DE LOS DSs
  *
  * Catalyst no puede optimizar todas las operaciones
  * por ejemplo con filter:
  *
  * -Con optimizacion Catalyst:
  * Una operacion filter relacional, por ejemplo:
  * ds.filter($"city".as[String] === 'Boston'
  * El rendimiento es mejor porque le estamos diciendo explicitamente a Spark
  * qué columnas/atributos y condiciones son requeridas en la operacion de filter.
  * Con la info acerca de la estructura de los datos y la estructura de las computaciones,
  * el optimizador de Spark sabe que solo puede acceder los campos envueltos en el filter
  * sin tener que instanciar el tipo de dato entero
  * Evita movimiento de datos por la red
  *
  * -Sin optimizacion Catalyst:
  * Una operacion filter funcional, por ejemplo:
  * ds.filter(p => p.city == "Boston")
  * El mismo filtro escrito con una funcion literal es opaco para Spark,
  * es imposible para Spark hacer Introspect sobre la lambda function.
  * Lo unico que sabe Spark es que necesitas un registro entero serializado como
  * un Scala object para devolver true o false al analizar la condicion,
  * requiriendo a Spark que haga mucho mas trabajo para encontrar
  * ese requerimiento implicito
  */


/**
  * No se pierden todas las optimizaciones, solo algunas importantes.
  *
  * -La clave es que cuando usamos DSs con funciones high-order como map,
  * perdemos muchas optimizaciones de Catalyst.
  *
  * -Cuando usamos DSs con operadores relacionales como select,
  * tenemos todas las optimizaciones de Catalyst
  *
  * -Aunque no todas las operaciones sobre DSs se benefician de las optimizaciones
  * de Catalyst, Tungsten está siempre corriendo bajo el capo con los DSs,
  * guardando y organizando los datos de una forma muy optimizada,
  * que puede resultar en grandes velocidades en comparacion con los RDDs regulares,
  * asi que no es nada, solamente perdemos un poco en Catalyst si usamos funciones high-order
  * pero aun asi tenemos los beneficios de Tungsten
  *
  * Esto nos da una idea de la capacidad de Spark para optimizar las operaciones
  * sobre DSs
  */

/**
  * Otras dos limitaciones de los DSs, tambien compartidas por los DFs:
  *
  * -Limited data types: si no podemos expresar los datos con case classes/Products
  *   y los Spark SQL data types standard, puede ser dificil asegurar que un
  *   Encoder Tungsten exista para nuestro tipo de datos
  *   Por ejemplo tenemos una app con una clase regular Scala complicada y no podemos
  *
  * -Requieren datos estructurados o semi-estructurados:
  *   Si nuestros datos sin estructura no pueden ser reformulados para adherirse
  *   a algun tipo de esquema, sera mejor usar RDDs
  */

