import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._

val conf = new SparkConf().setAppName("Datasets").setMaster("local")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

case class Listing(street: String, zip: Int, price: Int)

val listings = List(
  Listing("Ribadesella", 33207, 90000),
  Listing("Luanco", 33207, 80000),
  Listing("Aviles", 33207, 100000),
  Listing("Schultz", 33210, 70000),
  Listing("Av. Llano", 33210, 75000)
)

val listingsDF = sc.parallelize(listings).toDF

val avgPricesDF = listingsDF.groupBy($"zip").avg("price")

/**
  * Ahora deberiamos llamar a collect() para devolver el
  * resultado al nodo maestro en forma de matriz:
  *
  * Array[org.apache.spark.sql.Row]
  * Es correcto que nos devuelva los datos en un Row, que esta destipado
  */
val avgPrices = avgPricesDF.collect()

/**
  * Tenemos que castear las cosas recordando las columnas y el orden
  * Esto es un intento de hacerlo.
  * ESTO VA A DAR ClassCastException asi que no es bueno probar tipos al
  * tuntun, mejor:
  * Mejor imprimir el esquema de un elemento para ver los tipos, porque
  * nos podemos equivocar
  */
//val avgPricesAgain = avgPrices.map {
//  row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Int])
//}
avgPrices.head.schema.printTreeString()

/**
  * Ahora sabemos que los tipos son un Int y un Double
  * Asi que podemos recuperar el resultado en un Array[(Int, Double)]
  */
val avgPricesAgain = avgPrices.map {
  row => (row(0).asInstanceOf[Int], row(1).asInstanceOf[Double])
}

/**
  * PUES NADA! Eso es horrible de leer y podria estar 3 años probando
  * diferentes moldes e incluso seguir equivocado,
  * esto es propenso a errores aunque a veces funciona.
  *
  * ¿No seria bueno que Spark SQL hiciera optimizaciones typesafety
  * para evitar eso?
  * Estaria bien tener disponible el ".price"
  * Pero en DFs no lo tenemos porque estan destipados
  */


/**
  * Aqui es donde entran los DATASETS!!
  * Datasets o conjuntos de datos.
  * Son muy buenos porque combinan las optimizaciones con la tipografia
  * de accesor por "."
  *
  * Hay algo importante que no sabemos, y es que:
  *
  * Los DFs son en realidad Datasets!!
  *
  * type DataFrame = Dataset[Row]
  *
  * Pero entoneces qué son los DS? Tienen algo mas que los RDD: diferencias:
  * - Pueden ser pensados como colecciones distribuidas de datos tipadas
  * - La API de Dataset unifica las API de Dataframe y la API de RDD. Se puede mezclar
  * - Los DS requieren datos estructurados o semi-estructurados.
  * El corazcon de los schemas y encoders parte de los Dataset
  *
  * Una manera de pensar en los Dataset es como un compromiso entre los RDD y los DF
  * - Se obtiene mas info de tipo con Datasets que con DataFrames
  * - Se obtiene mas optimizacion en Datasets(puede aprovechar Catalyst)
  * que en RDD (de canalizacion)
  *
  * - Los datasets adquieren algunas de esas optimzaciones de los DF y tiene a la vez
  * esa bonita API funcional y flexible
  *
  * Ejemplo de combinaciones de las API DF y DS:
  */

/**
  * AVG HOME PRICE PER ZIPCODE CON DATASETS
  */
val listingsDS = sc.parallelize(listings).toDS

val avgPricesDS = listingsDS.groupByKey(l => l.zip) // Se parece al groupByKey sobre RDDs
  .agg(avg($"price").as[Double]) // Se parece a los operadores sobre DFs pero diciendole el tipo

val avgPricesDS2 = listingsDF

avgPricesDS.collect()
avgPricesDS.show()

/**
  * DATASETS: son algo en el medio de RDDs y DFs
  * Permite usar las operaciones relaciones de los DFs
  * Los Datasets añaden mas typed operations que pueden ser usadas tambien
  * Los Datasets dejan usar funciones como map, flatMap, filter de nuevo!!
  *
  * Los DS pueden ser usados cuando queremos un MIX de transformaciones
  * funcionales y relacionales mientras seguimos beneficiandonos de
  * algunas de las optimizaciones sobre los DFs
  * Y casi tambien tenemos una API typesafe tambien
  */
case class Person(name: String, age: BigInt)

val myDS = spark.read.json("C:/Users/Usuario/IdeaProjects/ScalaSpark/src/main/resources/people/people.json")
  .as[Person]

myDS.collect()
myDS.show()

/**
  * Podemos crear DS con toDS sobre un DF
  * Leyendo de un JSON con un objeto en cada linea con spark.read.json(path).as[case class]
  * a partir de un RDD
  * De tipos comunes de Scala:
  */
val wordListDS = List("Hola", "me", "llamo", "Jorge").toDS
wordListDS.collect()

case class Employee(id: Int, fname: String, lname: String, age: Int, city: String)

val empList = List(
  Employee(1, "Jose", "Martinez", 31, "Oviedo"),
  Employee(2, "Jorge", "Pardavila", 15, "Gijon"),
  Employee(3, "Luis", "Martinez", 68, "Aviles"),
  Employee(4, "Monica", "Alvarez", 19, "Gijon"))

val empRDD = sc.parallelize(empList)
val empDS = empRDD.toDS
empDS.collect()

/**
  * Recordando las transformaciones destipadas sobre los DFs,
  * La API de Dataset incluye ambas transformaciones: tipadas y destipadas
  *
  * LAS TRANSFORMACIONES SOBRE DATASET SE AGRUPAN EN:
  *
  * - destipadas: las que aprendimos en los DFs
  * - tipadas: las variantes tipadas de la mayoria de los DFs
  * + transformaciones adicionales como las similares a RDD, las higher-order functions:
  * map, flatMap, etc....
  *
  * DF y DS son conjuntos de datos y las API estan perfectamente integradas
  * Se puede llamar a map sobre un DF aunque no lo hemos visto
  * Cuando llamamos a un map en un DataFrame acabamos recuperando un Dataset
  * ¿Y si no tenemos info del tipo como la creamos?
  *
  * Ejemplo: tenemos un DF de pares y quremos hacer un calculo, incrementar los valores
  * de las claves
  */
val keyValuesList = List((3, "Me"), (1, "Thi"), (2, "Se"), (3, "ssa"), (1, "sIsA"), (3, "ge:"),
  (3, "-)"), (2, "cre"), (2, "t"))
val keyValuesRDD = sc.parallelize(keyValuesList)
val keyValuesDF = keyValuesRDD.toDF
val keyValuesDFMap = keyValuesDF.map(row => row(0).asInstanceOf[Int] + 1)

/**
  * De nuevo tenemos esa cosa fea de castear cada elemento si quiero hacer algo con el
  * Podemos quedar sin info de tipo al hacer eso, hay que tener cuidado
  * No todas las operaciones RDD estan disponibles sobre Dataset
  * Las que están, no todas tienen el mismo aspecto en DS y en RDD
  *
  * No se puede comenzar a programar RDD en la parte superior del conjunto de datos
  * sin mirar la API.
  * Puede ser que tengamos que explicitar la info de tipo cuando vamos de un DF a un DS
  * con typed transformations.
  */


/**
  * TYPED TRANSFORMATIONS COMUNES SOBRE DATASETS, devuelven DS:
  *
  * - map: aplica una funcion a cada elemento y devuelve el DS resultante
  * - flatMap: lo mismo pero devuelve un DS de los contenidos de los iteradores devueltos
  * - filter: aplica un predicado a los elementos del DS y devuelve los que han pasado la condicion
  * - distinct: Devuelve el DS con los duplicados eliminados
  *
  * - groupByKey: agrupa por clave, en si mismo no es transformacion, devuelve un
  * KeyValueGroupedDataset[K, T] que contiene las operaciones de agregacion que SI
  * devuelven DS
  * - coalesce
  * - repartition
  */

/**
  * Con groupByKey sobre DS es un poco lo mismo que groupBy sobre los DF
  * se espera una operacion en medio para devolver el resultado agrupado
  *
  * Evitar groupBy sobre DS para mantenernos en la API de Dataset
  *
  * Despues del groupByKey sobre DS podemos llamar a:
  * - reduceGroups: reduce los elementos de cada grupo de datos usando la funcion binaria
  * especificada, la operacion debe ser asociativa y conmutativa o el resultado puede
  * ser non-deterministic
  * -agg: Computa la agregacion dada, devuelve un Dataset de tuplas por cada clave unica
  * y el resultado de computar esa agregacion sobre los elementos del grupo
  */
val avgEmpDS = empDS.agg(avg($"age"))
avgEmpDS.collect()

//SI NO TUVIERAMOS LA CASE CLASS Employee
// tendriamos que especificar el tipo de ($"age").as[Double]


/**
  * Volviendo a las operaciones sobre esos KeyValueGroupedDataset[K, T]:
  * Hay un par de metodos que realmente no son agregados pero que se usan:
  *
  * mapGroups:
  * aplica la funcion dada a cada grupo de datos,
  * para cada grupo la funcion le sera pasada cada clave de grupo y un iterador
  * con los elementos de cada grupo, la funcion puede devolver un elemento de un tipo
  * arbitrario que sera devuelto como un nuevo Dataset. Similar a reduceGroups
  *
  * flatMapGroups: hace lo mismo pero es una version aplanada del mismo
  */


/**
  * Acabamos de pasar por una gran lista de transformaciones y no hemos nombrado
  * reduceByKey que normalmente usabamos en RDDs
  *
  * Los DS no tienen una operacion reduceByKey
  * Se puede intentar hacer igual de facil que con los RDD?
  * ¿Se puede emular?
  *
  */
val reduceRDD = keyValuesRDD.reduceByKey(_ + _)
reduceRDD.collect()

/**
  * Sobre los DS, al agrupar por clave aun no tenemos un DS,
  * es mapGroups: toma una funcion de (k, v) => otro tipo
  * La k es un un int y el vs una coleccion de cadenas o grupo de valores
  * En RDDs primero agrupa por clavey luego reduce los valores
  * fue facil, una concatenacion _+_ de elementos adyacentes con la misma clave
  * Aqui tenemos que hacer algo asi en mapGroups:
  * De nuevo la funcion devuelve un par de valores (k, v) donde la k se conserva
  * y con la coleccion de valores vs hacemos un foldLeft para concatenar las
  * cadenas juntas.
  *
  * ASI ES COMO EMULAR EN DSs LO QUE HACE UN reduceByKey
  */
val keyValuesDS = keyValuesRDD.toDS
val reduceDS = keyValuesDS.groupByKey(p => p._1)
  .mapGroups((k, vs) => (k, vs.foldLeft("")((acc, p) => acc + p._2)))
  .sort($"_1").show() // ordenamos para ver el mensaje correctamente


/**
  * Parece que funciona pero hay un gran problema cone este enfoque
  * que podemos comprobar en la API de mapGroups:
  * cuidado, mapGroups no soporta la agregacion parcial, y como resultado
  * requiere Shuffling de todos los datos del DS
  * Si una app quiere hacer aggregacion sobre cada clave,
  *
  * ES MEJOR USAR LA FUNCION    reduce
  * O usar un                   org.apache.spark.sql.expressions#Aggregator
  */


/**
  * Usando reduceGroups con mapValues
  * reduceGroups es similar a mapGroups porque se centra en el grupo de valores
  * pero esto esta haciendo una reduccion en el grupo de valores
  */
val reduceGroupsDS = keyValuesDS.groupByKey(p => p._1)
  .mapValues(p => p._2) // nos quedamos solo con las cadenas individuales porque reduceGroups no puede trabajar con pares
  .reduceGroups((acc, str) => acc + str).sort($"_1") // va sumando las cadenas de cada grupo en acc
reduceGroupsDS.show()

/**
  * Aggregator: es una clase que genericamente permite hacer agregaciones de datos
  * Similar al metodo aggregate visto con los RDDs: en el cual pasabamos 2 funciones
  *   y el valor zero que pasariamos al metodo agregado en los RDDs
  * Tiene esta pinta
  * -IN: el tipo del input del agregator
  * BUF: tipo intermedio durante la agregacion
  * OUT: el tipo del output de la agregacion
  *
  * En los RDDs el metodo agregado permitia cambiar los tipos y hacer los calculos
  * en paralelo. Di vidimos nuestro calculo en un par de funciones, lo que hizo posible calcular
  * algunas piezas en paralelo y combinarlas todas mas tarde.
  *
  * Esto es exactamente lo que hace Aggregator
  * Son agregaciones muy especializadas
  * org.apache.spark.sql.expressions.Aggregator
  */

val strConcat = new Aggregator[(Int, String), String, String] {
  def zero: String = ""
  def reduce(b: String, a: (Int, String)): String = b + a._2
  def merge(b1: String, b2: String): String = b1 + b2
  def finish(reduction: String): String = reduction
  def bufferEncoder = ???
  def outputEncoder = ???
}.toColumn

/**
  * toColumn: lo ultimo que hacemos es convertirlo a Column porque vamos a analizarlo
  * en una agregacion, debe ser de tipo TypedColumn[(Int, String), String]
  * Con el Aggregator definido podemos usarlo en la funcion anterior
  */
val reduceWithAggregator = keyValuesDS.groupByKey(p => p._1)
  .agg(strConcat.as[String]) // lo hacemos con cadena porque es el tipo del resultado

//NO FUNCIONA EL WSHEET, creamos Datasets2 para continuar.........