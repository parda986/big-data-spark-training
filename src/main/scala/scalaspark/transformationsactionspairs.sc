import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val ages = List(3, 45, 64, 3, 45, 66, 64)

/**
  * GROUP BY: grupos por edades
  */
val agesGrouped = ages.groupBy {
  age =>
    if (age >= 18 && age < 65) "adult"
    else if (age < 18) "child"
    else "senior"
}

/**
  * GROUP BY KEY: presupuesto total por organizador
  */
case class Event(organizer: String, name: String, budget: Int)

val events = List(
  Event("Pepe", "Lev", 60000),
  Event("Luis", "Southwest 4", 100000),
  Event("Jorge", "Sonar", 80000),
  Event("Jorge", "Monegros", 120000)
)

val conf = new SparkConf().setAppName("Eventos").setMaster("local")
val sc = new SparkContext(conf)
//hacemos un map a los eventos (organizador, cantidad) y lo paralelizamos
val eventsRdd = sc.parallelize(events map (evt => (evt.organizer, evt.budget)))
//agrupamos los eventos por clave, el organizador
val groupedRDD = eventsRdd.groupByKey()
//suma las cantidades de los eventos de cada organizador
val budgetsRDD = eventsRdd.reduceByKey(_ + _)
groupedRDD.collect().foreach(println)
budgetsRDD.collect().foreach(println)

/**
  * calcular el presupuesto medio por organizador
  * - mapValues para obtener pares (organizer, (budget, 1))
  * - reduceByKey sobre elementos contiguos pora obtener RDD[organizer, (Int, Int)]
  * que nos dara pares (organizador, sumPresupuestos, numeroPresupuestos)
  * OBTIENE PRESUPUESTO TOTAL Y NºEVENTOS POR ORGANIZADOR
  */
val intermediate = eventsRdd.mapValues(b => (b, 1))
  .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

/**
  * AVG BUDGETS PER ORGANIZER: divide el presupuesto total / nº eventos
  */
val avg = intermediate mapValues {
  case (budget, countEvents) => budget / countEvents
}
avg.collect().foreach(println)

/**
  * KEYS: obtiene un RDD con las claves decada tupla
  */
groupedRDD.keys.collect().foreach(println)

/**
  * KEYS: contar el numero de visitantes unicos de una pagina
  */
case class Visitor(ip: String, timestamp: String, duration: String)
val visits = List(
  Visitor("198.24.23.01", "23 05 2020", "30s"),
  Visitor("198.24.23.02", "23 05 2020", "30s"),
  Visitor("198.24.23.03", "23 05 2020", "30s"),
  Visitor("198.24.23.03", "23 05 2020", "30s")
)
val visitsMap = sc.parallelize(visits map (v => (v.ip, v.duration)))
val numUniqueVisits = visitsMap.keys.distinct().count()

/**
  * MAS METODOS SOBRE PARES RDD en la API:
  * PairRDDFunctions
  */
