import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().setAppName("Joins").setMaster("local")
val sc = new SparkContext(conf)

/**
  * JOIN une dos PairRDD:
  * -Inner joins(join): devuelve un PairRDD cuyas claves estaban en ambos RDD y sus values pairs
  * con los values de ambos
  *
  * -Outer joins(leftOuterJoin/rightOuterJoin): las claves no tienen por que estar presentes en ambos RDD
  * Podemos usar "LEFT" o "RIGHT" en funcion del RDD que queremos discriminar o incluir
  * La diferencia entre los dos es qué pasa con las claves cuando ambos RDD no contienen la misma clave
  */
val abos = sc.parallelize(List(
  (101, ("Ruetli", "ABONO GENERAL")),
  (102, ("Brelaz", "DemiTarif VISA")),
  (103, ("Gress", "DemiTarif")),
  (104, ("Schatten", "DemiTarif"))
))

val locations = sc.parallelize(List(
  (101, "Bern"),
  (101, "Thun"),
  (102, "Lausanne"),
  (102, "Geneve"),
  (102, "Nyon"),
  (103, "Zurich"),
  (103, "St-Gallen"),
  (103, "Chur")
))
//CONSERVA CLAVES PRESENTES EN AMBOS
val trackCustomers = abos.join(locations)
trackCustomers.collect().foreach(println)
//CONSERVA LAS CLAVES de abos
val trackCustomers2 = abos.leftOuterJoin(locations)
trackCustomers2.collect().foreach(println)
//CONSERVA LAS CLAVES DE LOCATIONS
val trackCustomers3 = abos.rightOuterJoin(locations)
trackCustomers3.collect().foreach(println)



