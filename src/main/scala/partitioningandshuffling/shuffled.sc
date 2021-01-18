import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().setAppName("Shuffled").setMaster("local")
val sc = new SparkContext(conf)

val pairs = sc.parallelize(List((1, "one"), (2, "two"), (3, "three")))

/**
  * Con esto movemos los datos a traves de la red para agruparlos por clave,
  * proceso tambien llamado "shuffling"
  * org.apache.spark.rdd.RDD[(Int, Iterable[String])] = ShuffledRDD[1]
  */
pairs.groupByKey()

case class CFFPurchase(customerID: Int, destination: String, price: Double)

val purchases = sc.parallelize(List(
  CFFPurchase(1, "Gijon", 15),
  CFFPurchase(1, "Oviedo", 10),
  CFFPurchase(1, "Ibiza", 30),
  CFFPurchase(2, "Barcelona", 10)
))

val purchasesPerMonth = purchases.map(p => (p.customerID, p.price))
  .groupByKey()
  .map(p => (p._1, (p._2.size, p._2.sum)))
purchasesPerMonth.collect().foreach(println)
/**
  * CON REDUCEBYKEY MEJOR!!!!
  * APLICAMOS EL MISMO MAP PERO AÑADIENDO UN 1 PARA SER MAS FACIL CONTAR
  *
  * HASTA 3 VECES MAS RAPIDO ! ! !
  *
  * APLICAMOS REDUCEBYKEY SOBRE LOS ELEMENTOS ADYACENTES con la misma clave
  * -Para cada par de elementos adyacentes sumamos sus viajes (nº viajes, dinero total)
  */
val purchasesPerMonth2 = purchases.map(p => (p.customerID, (1, p.price)))
  .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
purchasesPerMonth2.collect().foreach(println)