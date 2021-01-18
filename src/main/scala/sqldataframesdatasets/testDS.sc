import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._

val conf = new SparkConf().setAppName("Datasets").setMaster("local")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

case class Listing(street: String, zip: Int, price: Int)

val listings = Seq(
  Listing("Ribadesella", 33207, 90000),
  Listing("Luanco", 33207, 80000),
  Listing("Aviles", 33207, 100000),
  Listing("Schultz", 33210, 70000),
  Listing("Av. Llano", 33210, 75000)
)

val listingsDS = sc.parallelize(listings).toDS

listingsDS.groupByKey(l => l.zip) // Se parece al groupByKey sobre RDDs
  .agg(avg($"price").as[Double]) // Se parece a los operadores sobre DFs