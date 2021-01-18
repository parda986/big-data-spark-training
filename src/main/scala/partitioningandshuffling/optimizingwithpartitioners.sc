/**
  * ¿Por que deben usarse los particionadores?
  * Tenemos:
  *
  * -HashPartitioners
  * -RangePArtitioners
  *
  * -Operaciones que pueden introducir nuevos partitioners
  *   o que pueden descartar partitioners custom
  *
  * ¿Pero por qué alguien va a querer reparticionar sus datos?
  * Es porque puede conllevar enormes ganancias de rendimiento,
  * especialmente en las operaciones que exigen barajado o shuffle
  * Si hay alguna manera de optimizar la localidad de los datos, eso
  * podria evitar un gran trafico de red que significa grandes latencias
  *
  * ¿Cuanto particionamiento usar para ayudar al rendimiento?
  *
  * Ej: groupByKey() vs. reduceByKey() => x3 speedUp
  *
  * Todavia se puede hacer mejor que eso, podemos usar RangePartitioners
  * para optimizar el uso temprano de reduceByKey, asi que no resulta en ningun
  * shuffle alrededor de la red !!!!
  */
import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

val conf = new SparkConf().setAppName("Partitioning").setMaster("local")
val sc = new SparkContext(conf)

case class CFFPurchase(customerID: Int, destination: String, price: Double)

val purchases = sc.parallelize(List(
  CFFPurchase(1, "Gijon", 15),
  CFFPurchase(1, "Oviedo", 10),
  CFFPurchase(1, "Ibiza", 30),
  CFFPurchase(2, "Barcelona", 10)
))

/**
  * HACER EL REDUCEBYKEY PERO CON EL RANGEPARTITIONER
  * hace que el proceso sea 9 veces mas rapido que el groupByKey
  */
val pairs = purchases.map(p => (p.customerID, p.price))
// 8 partitions puede tener sentido para nuestro cluster
val tunedPArtitioner = new RangePartitioner(8, pairs)
val partitioned = pairs.partitionBy(tunedPArtitioner).persist()
val purchasesPerCust = partitioned.map(p => (p._1, (1, p._2)))
val purchasesPerMonth = purchasesPerCust
  .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).collect()

/**
  * OTRO EJEMPLO CON PARTITIONBY del libro "Learning Spark" pags. 61-64
  * Tenemos una aplicacion de medios en la que los usuarios pueden suscribirse a temas de articulos
  * Quieren leer articulos de un tema relacionado
  * En la app tenemos una enorme tabla de info del usuario y la almacenamos en memoria en forma de un
  * PairRDD, podemos llamarlo userData, con clave idUser y el valor userInfo, el valor son todos los temas a los que el
  * user esta suscrito
  *
  * De vez en cuando la aplicacion tiene que conbinar la info de los usuarios con un conjunto mas pequeño que representa
  * eventos sucedidos en los ultimos 5 minutos que contendra pares (userID, LinkInfo) en la que se refleja los links
  * pinchados. LinkInfo son los enlaces que el user ha pinchado en los ultimos cinco minutos.
  *
  * En el ejemplo podemos contar cuantos usuarios han pulsado un enlace no relacionado con los temas a los que esta suscrito
  * Es parecido a la aplicacion CFF vista. Sabemos que vamos a tener que hacer join de los conjuntos de datos,
  * vamoa a querer agrupar userInfo en LinkInfo para los usuarios activos en los ultimos 5 minutos
  *
  * SEMANA 3 OPTIMIZING WITH PARTITIONERS
  * - EJEMPLO MALO, NO EFICIENTE POR EL JOIN, PORQUE NO SABE COMO ESTAN PARTICIONADAS LAS CLAVES EN LOS CONJUNTOS DE DATOS
  *   ENTONCES SPARK SOLO PUEDE HACER HASH DE TODAS LAS CLAVES DE AMBOS CONJUNTOS Y LUEGO ENVIAR TODOS LOS ELEMENTOS CON EL
  *   MISMO HASH DE CLAVE A TRAVES DE LA RED PARA ESTAR EN LA MISMA MAQUINA, UNA VEZ HECHO ESO PUEDE UNIR LOS ELEMENTOS
  *   CON LA MISMACLAVE EN ESA MAQUINA.
  *   ESTO LO HACE CADA VEZ QUE SE INVOCA EL METODO DEL EJEMPLO DEL LIBRO:
  *   Cada vez que se invoca, a pesar de que el PairRDD userData no cambia, asi que no tiene sentido enviarla a traves de la
  *   red si no cambia.
  *
  * - ARREGLAR ESO ES FACIL !! solo hay que usar "partitionBy(new HashPartitioner(100))" con x partitions y persist() en el PairRDD userData
  *   Al hacer esto Spark ya sabra que es un RDD hash-partitioned, y las llamadas a join sobre el tomaran ventaja de esa
  *   info, cuando llamemos a "userData.join(events)", Spark solo barajara la eventsRDD, mandando los eventos con cada
  *   userID particular a la maquina que contiene la particion hash correspondiente de userData.
  *   Los eventos con idUser especificos son los unicos que tienen que ser enviados a maquinas con las particiones hash
  *   correspondientes de userData. No hace falta barajar mas el grupo grande de datos userData
  */


/**
  * Volviendo al ejemplo del groupByKey, viendo lo que pasa por debajo, recordamos que agrupar todos los valores de los
  * pares clave-valor con la misma clave requiere recopilar todos los pares clave-valor con la misma clave en la misma
  * maquina. la agrupacion se hace usando un particionador con los params predetermindos.
  * Luego el RDD resultante se configura para usar el mismo particionador hash que se uso para construirlo.
  *
  * ¿Pero entonces cuando sucedera un barajado?
  *
  * Puede ocurrir "cuando el RDD resultante depende de otros elementos del mismo RDD u otro RDD"
  * Asi que algunas operaciones como join, solo viendo como funcionan fundamentalmente, la idea es que deberian
  * depender de otras partes del mismo RDD u otros RDD, lo que es como una advertencia de que el barajado podria
  * estar ocurriendo.
  * Para reducirlo lo que podemos hacer es usar un particionado inteligente de los datos, evitando en gran medida
  * o totalmente el barajado de los datos
  *
  * Hay otros trucos o metodos para ayudar a descubrir cuando un shuffle se ha ejecutado:
  * - tipo de retorno en la workSheet "ShuffledRDD"
  * - usando la funcion toDebugString para ver su plan de ejecucion por ejemplo: partitioned.reduceByKey(...).toDebugString
  *   que dara toda la info de como se planea el trabajo y permite estar atento a las señales de advertencia como los
  *   ShuffledRDDs que pueden aparecer
  *
  * - Lo mejor que se puede hacer es tener en cuenta qué operaciones pueden causar shuffling_
  * cogroup, groupWith, join, leftOuterJoin, rightOuterJoin, groupByKey, reduceByKey, combineKey, distinct, intersection,
  * repartition, coalesce.
  * *Se puede intuir logicamente por lo que hacen los metodos, pero esos son los mas sospechosos habituales.
  * Por el nombre todos parecen tener que ver con la particion, asi que si los datos estan particionados, van a estar
  * moviendo datos alrededor, asi que son los que posiblemente van a causar shuffling
  *
  * LO QUE DEBEMOS RECORDAR DE ESTO ES QUE A VECES ES POSIBLE EVITAR QUE SE BARAJEN LOS DATOS EN LA RED CON EL PARTICIONADO
  *
  * HAY DOS ESCENARIOS EN LOS QUE SE PUEDE EVITAR EL SHUFFLING MEDIANTE EL PARTICIONADO:
  * 1 Cuando se usa una operacion como groupByKey en RDDs preparticionados
  *   -> con "reduceByKey" hacemos que los valores se computen localmente ya que ya han sido preparticionados/preshufflados
  *   de manera que el trabajo se puede hacer en las particiones locales en los nodos de trabajo sin que los Workers tengan
  *   que volver a mezclar sus datos entre si; y en este caso el unico mmomento en que los datos tienen que ser movidos es
  *   con la reduccion final de los valores, momento en que se enviaran de vuelta de los nodos de trabajo al nodo controlador
  *
  * 2 Preparticion antes de hacer uniones. Podemos evitar por completo barajando previamente los dos RDD unidos con el mismo
  * particionador. Y por supuesto tambien debe asegurarse de que los RDD previamente particionados se almacenen en cache
  * despues de la particion, lo que hace posible calcular la union completa localmente sin mezcla en la red, ya que los datos
  * que se deben unir de ambos RDD ya se han reubicado para estar en el mismo nodo en la misma particion, por lo tanto no es
  * necesario mover los datos en ese caso.
  *
  * Lo importante: como organizar los datos en un cluster, porque se puede pasar de mezclar repetidamente datasets grandes
  * mientras intentamos unirlos a un dataset mas pequeño a no tener que mezclar ningun dato en absoluto. Solo con organizar
  * y particionar los datos de forma inteligente desde el comienzo del trabajo.
  *
  * Solo evitando que los datos se muevan por la red hemos visto mejoras de x10 en la velocidad en pequeños ejemplos,
  * cuando no tienen que hacerlo. esa aceleracion segun lo visto en la semana 1 deberia acelerar el trabajo un x10.
  * Un trabajo podria pasar de durar 4 horas a 40 horas viendolo asi, de manera que la particion es importante.
  * 
  */