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
  * HASH PARTITIONING = GROUPBYKEY:
  * mueve los datos alrededor de la red o el cluster
  * Su objetivo es hacerlo de la manera mas justa posible
  * Lo primero es calcular la particion p para cada tupla en el PairRDD
  * El hash de la clave % numPartitions predeterminado
  * Cualquiera que sea el resultado en funcion del nº de particiones
  * Esa es la particion en la que entra una clave.
  * Las tuplas de la misma particion se enviaran a la maquina que aloja esa
  * particion.
  * La particion hash intenta difundir alrededor de los datos de la manera mas uniforme posibole
  * sobre todas las particiones basadas en las claves
  */
val purchasesPerCust = purchases.map(p => (p.customerID, p.price))
  .groupByKey()
purchasesPerCust.collect().foreach(println)

/**
  * RANGE PARTITIONING: mas eficiente cuando tenemos claves int char string...
  * las claves son particionadas de acuerdo a:
  * - Un orden en las claves,
  * - Un set de rangos ordenados de claves
  * Propiedad: las tuplas con claves en el mismo rango aparecen en la misma maquina
  *
  * Si lo hicieramos con hash vemos que las claves no se distribuyen de forma uniforme
  * en las particiones.
  * Ya que nuestras claves no son negativas y tienen un orden Podemos usar la particion
  * de rango para mejorar la particion e igualarla significativamente
  * Cada nodo tendra un conjunto de rangos, por ejemplo para los id de clientes
  * Y asi las claves se distribuiran entre ellos de forma mas uniforme
  */

/**
  * Podemos querer personalizar como se distribuyen los datos en el cluster de 2 formas:
  * 1-  "partitionBy" sobre un RDD aportando un "Partitioner" explicito
  * 2-  Usando transformaciones que devuelven RDDs con particiones especificas
  */


/**
  * PARTITIONBY
   */
val pairs = purchases.map(p => (p.customerID, p.price))
val tunedPArtitioner = new RangePartitioner(8, pairs)
/**
  * SI NO HACEMOS PERSIST(), LOS DATOS SE VAN A RECALCULAR UNA Y OTRA VEZ ALREDEDOR DE LA RED
  * Y eso lo queremos evitar diciendoselo a spark con persist()
  * Una vez que mueve los datos a traves de la red y reparticiones, mantenlos donde estan,
  * persistelos en la memoria. Si no estaremos accidentalmente reparticionando los datos
  * en cada iteracion de un algoritmo de ejecucion de maquina sin saberlo.
  * Para crear un particionador de rango hay que especificar el numero de particiones
  * y proporcionar un PairRDD con claves que se ordenan y asi Spark sea capaz de crear un conjunto adecuado
  * de rangos para usar para particionar en funcion del conjunto de datos real
  *
  * LOS RESULTADOS DE PARTITIONBY SIEMPRE DEBEN PERSISTIR !!!!
  * DE LO CONTRARIO LA PARTICION SE APLICA REPETIDAMENTE, ES DECIR SE BARAJA CADA VEZ
  * QUE SE USA EL RDD, Y NO DESEAMOS QUE ESO SUCEDA
  */
val partitioned = pairs.partitionBy(tunedPArtitioner).persist()


/**
  * TRANSFORMATIONS ON RDDs
  * - pasar particionadores a traves de RDDs padre, es decir RDDs emparejados resultado
  * de alguna transformacion en un PairRDD ya particionado; normalmente se configura para
  * utilizar el mismo HashPartitioner que se uso para construir a su padre.
  *
  * - particionadores seteados automaticamente:
  * alguna de las transformaciones resultan automaticamente en un RDD con un particionador diferente
  * Se hace cuando contextualmente tiene sentido, por ejemplo:
  *   -si usamos un sortByKey: se usa un RangePartitioner porque necesita claves ordenadas para ordenarlas
  *   tiene sentido que las claves una vez ordenadas esten particionadas de tal manera que las claves similares
  *   esten en la misma particion.
  *   -Por el contrario usaremnos la particion predeterminada o hashPartitioning cuando usamos la
  *   particion hash groupByKey()
  */

/**
  * LISTA DE TRANSFORMACIONES EN UN PairRDD que mantienen y propagan un particionador:
  * cogroup, groupWith, join, leftOuterJoin, rightOuterJoin, groupByKey, reduceByKey,
  * foldByKey, comnbineByKey, partitionBy, sort, mapValues(if parent has partitioner),
  * flatMapValues(if parent has partitioner), filter(if parent has partitioner)
  *
  * El resto de operaciones producen un RDD sin particionador
  *
  * esto es importante por cuestiones de rendimiento mas adelante
  *
  * Son operaciones que podrian resultar en una particion diferente para sus datos
  * solo hay que tener en cuenta lo que pasa por debajo para controlar el rendimiento
  *
  * Podemos ver que ne la lista falta map y flatMap sin embargo estan flatMapValues filter y mapValues
  * Podemos ver que esas tres operaciones mantienen particionador si el padre lo tenia
  * Por otro lado cualquier transformacion que no sean esas producira un resultado que no es un
  * particionador y que incluya map y flatMap.
  * Si usamos map o flatMap en un RDD con particiones el RDD perdera su particionador
  * Lo que puede arruinarlo porque hemos organizado los datos de una manera a traves de un cluster
  * y si se usa una operacion como flatMap, perdemos toda la particion y los datos tienen que ser
  * movidos otra vez posiblemente.
  *
  * ¿Porque el resto de operaciones produce un resultado que no tiene una particion?
  * Por ejemeplo con map/flatMap: tenemos un RDD particionado hash que queremos mapear.
  * ¿Porque puede tener sentido perder el particionador en el resultado?
  * Porque en una operacion map es posible cambiar la clave y puede ser intencionado
  * Puede que no queramos conservar el particionador en el resultado porque las claves
  * van a ser diferentes, ya no se corresponderan con las particiones correctas
  *
  * En vez de map o flatMap usaremos "MAPVALUES" CUANDO SE PUEDA
  * SI ESTAMOS TRANSFORMANDO UN RDD DEBEMOS INTENTAR LLEGAR A MAPVALUES, QUE PERMITE SEGUIR
  * HACIENDO TRANSFORMACIONES DE MAPA SIN PODER CAMBIAR LAS CLAVES EN UN PairRDD.
  * Podemos mantener la particion alrededor usando mapValues lo que es inteligente
  * Si hacemos el trabajo para dividir los datos de forma inteligente en el cluster
  * todavia permite hacer una operacion de mapa sin perder la particion alrededor
  * que corresponde a como nuestros datos estan organizados.
  *
  * AUN HAY MAS COSAS QUE PODEMOS HACER PARA QUE LA PARTICION REDUZCA EL BARAJADO DE DATOS
  * Y COMO PUEDE SER MUY BUENO PARA EL RENDIMIENTO SI SE SINTONIZA CON CUIDADO
  */

val pruebaGit = "Esto es una prueba"