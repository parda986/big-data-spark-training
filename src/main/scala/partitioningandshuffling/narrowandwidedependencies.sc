/**
  * en las previas sesiones vimos que el shuffling ocurre en algunas transformaciones
  */


/**
  * En esta sesion:
  * - Cómo los RDD son representados
  * - Profundizaremos en cuando spark decide mezclar los datos
  *   DAG: Directed Acyclic Graph. Representa las tranformaciones hechas en un RDD
  *   "lineage graph"
  *
  * Por ejemplo tenemos una cadena de transformaciones y queremos usarlas para dos calculos
  * posteriores: por ejemplo en un recuento y una reduccion de los valores
  *
  * Las operaciones inciales pueden ser un map y un filter sobre el RDD por ejemplo, que no devuelven RDD,
  * pero al llamar a persist() permanecen en la memoria para usarse despues en forma de RDD
  *
  * Los graficos de linaje son gran parte de como se representan los RDD en Spark.
  * Spark puede analizar la representacion y hacer optimizaciones
  */

/**
  * Representacion de un RDD
  * Parecen una especie de abstraccion sobre la que invocamos metodos
  * Pero un RDD esta hecho de particiones, que son piezas atomicas del conjunto de datos
  * y pueden existir en uno o varios nodos de computacion. Un RDD puede estar distribuido en muchos nodos de computacion
  * y todas las piezas diseminadas son las particiones.
  */

/**
  * Dependencias
  * Son relaciones de modelo entre un RDD y sus particiones con el RDD o los RDD de los que derivó
  * Si tenemos un RDD padre y un RDD hijo, las dependencias son cómo las particiones padre se asignan a las particiones hijo
  * por ejemplo con una funcion map. Podemos pensar en map como una flecha grande de un RDD hacia otro; pero son varias flechas
  * entre las particiones correspondientes, de las que se van a derivar, realmente
  */

/**
  * Function
  * La funcion que por ejemplo le pasamos a map en el ejemplo anterior
  * La funcion es necesaria para obtener la particion secundaria a partir de la padre, anfitrion
  * Son una gran parte de los RDD porque dicen como calcular los conjuntos de datos basados en los RDDs padre
  */

/**
  * Metadata
  * Son metadatos sobre el esquema de particion y donde se colocan los datos que forman parte de un RDD
  */

/////////////////////////////////////////////

/**
  * Lo anterior esta relacionado con los shuffle/barajadas
  * Recordando la norma:
  * 1. Puede ocurrir un shuffle cuando el RDD resultante "depende" de otros elementos del mismo RDD u otro RDD
  *
  * Ahora tenemos una idea de los que es una dependencia, las dependencias codifican cuando los datos deben moverse por
  * la red, lo que nos lleva a las transformaciones. Asi que sabemos que las transformaciones causan shuffle y la info
  * de dependencias nos puede decir cuando puede ocurrir un shuffle
  *
  * Para diferenciar entre estos podemos definir 2 conjuntos de dependencias que pueden decirnos cuando va a ocurrir shuffle
  *
  * NARROW / ESTRECHAS: cada particion del RDD padre es usada por 1 particion del RDD hijo como mucho
  * "BASTANTE RAPIDAS" No necesitan shuffle necesariamente, es posible optimizaciones como el pipelining / canalizacion
  * (agrupar transformaciones en un solo paso)
  *
  * WIDE / AMPLIAS: cada particion del RDD padre puede ser dependida por multiples RDD hijos, podemos tener muchas particiones hijas
  * "LENTAS" Requieren que todos o algunos datos hagan shuffling en la red
  * Estas son las que provocan SHUFFLING !!
  */

/**
  * Diferencias Narrow vs. Wide
  * - map y filter son narrow porque cada particion hija depende como mucho de una particion padre
  * - union es narrow porque podemos poner los elementos de dos RDD en las particiones correspondientes del RDD resultante
  *   no hay relacion entre RDDs cuando se trata de ver de donde se deriva una particion hija
  * - join con RDDs que ya estan particionados, las dependencias tambien son narrow. Si recordamos que se puede hacer union sin
  * suffling, estos son los tipos de join que tiene una dependencia estrecha. Es asi porque como maximo una particion del RDD padre
  * es usada en el RDD hijo.
  *
  * NARROW: map filter union join...
  *
  * Ejemplos WIDE:
  * groupByKey
  * join cuando las entradas no estamn particionadas, vamos a tener que mover los datos por toda la red para asegurarnos
  * de que el RDD resultante contenga los valores asignados a una clave en las mismas particiones
  * antes de llamar a join podrian extenderse por toda la red.
  *
  * Ejemplo:
  * Si tenemos una transformacion groupBy que provoca shuffling
  * y despues hacemos un join con otro RDD porque esa parte de la union seria Narrow??
  * porque seguramente hayamos hecho un cache() sobre el groupBy, el cual ya hace particiones, y las queremos aprovechar
  * en memoria
  */

/**
  * operaciones NARROW y operaciones WIDE
  *
  * NARROW:
  * map mapValues flatMap filter mapPartitions mapPartitionsWithIndex
  *
  * WIDE (puenden causar Shuffle):
  * cogroup groupWith join leftOuterJoin rightOuterJoin groupByKey
  * reduceByKey combineByKey distinct intersection repartition coalesce
  */

/**
  * Method ".dependencies" sobre un RDD
  * Nos dara la secuencia de objetos de depndencia que en realidad son las dependencias que usa Spark en su programador
  * para saber cuando el RDD depende de otros RDD. Cuando se llame al metodo se obtiene una secuencia de objetos de dependencia con
  * diferentes nombres:
  * NARROW:
  * -OneToOneDependency(la que se ve en la mayoria de los casos), PruneDependency, RangeDependency
  *
  * WIDE:
  * -ShuffleDependency indicador bastante claro de que puede ocurrir un shuffle, por ejemplo si llamamos a dependencies
  * sobre un RDD al que se le ha hecho un groupByKey
  */

/**
  * toDebugString: es otro metodo util sobre los RDD, que imprime el linage entre dos RDDs y otra info relevante para
  * programacion. Sobre un groupByKey veriamos que el RDD devuelto es de tipo Shuffled, que vino de un MapPartitionsRDD,
  * que en si vino de una parallel collection RDD que fue la que paralelizamos incicialmente a partir de una lista de
  * palabras por ejemplo.
  * Spark muestra como se agrupan las operaciones en la jerarquia nen la misma sangria y vemos cuando ocurren shuffles
  * entre ellas. Esas agrupaciones estan separadas por shuffles.
  * Entonces con toDebugString podemos ver como el trabajo se divide en diferentes agrupaciones de operaciones narrow
  * separadas por shuffles, que lo podemos ver con la sangria del linage impreso
  */

/**
  * Spark JOBS:
  * Por saber mas de como se ejecutan los Spark Jobs, las agrupaciones de operaciones Narrow se llaman "Stages" o etapas
  * Asi que un trabajo Spark se divide por el programador en Stages
  */

/**
  * Tolerancia a fallos: vimos los graficos de linaje y como determinar cuando se va a producir un shuffle
  *
  * Los linajes o secuencias de operaciones son clave para la tolerancia a fallos de Spark-
  * Ideas de la programacion funcional lo permiten, se debe a que los RDDs son "inmutable" no se pueden cambiar los datos
  * dentro de un RDD, se pueden usar operaciones como map flatMap o filter para transformar los datos inmutables
  * Lo que tenemos es un DAG (Directed Acyclic Graph). Y suponiendo que nos mantenemos alrededor de la funcion que le pasamos
  * a esas funciones lo que podemos hacer es volver a calcular cualquier punto en el tiempo, cualquier RDD que tengamos en el
  * grafico de linaje. Lo que permite esto es:
  * - los RDD son inmutables
  * - las HighOrder functions map filter flatMap funcionales
  * - una funcion para computar el dataset basado en su padre RDDs tambien es una parte de la representacion
  *
  * CONCLUSION: Podemos recuperar un error al recomputar particiones perdidas de graficos de linaje
  * si guardamos en algun lugar en un almacenamiento estable siempre podemos volver a derivar RDDs individuales en su
  * grafico de linaje, esta es la idea clave. Asi es como Spark logra tolerancia a fallos sin escribir en disco.
  *
  * Asi es como Spark hace el trabajo en memoria y tambien es tolerante a errores
  */

/**
  * Ejemplo: una particion falla
  * Spark puede vover a conducirlo gracias al grafico de dependencias que lo tiene tod listo
  * Sabemos de donde se deriva una pieza de datos y podemos vover a realizar el camino. En caso de que en ningun momento
  * se haya almacenado en memoria con cache() seria desde el principio.
  * Solo necesitamos la info de dependencia y las funciones que se almacenan con esas dependencias y recomputar
  *
  * Recomputar es rapido para las narrow dependencies y lento para las wide
  */
