/**
  * Antes de hablar de Spark SQL,
  * hay que hablar de estructura y optimizacion, para motivar a Spark SQL
  *
  * EJEMPLO:
  * Empresa llamada Code Award que da becas a programadores que han superado alguna adversidad
  *
  * Tenemos 2 conjuntos de datos: Demographic y Finances
  * -Demographic: info de las personas ==> PairRDD(id, demographic)
  * -Finances: para saber si las personas tienen deudas, personas dependientes o algo que apoyar financieramente, etc
  * ==> PairRDD(id, finances)
  *
  * El criterio para asignar la beca puede ser economico por necesidades economicas de la gente: tienen deudas o dependientes
  * Ver cuantas personas son elegibles para la beca: tienen que ser suizos y con alguna dependencia economica
  *
  * POSIBILIDADES:
  *
  * 1. Para hacer esto podemos hacer primero un innner join de los dos RDD con las claves comunes y despues filtrar:
  * suizos, con dependientes, con deudas y contar
  * Nos devolvera pares (Id, (demographics, finances))
  *
  * 2. Otra posibilidad seria hacer los filtros primero. Asi a la hora de hacer el join los conjuntos de datos serian mas
  * pequeños
  *
  * 3. Otra posibilidad seria empezar con el producto cartesiano de demo y finances y luego pasar a traves de los filtros ç
  * que tienen los  mismos identificadores o IDs (equivalente al inner join), y finalmente filtrar las que son de suiza con dependientes y deudas y
  * contar al final
  */


/**
  * Aunque el resulktado de las 3 posibilidades es el mismo, en terminos de velocidad hay grandes diferencias:
  *
  * MEJOR OPCION:
  * SOLUCION 2: es 3.6x faster que la solucion 1. FILTRANDO ANTES DE HACER EL JOIN
  *
  * PEOR SOLUCION:
  * LA 3: 177X slower que la solucion 1
  * NUNCA USAR EL PRODUCTO CARTESIANO PRIMERO!!!
  *
  * USAR PRIMERO EL FILTRO QUE LA COMBINACION ES LA OPCION MAS EFICIENTE AUNQUE MUCHA GENTE PUEDA PENSAR QUE NO
  *
  * ¿No estaria muy bien que Spark supiera que la mejor opcion es la 2 y optimizara el codigo?
  * Si hemos dado un poco de info estructural Spark es capaz de hacer muchas de esas optimizaciones por nosotros
  * y ello viene en forma de SparkSQL.
  *
  * Estructura:
  * No todos los datos son iguales estructuralmente, caen en un espectro que va de no estructurado a estructurado
  *
  * - Structured: es algo como una tabla de base de datos como SQL o algo que es muy rigido con un esquema fijo
  *
  * - Semi-Structured: JSON XML. Estos tienen algun tipo de esquema y son llamados auto-descriptivos, decriben su propia estructura
  * Pero los esquemas no son tan rigidos como en las tablas de bases de datos.
  *
  * - Unstructured: ficheros de log o imagenes
  *
  * Con esto podemos ver que hay un buen espectro en cuanto a estructura
  *
  * Con los RDD nos hemos enfocado en Unstructured y Semi-Structured, ya que hemos leido de registros o datos JSON
  * Hemos analizado esos JSON en case class objects manualmente y luego hemos hecho calculos a gran escala.
  *
  * Hasta ahora Spark y los RDD vistos no tienen ningun concepto sobre el esquema de los datos que esta tratando
  * No tiene idea de donde esta la estructura de los datos que esta operando.
  * Lo que sabe Spark es que el RDD esta parametrizado con algun tipo de type arbitrario como Persona Account o Demographic
  * Pero no sabe nada de esos tipos de estructura, solo tiene un nombre Persona que debe tener una Persona dentro, pero
  * no sabe de que esta hecha esa persona.
  *
  * Por ejemplo:
  * tenemos una clase Account compuesta por nombre:String balance:Double risk:Boolean
  * En un RDD de Accounts lo unico que sabe Spark es que ahi hay Account, su nombre.
  * Spark no puede mirar en los objetos y analizar las partes de la estructura usadas en el calculo.
  * A lo mejor uno de los objetos Account tiene 100 y solo necesitamos 1 asi que Spark serializara los
  * objetos de cuenta realmente grandes y los enviara por la red aunque no sea necesario la mayoria de los
  * datos enviados.
  * Spark no puede optimizar eso por nosotros porque no puede ver dentro de esos objetos, no puede optimizar en funcion de
  * la estructura
  *
  *
  * Sin embargo en algun tipo de datos estructurado como una tabla de BDD por ejemplo, los calculos se realizan en columnas
  * de valores con nombre y tipo, asi que se conoce toda la estructura del conjunto de datos.
  * Es una configuracion estructurada como en una tabla de BDD o en Hive.
  * En una tabla de BDD se conoce toda la estructura del conjunto de datos. las BDD tienden a ser muy optimizadas
  * Asi que en el ejemplo de las Account de antes esto se podria hacer porque conocemos toda la estructura del conjunto
  * de datos en el que estamos operando, sabemos que partes vamos a utilizar y podemos hacer la optimizacion en base a esa info
  *
  * Computacion
  * Hasta ahora hemos hablado de datos estructurados frente a no estructurados
  * Pero lo mismo podria decirse de la computacion, que puede ser estructurada o no estructurada
  * En Spark hacemos transformaciones funcionales a los datos: pasamos funciones definidas por el usuario a higher-Order
  * functions como map, flatMap, filter. Estas operaciones tambien son completamente opacas para Spark:
  * un usuario puede hacer algo dentro de una de ellas y lo unico que ve Spark es: $anon$1@604f1a67
  * Podemos decir que tambien es opaco y desetructurado para Spark. Un blob opaco que Spark no puede optimizar
  *
  * Sin embargo en una BDD / Hive hacemos transformaciones declarativas en los datos estructurados del conjunto y todas las
  * operaciones tienden a ser muy estructuradas muy especializadas. Muy fijas, muy rigidas, predefinidas. Sabemos las
  * operaciones que podriamos hacer potencialmente y podemos hacer muchas optimizaciones basadas en conocer todas las
  * posibilidades para estas operaciones.
  *
  * Para poner una cosa al lado de la otra:
  * -Cuando miramos a los RDD de Spark lo que vemos  es un monton de objetos desestructurados de los que no sabemos mucho
  * y algun tipo de funcionalidad de la que no sabemos nada, un lambda que no podemos ver ni optimizar
  *DIFICIL OPTIMIZAR AGRSIVAMENTE POR TENER POCA ESTRUCTURA
  *
  * -Por otro lado tenemos ese tipo de datos muy estructurado, son datos especificados, organizados en filas y columnas
  * en una estructura muy rigida con un conjunto muy rigido de operaciones que podemos hacer en esos datos estructurados
  *TENEMOS MUCHA ESTRUCTURA, LAS POSIBILIDADES DE OPTIMIZACION SON MUCHAS: reordenar operaciones, poner filtros antes de
  * las uniones etc......
  *
  * OPTIMIZACIONES SPARK
  * toda esa charla sobre datos y calculos estructurados, ¿como la entiende Spark?
  * Porque normalmente, los RDDs no trabajan con datos no estructurados, tenemos que analizarlos nosotros, y hay limites
  * en los calculos que podemos hacer, los calculos estan definidos como funciones de alto nivel que hemos definido nosotros mismos
  * sobre nuestros propios tipos de datos.
  * Pero como hemos visto tenemos que hacer que toda la optimizacion funcione por nosotros mismos!!
  * Hay pensar en qué esta pasando en el cluster y optimizar nosotros mismos.
  *
  * En el mundo de las BDD las optimizaciones se realizan automaticamente
  *
  * ¿No molaria que Spark pudiera hacer las optimizaciones que hacen las BDD?
  * Eso es lo que hace Spark SQL, aunque renunciando a la flexibilidad y libertad que hemos aprendido con la API de colecciones
  * funcionales con el fin de darle a Spark alguna estructura y mas posiblidades de optimizar
  *
  *
  */