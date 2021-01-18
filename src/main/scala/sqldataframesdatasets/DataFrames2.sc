/**
  * Vamos a ver :
  * - Trabajar con values perdidos
  * - Actions sobre los DF
  * - Joins con los DF
  * - Optimizaciones sobre los DF
  */

/**
  * A veces en un dataset tenemos valores con null o Nan
  * En estos caso es preferible hacer lo siguiente:
  * - eliminar las filas o registros con valores null o NaN que no queremos
  * - reemplazar algunos valores con una constante
  *
  * ELIMINACIONES
  * drop(): elimina filas que contienen null o NaN en cualquier columna y devuelve una nuevo DF
  * drop("all"): elimina filas que contienen null o NaN en todas las columnas y devuelve una nuevo DF
  * drop(Array("id", "name")): elimina las filas que contienen null o NaN en las columnas especificadas devuelve nuevo DF
  *
  * REEMPLAZOS
  * fill(0): reemplaza null o NaN en columnas numericas con el valor especificado y devuelve nuevo DF
  * fill(Map("minBalance" -> 0)): reemplaza null o NaN en columnas numericas especificadas con el valor especificado
  * y devuelve nuevo DF
  * replace(Array("id"), Map(1234 -> 8923))): reemplaza en la columna id las ocurrencias de 1234, por 8923
  *
  */


/**
  * ACTIONS SOBRE DATAFRAMES
  *
  * Como los RDD, los DF tambien tienen su propio set de acciones:
  * collect() : Array[Row] devuelve un Array que contiene todas las Rows del DF
  * count() : Long numero de filas del DF
  * first() : Row/head(): Row devuelve la primera fila del DF
  * show() : Unit muestra las primeras 20 filas del DF de forma tabular
  * take(n) : Array[Row] devuelve las primeras n filas del DF
  *
  * La mas util es show() que ya hemos usado
  * Mas bien estamos usando agregacion en vez de esas operaciones
  */


/**
  * JOINS
  * Son similares a los join con PairRDD. Con una diferencia principal de uso: DF no son pares K-V,
  * tenemos que especificar las columnas por las que vamos a hacer el join
  *
  * Varios tipos de join disponibles: inner, outer, left_outer, right_outer, leftsemi:
  *
  * df1.join(df2, $"df1.id" === $"df2.id")
  *
  * Para escoger un tipo de join diferente a la inner pasamos un segundo parametro:
  *
  * df1.join(df2, $"df1.id" === $"df2.id", "right_outer")
  */

/**
  * EJEMPLO CON EL METRO DE SUIZA
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val conf = new SparkConf().setAppName("DF").setMaster("local")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().getOrCreate()

import spark.implicits._

case class Abo(id: Int, v: (String, String))

case class Loc(id: Int, v: String)

//ABOS
val as = List(Abo(101, ("Ruetli", "AG")), Abo(102, ("Brelaz", "DemiTarif")),
  Abo(103, ("Gress", "DemiTarifVisa")), Abo(104, ("Schatten", "DemiTarif")))

val abosDF = sc.parallelize(as).toDF

//LOC
val ls = List(Loc(101, "Bern"), Loc(101, "Thun"), Loc(102, "Lausanne"), Loc(102, "Geneve"),
  Loc(102, "Nyon"), Loc(103, "Zurich"), Loc(103, "St-Gallen"), Loc(103, "Chur"))

val locationsDF = sc.parallelize(ls).toDF()

//Join
val joinDFs = abosDF.join(locationsDF, abosDF("id") === locationsDF("id")).show()

//left_outer,
//Queremos la info de los pasajeros y los viajes aunque
//no haya info de viajes de ese pasajero
val leftOuterJoinDFs = abosDF.join(locationsDF, abosDF("id") === locationsDF("id"), "left_outer").show()

/**
  * Volviendo al ejemplo de las becas para estudiantes podriamos hacer
  * la consulta asi, y el optimizador de consultas del Catalyst haria
  * el resto de trabajo por nosotros:
  */

//demographicsDF.join(financesDF("ID") === financesDF("ID"), "inner")
//.filter($"hasDebt" && $"hasFinancialDependents")
//.filter($"CountryLive" === "Switzerland")
//.count

/**
  * COMPONENTES ESPECIFICOS DE BACKEND, para optimizar
  * -CATALYST: query optimizer
  * -THUNGSTEN: codificador de datos fuera del monton o serializador
  *
  * Viendo como Spark SQL se relaciona con el resto de Spark
  * en el mapa que teniamos en otra sesion, el Catalyst crea RDD muy optimizados
  * para ser analizados en el Spark regular
  *
  * Por otro lado veiamos que en el lado de los RDD no teniamos a penas estructura
  * y en el lado de los DF si, entonces son mas faciles de optimizar
  *
  * En los RDD teniamos esa funcion que se ejecuta sobre los elementos
  * Pero no sabemos exactamente su contenido
  */


/**
  * Catalyst:
  * Asumimos que tiene un conocimiento completo y comprension de todos
  * los tipos de datos
  * Conoce el esquema exacto de nuestros datos
  * Conoce detalladamente las computaciones que queremos hacer
  *
  * En funcion de eso puede optimizar:
  *
  * - reordenar las operaciones que son perezosas antes de ejecutarse en el cluster
  * (por ejemplo fusiona las operaciones de filtrado y las lanza lo antes posible
  * sobre la capa de datos)
  *
  * -reducir la cantidad de datos a leer: si sabemos los tipos de datos posibles
  * en el programa, y sabemos el esquema exacto de los datos y sabemos el
  * calculo exacto que se va a hacer, y los campos. Asi podemos mover menos
  * datos por la red.
  * De esta manera Catalyst puede reducir, seleccionar, serializar y enviar
  * solo las columnas relevantes para el conjunto de datos
  *
  * -eliminacion de particiones innecesarias
  * Analiza las operaciones de DF y filtrado para averiguar y omitir particiones
  * no necesarias en los calculos. Saben que no hay que ejecutar un calculo en algunas
  * particiones, ahorra tiempo.
  *
  * -Otras....
  */


/**
  * Thungsten:
  * Los tipos de datos estan restringidos a los SQL datatypes
  * -Necesitamos un encoder de datos altamente especializado
  * -Basado en columnas
  * -Fuera del recolector de basura
  *
  * Tenemos un conjunto de datos del que lo sabemos toda
  * Eso le da a Tungsten la posibilidad de proporcionar codificadores
  * altamente especializados para codificar esos datos
  *
  * Tungsten puede tomar la info del esquema y conociendo los tipos de datos
  * posibles para construir el esquema, puede empaquetar de manera apretada y
  * optima los datos serializados en la memoria.
  *
  * Permite que quepan mas datos en la memoria y tener una serializacion/deserializacion
  * mas rapidas, que son tareas de la CPU
  * Es un descodificador altamente optimizado y especializado y super rendimiento
  *
  * Mantiene los datos serializados en memoria en un formato especial y acceder a ellos mas rapido
  *
  * Almacena los tods los datos en columnas.
  * Almacenar los datos tabulares en formato columnar viene de la observacion de que
  * la mayoria de los calculos que se realizan en tablas tienden a centrarse
  * en hacer operaciones en columnas o atributos especificos del conjunto de datos
  * Vale mas almacenar en columnas que almacenar en filas
  * Cuesta mas buscar en unas filas que agarrar una columna entera directamente
  * con los datos agrupados. es mas eficiente segun los Sistemas de BDDs
  *
  */


/**
  * LIMITACIONES IMPORTANTES DE LOS DF
  *
  * -Estan destipados: tenemos que decirle las cols con literales,
  *   que si no existen daran RunTimeException
  *
  * -Solo podemos usar un conjunto limitado de tipos de datos
  * Si nuestros tipos no pueden ser expresados por classes/Products
  * y los tipos standard de Spark SQL, puede ser dificil asegurarse de que
  * exista un codificador Tungsten para nuestro tipo de datos.
  * Por ejemplo si tenemos una clase que ya tiene algun tipo de clase Scala
  * regular que es super complicada y queremos crear un DF con esa cosa complicada
  * que tenemos. En ese caso podria ser dificil separalo y repesentarlo con case classes,
  * Product o tipos de datos de Spark SQL....
  * Tener los tipos de datos limitados puede ser dificil
  * Tenerlos estructurados es dificil en ese caso
  * Si no conocemos bien la estructura de los datos, los DF no serian apropiados
  * No habria ni estructura ni esquema, no seria como JSON que se autodescribe
  * No es como una tabla.
  * En ese caso mejor usar RDDs regulares
  */