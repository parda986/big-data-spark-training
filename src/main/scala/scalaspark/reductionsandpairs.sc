case class Taco(kind: String, price: Double)

val tacoOrder = List(
  Taco("Carnitas", 2.25),
  Taco("Corn", 1.75),
  Taco("Barbacoa", 2.50),
  Taco("Chicken", 2.00))

/**
 * Para cada elemento de tacoOrder vamos a acumular su precio
 * en sum y devolvemos la suma total de precios
 */
val cost = tacoOrder.foldLeft(0.0)((sum, taco) => sum + taco.price)

/**
 * Parda cada elemento de xs vamos acumulando su valor String
 * en str y devolvemos str
 */
val xs = List(1, 2, 3, 4)
/**
 * foldLeft no es paralelizable porque
 * no se puede determinar el tipo al mergear los calculos
 */
val res = xs.foldLeft("")((str, i) => str + i)
/**
 * podemos usar fold que recibe un tipo y devuelve ese tipo:
 * - acc: acumulador Int
 * - i: indice Int (tipo xs) para recorrer xs
 */
val res1 = xs.fold(0)((acc, i) => acc + i)