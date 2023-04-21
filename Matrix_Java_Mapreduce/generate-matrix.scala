import scala.util.Random

object Gen {
  def main ( args: Array[String] ) {
    val n = args(0).toInt
    val m = args(1).toInt
    val ns = Random.shuffle(0.until(n).toList)
    val ms = Random.shuffle(0.until(m).toList)
    val rand = new Random()
    for { i <- ns; j <- ms }
      println("%d,%d,%.3f".format(i,j,rand.nextDouble()*100))
  }
}
