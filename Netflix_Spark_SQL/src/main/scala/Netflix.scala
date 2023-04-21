import org.apache.spark._
import org.apache.spark.sql._

object Netflix {

  case class X (userID: Int, rating: Double)
  case class Y (avgRating: Double, count: Int)

  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /* ... */
    val XC = spark.sparkContext.textFile(args(0)).filter(line => line.contains(",")).map( line => { val a = line.split(",")
                                      X(a(0).toInt, a(1).toDouble)}).toDF()
    // XC.collect().foreach(println)
    XC.createOrReplaceTempView("X")
    var YC = spark.sql("""
              SELECT x.userID, FLOOR(SUM(x.rating)/COUNT(*) *10) AS Average
              FROM X x
              GROUP BY x.userID
              """)
    // YC.collect().foreach(println)
    YC.createOrReplaceTempView("Y")
    var ZC = spark.sql("""
              SELECT x.Average/10 AS Average, COUNT(*) AS Freq
              FROM Y x
              GROUP BY x.Average""")
    ZC.show()
  }
}

