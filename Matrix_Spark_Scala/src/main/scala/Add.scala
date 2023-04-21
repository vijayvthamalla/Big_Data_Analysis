import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
    override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += "\t%.3f".format(data(i*rows+j))
        s += "\n"
      }
      s
    }
  }

  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
    /* ... */
    var x = new Array[Double](10000)
    for (a <- triples) {
      var i = a._1
      var j = a._2
      var v = a._3
      x( (i % rows) * rows + (j % columns) ) = v
    }
    Block(x)
  }

  /* Add two Blocks */
  def blockAdd ( m: Block, n: Block ): Block = {
    /* ... */
    var sum = new Array[Double](10000)
    for (i <- 0 until 10000) {
      sum(i) = m.data(i) + n.data(i)
    }
    Block(sum)
  }

  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( sc: SparkContext, file: String ) : RDD[((Int, Int), Block)] = {
    /* ... */
    val x = sc.textFile(file).map( line => { val a = line.split(",")
                                ((a(0).toInt/rows,a(1).toInt/columns) , (a(0).toInt,a(1).toInt,a(2).toDouble) ) } )
    val y = x.groupByKey.mapValues(_.toList)
    y.map( y => (y._1,toBlock(y._2) ) )
  }

  def main ( args: Array[String] ) {
    /* ... */
    val conf = new SparkConf().setAppName("Block Addition")
    val sc = new SparkContext(conf)
    val e = createBlockMatrix(sc,args(0))
    val d = createBlockMatrix(sc,args(1))
    val res = e.map( e => (e._1,e) ).join(d.map( d => (d._1,d) ))
                .map { case (k,(e,d)) => (k,blockAdd(e._2,d._2)) }
    res.saveAsTextFile(args(2))
    // res.collect().foreach(println)
    sc.stop()
  }
}

