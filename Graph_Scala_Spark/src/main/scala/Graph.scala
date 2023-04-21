import org.apache.spark._
import org.apache.spark.rdd._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Graph {
  def main ( args: Array[String] ) {
   val conf = new SparkConf().setAppName("Graph")
   val sc = new SparkContext(conf)

    // A graph is a dataset of vertices, where each vertex is a triple
    //   (group,id,adj) where id is the vertex id, group is the group id
    //   (initially equal to id), and adj is the list of outgoing neighbors
   var graph: RDD[ ( Long, Long, List[Long] ) ]
       = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                (a(0).toLong,a(0).toLong,a.slice(1,a.length).toList.map(_.toLong)) } )
  //  graph.collect().foreach(println)
                                /* put your code here */      // read the graph from the file args(0)

   for ( i <- 1 to 5 ) {
      // For each vertex (group,id,adj) generate the candidate (id,group)
      //    and for each x in adj generate the candidate (x,group).
      // Then for each vertex, its new group number is the minimum candidate
      val groups: RDD[ ( Long, Long ) ] 
         = graph.flatMap{case (group,id , adj) => (id,group)::adj.map(( _,group))}.reduceByKey((x,y)=> if(x<y) x else y)/* put your code here */
      // groups.collect().foreach(println)

   //     // reconstruct the graph using the new group numbers
       graph = groups.join(graph.map{ case (_,id,adj) => (id,adj) })
                      .map{ case (id,(group,adj)) => (group,id,adj) }/* put your code here */
    }

    // print the group sizes
    graph.map{ case (group,id,adj) => (group,id) }.countByKey.foreach(println)/* put your code here */
   sc.stop()
  }
}

