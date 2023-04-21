import org.apache.spark.graphx.{Graph,Edge,EdgeTriplet,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ConnectedComponents {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Connected Components")
    val sc = new SparkContext(conf)

    // A graph G is a dataset of vertices, where each vertex is (id,adj),
    // where id is the vertex id and adj is the list of outgoing neighbors
    var G: RDD[ ( Long, List[Long] ) ]
    // read the graph from the file args(0)
       = sc.textFile(args(0)).map(a => {val x = a.split(",").map(_.toLong)
              (x(0),x.tail.toList) })/* ... */

    // graph edges have attribute values 0
    val edges: RDD[Edge[Int]] = G.flatMap(y => y._2.flatMap(adj => Seq(Edge(y._1,adj,0))))/* ... */

    // a vertex (id,group) has initial group equal to id
    val vertices: RDD[(Long,Long)] = G.map(b => (b._1,b._1))/* ... */

    // the GraphX graph
    val graph: Graph[Long,Int] = Graph(vertices,edges,0L)

    // find the vertex new group # from its current group # and the group # from the incoming neighbors
    def newValue ( id: VertexId, currentGroup: Long, incomingGroup: Long ): Long
      = math.min(currentGroup, incomingGroup).toLong/* ... */

    // send the vertex group # to the outgoing neighbors
    def sendMessage ( triplet: EdgeTriplet[Long,Int]): Iterator[(VertexId,Long)]
      = {
          if (triplet.srcAttr < triplet.dstAttr) {
                  Iterator((triplet.dstId, triplet.srcAttr))
                      }
          else {
              Iterator.empty
              }
         }/* ... */

    def mergeValues ( x: Long, y: Long ): Long
      = math.min(x, y)/* ... */

    // derive connected components using pregel
    val comps = graph.pregel (Long.MaxValue,5) (   // repeat 5 times
                      newValue,
                      sendMessage,
                      mergeValues
                   )

    // print the group sizes (sorted by group #)
    var out = comps.vertices.collect.map(c => (c._2,1)).groupBy(_._1).mapValues(_.map(_._2).sum).toList.sortBy(_._1)/* ... */
    out.foreach(println)
  }
}

