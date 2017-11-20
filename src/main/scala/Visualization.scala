import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.graph.{Graph => GraphStream}

object Visualization {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .set("spark.master", "local[*]")

    val sc = new SparkContext(sparkConf)

    val graph: SingleGraph = new SingleGraph("graphDemo")

    val vertices: RDD[(VertexId, (String, String))] = sc.parallelize(List(
      (1L, ("A", "A")),
      (2L, ("B", "A")),
      (3L, ("C", "A")),
      (4L, ("D", "A")),
      (5L, ("E", "A")),
      (6L, ("F", "A")),
      (7L, ("G", "A"))))

    val edges: RDD[Edge[String]] = sc.parallelize(List(
      Edge(1L, 2L, "1-2"),
      Edge(1L, 3L, "1-3"),
      Edge(2L, 4L, "2-4"),
      Edge(3L, 5L, "3-5"),
      Edge(3L, 6L, "3-6"),
      Edge(5L, 7L, "5-7"),
      Edge(6L, 7L, "6-7")))

    val srcGraph = Graph(vertices, edges)

    srcGraph.subgraph(vpred = (vid, attr) => attr._1 != "")

    graph.setAttribute("ui.quality")
    graph.setAttribute("ui.antialias")

    //    load the graphx vertices into GraphStream
    for ((id, _) <- srcGraph.vertices.collect()){
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
      node.addAttribute("ui.label",id + "")
    }

    //    load the graphx edges into GraphStream edges
    for (Edge(x, y, weight) <- srcGraph.edges.collect()){
      val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
      edge.addAttribute("ui.label",weight + "")
    }

    graph.display()

  }
}
