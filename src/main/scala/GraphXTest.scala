/**
  * Created by zzt on 10/26/17.
  *
  * <h3></h3>
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.graph.{Graph => GraphStream}

object GraphXTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("Spark SQL basic example").master("local").getOrCreate()

    val payDF = spark.read.option("header", "false").csv("hdfs://master:9000/count2/*.csv")
    val shopDF = spark.read.option("header", "false").csv("hdfs://master:9000/shop/*.csv")

    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Edge[Long]]
    implicit val mapEncoder2 = org.apache.spark.sql.Encoders.kryo[(Long, (String, String, String, String, String, String, String, String, String))]

    def string2long (str: String):Long = {
      if (str.length > 0) {
        return str.asInstanceOf[Long]
      } else {
        return 0l
      }
    }

    val edges = payDF.map(row =>
      Edge(row.get(0).asInstanceOf[String].toLong, -row.get(1).asInstanceOf[String].toLong, row.get(2).asInstanceOf[String].toLong)
    )(mapEncoder).rdd.filter(edge => edge.dstId > -3 && edge.srcId < 30000)

    val vertices2: RDD[(VertexId, (String, String, String, String, String, String, String, String, String))] = shopDF.map(row =>
      (-row.get(0).asInstanceOf[String].toLong,
        (
          row.get(1).asInstanceOf[String],
          row.get(2).asInstanceOf[String],
          row.get(3).asInstanceOf[String],
          row.get(4).asInstanceOf[String],
          row.get(5).asInstanceOf[String],
          row.get(6).asInstanceOf[String],
          row.get(7).asInstanceOf[String],
          row.get(8).asInstanceOf[String],
          row.get(9).asInstanceOf[String]
        ))
    )(mapEncoder2).rdd



    val tmpGraph = Graph.fromEdges(edges, ("", "", "", "", "", "", "", "", ""))
    val value = tmpGraph.outerJoinVertices(vertices2){
      (id, oldAttr, newAttr) =>
        newAttr match {
          case Some(attr) => attr
          case None => ("", "", "", "", "", "", "", "", "")
        }
    }
//
    val graph: SingleGraph = new SingleGraph("graphDemo")
    graph.addAttribute("ui.stylesheet","url(src/style.css)")
    graph.addAttribute("ui.quality")
    graph.addAttribute("ui.antialias")

    for ((id, attr) <- value.vertices.collect()){
      val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
      if (attr._1.length > 0) {
        node.addAttribute("ui.label","id:" + id + ",city:" + attr._1 + ",locationId:" + attr._2 + ",avgPay:" + attr._3 + ",score:" + attr._4 + ",commentCount:" + attr._5 + ",shopLevel:" + attr._6 + ",cate:" + attr._7 + "," + attr._8 + "," + attr._9)
      } else {
        node.addAttribute("ui.label", "id:" + id)
      }
    }

    //    load the graphx edges into GraphStream edges
    for (Edge(x, y, weight) <- value.edges.collect()){
      val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
      edge.addAttribute("ui.label", weight.toString)
    }

    graph.display()
  }
}