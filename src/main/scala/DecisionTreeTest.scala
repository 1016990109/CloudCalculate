import ClassifyTest.NewLabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.{Encoders, SparkSession}

object DecisionTreeTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Classify").master("local").getOrCreate()

    val data = spark.read.option("header", "false").csv("hdfs://master:9000/" + C.mlPath + "/logic_result")

    //predict,label,city_name
    val trainData = data.map(row => {
      LabeledPoint(row.getString(1).toDouble, Vectors.dense(Array(row.getString(0).toDouble, row.getString(2).toDouble)))
    })(Encoders.kryo[LabeledPoint])

    //2 is numClasses, gini is algorithm, 4 is maxDepth, 32 is maxBins
    val model = DecisionTree.trainClassifier(trainData.rdd, 2, Map[Int, Int](), "gini", 4, 32)

    model.save(spark.sparkContext, "hdfs://master:9000/" + C.mlPath + "/decisionTreeModel")
  }
}
