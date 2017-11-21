import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.sql.{Encoders, SparkSession}

object DecisionTreeModelTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Classify").master("local").getOrCreate()

    val data = spark.read.option("header", "false").csv("hdfs://master:9000/" + C.mlPath + "/second_logistic_result")
    //predict,label,city_name
    val testData = data.map(row => {
      LabeledPoint(row.getString(1).toDouble, Vectors.dense(Array(row.getString(0).toDouble, row.getString(2).toDouble)))
    })(Encoders.kryo[LabeledPoint])

    //load model
    val model = DecisionTreeModel.load(spark.sparkContext, "hdfs://master:9000/" + C.mlPath + "/decisionTreeModel2")

    //predict and write predict result
    testData.map(row => {
      model.predict(row.features) + "," + row.label
    })(Encoders.kryo[String]).rdd.saveAsTextFile("hdfs://master:9000/" + C.mlPath + "/decisionTree_result")

    //calculate pr and roc
    val socoreAndLabels = testData.map {
      point => (model.predict(point.features), point.label)
    }(Encoders.kryo[(Double, Double)])

    val metrics = new BinaryClassificationMetrics(socoreAndLabels.rdd)
    val result = (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())

    println(f"${result._1}, Area under PR: ${result._2 * 100.0}%2.4f%%, Area under ROC: ${result._3 * 100.0}%2.4f%%")
  }
}
