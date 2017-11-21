import java.io.{File, FileWriter}
import java.util

import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Encoders, SparkSession}

object ClassifyTest {

  case class NewLabeledPoint(labeledPoint: LabeledPoint, city: String) extends java.io.Serializable

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Classify").master("local").getOrCreate()

    val testDF = spark.read.option("header", "false").csv("hdfs://master:9000/" + C.mlPath + "/test")

    val parsedData = testDF.map(
      r => {
        val feature = Array(r.getString(1).toDouble, r.getString(4).toDouble, r.getString(5).toDouble, r.getString(6).toDouble, r.getString(7).toDouble, r.getString(8).toDouble, r.getString(9).toDouble)
        NewLabeledPoint(LabeledPoint(r.getString(0).toDouble, Vectors.dense(feature)), r.getString(3))
      }
    )(Encoders.kryo[NewLabeledPoint])

    /*特征标准化优化*/
    val vectors = parsedData.map(x => x.labeledPoint.features)(Encoders.kryo[org.apache.spark.mllib.linalg.Vector])

    //标准化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors.rdd)
    val scaled_data = parsedData.map(point => NewLabeledPoint(LabeledPoint(point.labeledPoint.label, scaler.transform(point.labeledPoint.features)), point.city))(Encoders.kryo[NewLabeledPoint])

    //load model
    val model = LogisticRegressionModel.load(spark.sparkContext, "hdfs://master:9000/" + C.mlPath + "/logisticRegression").setThreshold(0.5)

    //predict and write
    val predictionAndLabels = scaled_data.map(point =>
      (model.predict(point.labeledPoint.features), point.labeledPoint.label, point.city)
    )(Encoders.kryo[(Double, Double, String)])
    //file write: predict, real_label, city_name
//    predictionAndLabels.map(r => r._1.toString + "," + r._2.toString + "," + r._3.toString)(Encoders.kryo[String]).rdd.saveAsTextFile("hdfs://master:9000/ml/decisionTree_logistic_result")

    //calculate pr and roc
    val socoreAndLabels = scaled_data.map {
      point => (model.predict(point.labeledPoint.features), point.labeledPoint.label)
    }(Encoders.kryo[(Double, Double)])

    val metrics = new BinaryClassificationMetrics(socoreAndLabels.rdd)
    val result = (model.getClass.getSimpleName, metrics.areaUnderPR(), metrics.areaUnderROC())

    println(f"${result._1}, Area under PR: ${result._2 * 100.0}%2.4f%%, Area under ROC: ${result._3 * 100.0}%2.4f%%")
  }
}
