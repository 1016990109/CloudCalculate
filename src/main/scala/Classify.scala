import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Encoders, SparkSession}

object Classify {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Classify").master("local").getOrCreate()

    val trainDF = spark.read.option("header", "false").csv("hdfs://master:9000/" + C.mlPath + "/train")

    val parsedData = trainDF.map(
      r => {
        //label, view_times, city_name, location_id, per_pay, score, comment_cnt, shop_level, pay_times, avg
        //view_times, per_pay, score, comment_cnt, shop_level, pay_times, avg
        val feature = Array(r.getString(1).toDouble, r.getString(4).toDouble, r.getString(5).toDouble, r.getString(6).toDouble, r.getString(7).toDouble, r.getString(8).toDouble, r.getString(9).toDouble)
        LabeledPoint(r.getString(0).toDouble, Vectors.dense(feature))
      }
    )(Encoders.kryo[LabeledPoint])

    /*特征标准化优化*/
    val vectors = parsedData.map(x => x.features)(Encoders.kryo[org.apache.spark.mllib.linalg.Vector])
    //每列的方差
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors.rdd)
    //标准化
    val scaled_data = parsedData.map(point => LabeledPoint(point.label, scaler.transform(point.features)))(Encoders.kryo[LabeledPoint])

    val model_log = new LogisticRegressionWithLBFGS().setNumClasses(2).run(scaled_data.rdd)

    model_log.save(spark.sparkContext, "hdfs://master:9000/" + C.mlPath + "/logicModel")
  }
}
