import org.apache.spark.sql.SparkSession

object LogicRegressionAccuracyTest {
  val spark = SparkSession.builder.appName("Classify").master("local").getOrCreate()
  var TP = 0
  var FP = 0
  var FN = 0
  var TN = 0
  var sum = 0



  def main(args: Array[String]): Unit = {
    spark.read.textFile("hdfs://master:9000/" + C.mlPath + "/second_logistic_result").foreach(line => {
      if (line.startsWith("1.0,1.0")) {
        TP += 1
      } else if (line.startsWith("0.0,1.0")) {
        FN += 1
      } else if (line.startsWith("1.0,0.0")) {
        FP += 1
      } else {
        TN += 1
      }

      sum += 1
    })

    println("TP count is " + TP)
    println("FP count is " + FP)
    println("TN count is " + TN)
    println("FN count is " + FN)
    println("all count is " + sum)
  }
}
