import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("Real Scala Application").getOrCreate()

    val input = args(0)
    val output = args(1)
    val result = DstiJob.createReport(spark, input, output)
    spark.stop

    println(s"result of job is : $result")
  }
}