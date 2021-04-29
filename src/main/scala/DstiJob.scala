import org.apache.spark.sql.SparkSession

object DstiJob {

  def run(spark: SparkSession): Long = {
    val df = spark.range(1000L)
    df.count
  }
}
