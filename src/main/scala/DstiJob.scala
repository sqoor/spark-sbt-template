import org.apache.spark.sql.{Dataset, SparkSession}

object DstiJob {

  def run(spark: SparkSession): Long = {
    val df: Dataset[java.lang.Long] = spark.range(1000L)
    df.count
  }
}
