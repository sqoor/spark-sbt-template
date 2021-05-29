import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

case class AccessLog(
  ip: String, ident: String, user: String,
  datetime: String, request: String,  status: String,
  size: String, referer: String, userAgent: String, unk: String
)

object DstiJob {
  def createReport(spark: SparkSession, inputPath: String, outputPath: String): String = {
    import spark.implicits._

    createTempView(spark, inputPath, "AccessLog")

    val datesOver20000Req = spark.sql("select cast(datetime as date) as date, count(*) as count from AccessLog group by date having count > 20000")
      .map(_.getDate(0)).collect.toList

    val uris = getCollection(spark, "referer", datesOver20000Req)
    uris.coalesce(1).write.mode("overwrite").json(outputPath + "/URIS")

    val ips = getCollection(spark, "ip", datesOver20000Req)
    ips.coalesce(1).write.mode("overwrite").json(outputPath + "/IPS")

    return "yay, you finished the job successfully, the output file is: " + outputPath
  }

  def createTempView(spark: SparkSession, inputPath: String, name: String): Unit = {
    import spark.implicits._

    val logsStr = spark.read.text(inputPath).as[String]

    val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r
    val dsParsed = logsStr.flatMap(x => R.unapplySeq(x))
    def toAccessLog(params: List[String]) = AccessLog(
      params(0), params(1), params(2), params(3),
      params(5), params(5), params(6), params(7),
      params(8), params(9))

    val ds: Dataset[AccessLog] = dsParsed.map(toAccessLog _)
    val dsWithTime = ds.withColumn("datetime", to_timestamp(ds("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))

    dsWithTime.cache
    dsWithTime.createOrReplaceTempView(name)
  }

  def getCollection(spark: SparkSession, columnName: String, datesList: Seq[Any]): Dataset[org.apache.spark.sql.Row] = {
    import spark.implicits._

    val collection = spark.sql("select " + columnName + ", cast(datetime as date) as date from AccessLog")
    val collectionByDate = collection.groupBy("date", columnName).count

    val colNameAlias = if (columnName == "referer") "uris" else columnName + "s"
    val collectionByDateFiltered = collectionByDate
      .groupBy("date")
      .agg(collect_list(col(columnName)).as(colNameAlias), collect_list("count").as("counts"))
      .select(col("date"), map_from_arrays(col(colNameAlias), col("counts")).as("countBy" + colNameAlias))
      .filter($"date".isin(datesList: _*))

    return collectionByDateFiltered
  }
}

