import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object StructuredStreamingML extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val config = ConfigFactory.load()

  val bootstrap_servers = config.getString("bootstrap.servers")
  val topic_in = config.getString("topic_in")
  val topic_out = config.getString("topic_out")

  val csv_path = config.getString("csv_path")
  val model_path = config.getString("model_path")
  val checkpoint = config.getString("checkpoint")

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("spark-streaming ML")
    .getOrCreate()

  import Utils._
  import spark.implicits._

  val model = PipelineModel.load(model_path)

  val spark_csv = spark.read
    .option("header", "true")
    .schema(schema_csv)
    .csv(csv_path)
    .select(to_csv(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")

//  spark_csv.show(false)

  val spark_put = spark_csv.write
    .format("kafka")
    .option("topic", topic_in)
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .save()

  val spark_in = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_in)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING)")
    .as[(String)]
    .map(_.split(","))
    .map(Iris(_))

//  spark_in.printSchema()

  val spark_out: DataFrame = model.transform(spark_in)

//  val query = spark_out
//    .select(to_csv(struct("sepal_length", "sepal_width", "petal_length", "petal_width", "predictionLabel")).as("value"))
//    .writeStream
//    .format("console")
//    .outputMode("append")
//    .start()
//
//  query.awaitTermination()


  val query = spark_out
    .select(to_csv(struct("sepal_length", "sepal_width", "petal_length", "petal_width", "predictionLabel")).as("value"))
    .writeStream
    .format("kafka")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("topic", topic_out)
    .start()

  query.awaitTermination()

  spark.close()
}
