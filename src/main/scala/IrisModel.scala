import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassifier}
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object IrisModel extends App {
  Logger.getLogger("org").setLevel(Level.OFF)

  val config = ConfigFactory.load()

  val csv_path = config.getString("csv_path")
  val model_path = config.getString("model_path")

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("iris ML")
    .getOrCreate()

  import Utils._
  import spark.implicits._

    val intputData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csv_path)
      .as[Iris]
      .toDF()

  val seed = 1234

  val Array(trainingData, testingData) = intputData.randomSplit(Array[Double](0.7, 0.3), seed)
  trainingData.cache()
//  trainingData.show()

  val indexer = new StringIndexer()
    .setInputCol("species")
    .setOutputCol("label")
    .fit(intputData)

  val inputCols = Array("sepal_length", "sepal_width", "petal_length", "petal_width")
  val assembler = new VectorAssembler()
    .setInputCols(inputCols)
    .setOutputCol("features")

  val randomFC = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setPredictionCol("prediction")
    .setNumTrees(10)
    .setSeed(seed)

  val converter = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("predictionLabel")
    .setLabels(indexer.labelsArray(0))

  val pipeline = new Pipeline().setStages(Array(indexer, assembler, randomFC, converter))

  val model = pipeline.fit(trainingData)

  val predictions = model.transform(testingData)
//  predictions.select("*").show(false)

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  println("rmse: " + evaluator.evaluate(predictions))

  val evaluatorM = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
  println("accuracy: " + evaluatorM.evaluate(predictions))

  model.write.overwrite().save(model_path)

  spark.stop()

}
