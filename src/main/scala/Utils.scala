import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{DoubleType, StructType}

object Utils {

  case class Iris(
                   sepal_length: Double,
                   sepal_width: Double,
                   petal_length: Double,
                   petal_width: Double
                 )

  val schema_csv = new StructType()
    .add("sepal_length",DoubleType,true)
    .add("sepal_width",DoubleType,true)
    .add("petal_length",DoubleType,true)
    .add("petal_width",DoubleType,true)
  //    .add("species",StringType,true)

  object Iris extends Logging {
    def apply(a: Array[String]): Iris = {
      try {
        Iris(a(0).toDouble, a(1).toDouble, a(2).toDouble, a(3).toDouble)
      } catch {
        case e: Throwable =>
          logInfo(s"Error: ${e.getLocalizedMessage}")
          Iris(0, 0, 0, 0)
      }
    }
  }

}
