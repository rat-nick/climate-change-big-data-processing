
import org.apache.spark.sql.SparkSession

object DataPreparation {
  def main(args: Array[String]) {
    val csvPath = "data/GlobalLandTemperaturesByCity.csv" 
    val spark = SparkSession.builder.appName("Data Preparation").getOrCreate()
    val tempData = spark.read.csv(csvPath).cache()
    tempData.show(15)
    spark.stop()
  }
}