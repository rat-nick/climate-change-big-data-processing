
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
object DataPreparation {
  def main(args: Array[String]) {
    val csvPath = "data/GlobalLandTemperaturesByCity.csv" 
    val spark = SparkSession.builder.appName("Data Preparation").getOrCreate()
    
    val data = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(csvPath)

    data
      .withColumnRenamed("Latitude", "lat")
      .withColumnRenamed("Longitude", "lng")
      .withColumnRenamed("AverageTemperature", "temperature")
      .withColumn("month", col("dt").substr(6,2).cast(IntegerType))
      .withColumn("year", col("dt").substr(1,4).cast(IntegerType))
      .select("temperature", "City", "Country", "lat", "lng", "year", "month")
      .show(10)
    spark.stop()
  }
}

