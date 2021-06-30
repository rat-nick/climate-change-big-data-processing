
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.udf
object DataPreparation {
  def main(args: Array[String]) {
    val csvPath = "data/GlobalLandTemperaturesByCity.csv" 
    val spark = SparkSession.builder.appName("Data Preparation").getOrCreate()
    
    val data = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(csvPath)
    
    val latlng2num = udf((coord: String) => {
      val numericCoord = Transforms.latlng2num(coord)
      s"$numericCoord"
    })

    val temperatureData = data
      .withColumnRenamed("City", "city")
      .withColumnRenamed("Country", "country")
      .withColumnRenamed("Latitude", "lat")
      .withColumnRenamed("Longitude", "lng")
      .withColumnRenamed("AverageTemperature", "temperature")
      .withColumn("month", col("dt").substr(6,2).cast(IntegerType))
      .withColumn("year", col("dt").substr(1,4).cast(IntegerType))
      .select(col("temperature"), col("city"), col("country"), col("year"), col("month"), latlng2num(col("lat")) as "lat", latlng2num(col("lng")) as "lng")
      .na.drop("any")
      .show(10)
    spark.stop()
  }
}

object Transforms {
  def latlng2num(s: String) : Float = {
    if (s.endsWith("N") || s.endsWith("W")) {
      return s.dropRight(1).toFloat
    }
    else{ 
      return s.dropRight(1).toFloat * -1f
    }
  }
}