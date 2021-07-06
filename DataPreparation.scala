import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.DataFrameStatFunctions
object LatLngDataPreparation {
  def main(args: Array[String]) {
    
    val csvPath = "data/raw/GlobalLandTemperaturesByCity.csv" 
    val spark = SparkSession.builder.appName("Data Preparation").getOrCreate()
    
    val cityData = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(csvPath)

    val latlng2num = udf((coord: String) => {
      val numericCoord = Transforms.latlng2num(coord)
      numericCoord
    })

    val cityTemperatureData = cityData
      .withColumnRenamed("City", "city")
      .withColumnRenamed("Country", "country")
      .withColumnRenamed("Latitude", "lat")
      .withColumnRenamed("Longitude", "lng")
      .withColumnRenamed("AverageTemperature", "temperature")
      .withColumn("month", col("dt").substr(6,2).cast(IntegerType))
      .withColumn("year", col("dt").substr(1,4).cast(IntegerType))
      .select(col("temperature"), col("city"), col("country"), col("year"), col("month"), latlng2num(col("lat")) as "lat", latlng2num(col("lng")) as "lng")
      .na.drop("any")
      .cache()
      

      cityTemperatureData
        .write
        .format("csv")
        .option("header", true)
        .save("data/prepped/SpatioTemporalTemperatureData")
  }
}

object EmissionTemperatureDataPreparation {
  def main(args: Array[String]) {
    
    val emissionPath = "data/raw/EmissionData.csv"
    val spark = SparkSession.builder.appName("Data Preparation").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    
    val emissionsByYear = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(emissionPath)
      .where("Country LIKE 'World'")
      .na.drop("any")
      .toDF()
      .first().toSeq.toList.filter(x => x != "World")
      .map(x => x.toString().toFloat).dropRight(1)
    
    val globalTemperaturePath = "data/raw/GlobalTemperatures.csv"
    
    val yearlyAverageTemperatures = spark.read.format("csv")
      .option("header", true)
      .option("inferSchema", true)
      .load(globalTemperaturePath)
      .withColumnRenamed("LandAverageTemperature", "temperature")
      .select("dt", "temperature")
      .na.drop("any")
      .withColumn("month", col("dt").substr(6,2).cast(IntegerType))
      .withColumn("year", col("dt").substr(1,4).cast(IntegerType))
      .toDF().groupBy("year").avg("temperature")
      .withColumnRenamed("avg(temperature)", "temperature")
      .orderBy("year").select("temperature").rdd.map(_(0)).map(x => x.toString().toFloat).collect().toList
    
    val years : List[Int] = 1750 to 1750+265 toList
    val emissions: List[Float] = emissionsByYear
    val temperatures: List[Float] = yearlyAverageTemperatures  
    
    import spark.implicits._
    
    val yearlyEmissionTemperatureData = List(years, emissions, temperatures)
      .transpose
      .map{case List(a: Int,b: Float,c: Float) => (a,b,c)}
      .toDF("year", "emission", "temperature")
    
    yearlyEmissionTemperatureData
      .write.format("csv")
      .option("header", true)
      .save("data/prepped/YearlyEmissionsTemperature")

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