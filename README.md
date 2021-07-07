# climate-change-big-data-processing

This is a student project on my masters studies in data science, on the subject of Big Data Processing. 
It encompasses ETL (Extract, Transform, Load), train-test spilting, and training of linear regression models.

Dependancies are defined in `build.sbt` file.

## Requirements

- Spark 3.1.2
- sbt

## Usage

To **compile** and **package** the project run `sbt compile` and `sbt package` 

### Spatio-temporal temperature data preparation

```bash
spark-submit \
  --class LatLngDataPreparation \
  --master your-spark-master-url \
  target/scala-2.12/climatechange_2.12-1.0.jar
```
### Yearly CO<sub>2</sub> emission and temperature data preparation

```bash
spark-submit \
  --class EmissionTemperatureDataPreparation \
  --master your-spark-master-url \
  target/scala-2.12/climatechange_2.12-1.0.jar
```
### Train-test split

Supply the directory of the input files as an argument 

```bash
spark-submit \
  --class EmissionTemperatureDataPreparation \
  --master your-spark-master-url \
  target/scala-2.12/climatechange_2.12-1.0.jar /directory/of/inputs
```

### Model training

```bash
spark-submit \
  --class TrainSpatioTemporalModel \
  --master your-spark-master-url \
  target/scala-2.12/climatechange_2.12-1.0.jar
```

```bash
spark-submit \
  --class TrainTemporalTemperatureEmissionModel \
  --master your-spark-master-url \
  target/scala-2.12/climatechange_2.12-1.0.jar
```

#### Notice

You should supply the relative path to the `.jar` file, which should be located on the path `target/scala-2.12/`.


  
