if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C:\\Spark\\spark-3.3.2-bin-hadoop3\\bin")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Configure spark session
sparkR.session(master = "local[*]", appName = "quakes_etl",
               sparkPackages = c("org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"),
               sparkConfig = list(spark.mongodb.input.uri = "mongodb://127.0.0.1/quake.quakes?readPreferance=primaryPreferred", 
                                  spark.mongodb.output.uri = "mongodb://127.0.0.1/quake.quakes"))