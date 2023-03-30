if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "C:\\Spark\\spark-3.3.2-bin-hadoop3\\bin")
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

# Configure spark session
sparkR.session(master = "local[*]", appName = "quakes_etl",
               sparkPackages = c("org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"),
               sparkConfig = list(spark.mongodb.input.uri = "mongodb://127.0.0.1/quake.quakes?readPreferance=primaryPreferred", 
                                  spark.mongodb.output.uri = "mongodb://127.0.0.1/quake.quakes"))

# load the dataset
df_load <- read.df("database.csv", "csv", header="true", inferSchema="true", na.strings="NA")
#Preview df_load
head(df_load)

#Create subset of df_load
df_load_sub <- select(df_load, 'Date', 'Latitude', 'Longitude', 'Type', 'Depth', 'Magnitude', 'Magnitude Type', 'ID')
#Preview df_load_sub
head(df_load_sub)

#Create a year field and add it to df_load_sub
df_load_sub$Year <- year(to_date(df_load_sub$Date, 'dd/MM/yyyy'))
#Preview df_load_sub
head(df_load_sub)

#Build the quakes frequency dataframe using the year field and count for each year
df_quake_freq <- summarize(group_by(df_load_sub, df_load_sub$Year), Count = n(df_load_sub$Year))
#Preview df_quake_freq
head(df_quake_freq)

#Build the Maximum no. of quakes dataframe using the year field and count for each year
df_max <- summarize(groupBy(df_load_sub, df_load_sub$Year), Max_Magnitude = max(df_load_sub$Magnitude))
#Preview df_max
head(df_max)

#Build the Average no. of quakes dataframe using the year field and count for each year
df_avg <- summarize(groupBy(df_load_sub, df_load_sub$Year), Avg_Magnitude = avg(df_load_sub$Magnitude))
# Preview df_avg
head(df_avg)

#Join df_max and df_avg to df_spark_freq
df_quake_freq <- merge(df_quake_freq, df_max, by = 'Year')
# Preview df_avg
head(df_quake_freq)

#Drop "Year_y" column
df_quake_freq$Year_y <- NULL
#Rename "Year_x" column
df_quake_freq <- withColumnRenamed(df_quake_freq, "Year_x", "Year")
# Preview df_quake_freq
head(df_quake_freq)

df_quake_freq <- merge(df_quake_freq, df_avg, by = 'Year')
#Drop "Year_y" column
df_quake_freq$Year_y <- NULL
#Rename "Year_x" column
df_quake_freq <- withColumnRenamed(df_quake_freq, "Year_x", "Year")
# Preview df_quake_freq
head(df_quake_freq)

#Preview dfs
head(df_load_sub)
head(df_quake_freq)

# Write df_load_sum to mongoDB
write.df(df_load_sub, "", source="com.mongodb.spark.sql.DefaultSource", mode="overwrite",
         database = "Quake", collection = "quakes")

#Write df_quakes_freq to MongoDB
write.df(df_quake_freq, "", source="com.mongodb.spark.sql.DefaultSource", mode="overwrite",
         database = "Quake", collection = "quake_freq")

#######################################################################################################################
###################################### MACHINE LEARNING ###############################################################
#######################################################################################################################

#Load the test data into a dataframe
df_test <- read.df("query.csv", "csv", header="true", inferSchema="true", na.strings = "NA")
#preview df_test
head(df_test)

#Load the training data from MongoDB
df_train <- read.df("", source="com.mongodb.spark.sql.DefaultSource", database="Quake", collection="quakes")
#Preview df_train
head(df_train)

# Create a subset of df_test
df_test_sub <- select(df_test, 'time', 'latitude', 'longitude', 'mag', 'depth', 'place')
# Preview df_test_sub
head(df_test_sub)

# Rename fields in df_test_sub to match df_train
df_test_sub <- withColumnRenamed(df_test_sub, 'time', 'Date')
df_test_sub <- withColumnRenamed(df_test_sub, 'latitude', 'Latitude')
df_test_sub <- withColumnRenamed(df_test_sub, 'longitude', 'Longitude')
df_test_sub <- withColumnRenamed(df_test_sub, 'mag', 'Magnitude')
df_test_sub <- withColumnRenamed(df_test_sub, 'depth', 'Depth')
df_test_sub <- withColumnRenamed(df_test_sub, 'place', 'Place')
# Preview df_test_sub
head(df_test_sub)

# Create training and testing dataframes
df_testing <- select(df_test_sub, 'Latitude', 'Longitude', 'Magnitude', 'Depth', 'Place')
df_training <- select(df_train, 'Latitude', 'Longitude', 'Magnitude', 'Depth')

# Preview dfs
head(df_training)
head(df_testing)

# Create and Train the model
model_reg <- spark.randomForest(df_training, formula = Magnitude ~ Latitude + Longitude + Depth, type = "regression")

# Preview the model summary
summary(model_reg)

# Make the prediction
pred_results <- predict(model_reg, df_testing)
# Preview pred_results
head(pred_results)

# Create the prediction results dataframe and rename the prediction field
df_pred_results <- select(pred_results, 'Latitude', 'Longitude', 'Magnitude', 'Depth', 'Place', 'prediction')
df_pred_results <- withColumnRenamed(df_pred_results, 'prediction', 'Pred_Magnitude')
# Preview df_pred_results
head(df_pred_results)

#Add an additional year field to df_pred_results
df_pred_results$Year <- lit(2017)
#Preview df_pred_results
head(df_pred_results)

#Write df_pred_results to mongoDB
write.df(df_pred_results, "", source="com.mongodb.spark.sql.DefaultSource", mode="overwrite",
         database = "Quake", collection = "pred_results")

# Stop spark session
sparkR.stop()



