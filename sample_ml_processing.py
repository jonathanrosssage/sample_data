# Indicate use of PySpark
%pyspark

# Import all libraries and functions
from pyspark.sql.types import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create SparkSession
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()): globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']

# Create SparkSession StreamingContext
ssc = StreamingContext(sc, 10)

# Use ssc to read rows in from file
rows = ssc.textFileStream("#redacted#")

# Parse out each value on each row
sensor_values = rows.map(lambda row: row.split(","))

# Convert to dataframe, run ML algorithm
def process(time, rdd):

    try:
        
        # Instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to dataframe
        rowRdd = rdd.map(lambda w: Row(word = w))
        workingDataFrame = spark.createDataFrame(rowRdd)

        # Formatting/header
        header = rdd.first()
        data = rdd.filter(lambda row : row != header).toDF(header)

        # Vectorize relevant input data
        assembler = VectorAssembler(inputCols = ['L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'L9', 'L10', 'L11', 'L12', 'L13', 'L14', 'L15', 'L16', 'L17', 'L18', 'L19', 'L20', 'T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'T9', 'T10', 'T11', 'T12', 'T13', 'T14', 'T15', 'T16', 'T17', 'T18', 'T19', 'T20', 'Humidity', 'Temperature', 'PositionX', 'PositionY', 'PositiionZ', 'Ambient1', 'Ambient2', 'Ambient3', ],outputCol='features')

        # Transform data
        output = assembler.transform(data)

        # Working data, post transformation
        final_data = output.select('features','Status')

        # Training/testing split
        train_status,test_status = final_data.randomSplit([0.7,0.3])

        # Create LogReg
        lr_status = LogisticRegression(labelCol = 'Status')

        # Fit model to training data
        fitted_status_model = lr_status.fit(train_status)

        # Summary thus far
        training_sum = fitted_status_model.summary

        # Predictions thus far
        training_sum.predictions.describe()

        # Evaluate model
        pred_and_labels = fitted_status_model.evaluate(test_status) 

        # Final predictions
        predictions = pred_and_labels.predictions.select("prediction")

        # Prepare to evaluate accuracy
        status_eval = BinaryClassificationEvaluator(rawPredictionCol = 'prediction', labelCol = 'Status')

        # Evaluate accuracy
        auc = status_eval.evaluate(pred_and_labels.predictions)

        # Save/export data
        predictions.coalesce(1).write.format('csv').save("#redacted#/data.csv", mode = "append")
        data.coalesce(1).write.format('csv').save("#redacted#/data.csv", mode = "append")

    except:
        pass

sensor_values.foreachRDD(process)
ssc.start()
ssc.awaitTermination()
