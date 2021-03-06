import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.rand
import org.apache.spark.ml._

val modelSchema = StructType(Array(
    StructField("_c0",StringType,true),
    StructField("_c1",StringType,true),
    StructField("_c2",StringType,true),
    StructField("_c3", StringType, true),
    StructField("_c4", StringType, true),
    StructField("_c5", StringType, true),
    StructField("_c6", IntegerType, true),
    StructField("_c7", IntegerType, true),
    StructField("_c8", IntegerType, true),
    StructField("_c9", IntegerType, true),
    StructField("_c10", StringType, true)
  ))

val logisticRegressionModelLoaded = PipelineModel.load("/user/ak8257/Pegasus/model")

var model_df = spark.read.schema(modelSchema).csv("/user/ak8257/Pegasus/pegasus_score_inference/")
model_df = model_df.withColumnRenamed("_c0","userid").withColumnRenamed("_c1","super_userid").withColumnRenamed("_c2","ap_bucket").withColumnRenamed("_c3","city_type").withColumnRenamed("_c4","asp_bucket").withColumnRenamed("_c5","loyalty_bucket").withColumnRenamed("_c6","pscore").withColumnRenamed("_c7","searches").withColumnRenamed("_c8","fav_city").withColumnRenamed("_c9","city_search").withColumnRenamed("_c10","activity")

val indexer1 = new StringIndexer().setHandleInvalid("skip").setInputCol("ap_bucket").setOutputCol("ap_bucket_n")
model_df = indexer1.fit(model_df).transform(model_df)
model_df = model_df.drop(col("ap_bucket"))

val indexer2 = new StringIndexer().setHandleInvalid("skip").setInputCol("asp_bucket").setOutputCol("asp_bucket_n") 
model_df = indexer2.fit(model_df).transform(model_df)
model_df = model_df.drop(col("asp_bucket"))

val indexer3 = new StringIndexer().setHandleInvalid("skip").setInputCol("city_type").setOutputCol("city_type_n") 
model_df = indexer3.fit(model_df).transform(model_df)
model_df = model_df.drop(col("city_type"))

val indexer4 = new StringIndexer().setHandleInvalid("skip").setInputCol("loyalty_bucket").setOutputCol("loyalty_bucket_n") 
model_df = indexer4.fit(model_df).transform(model_df)
model_df = model_df.drop(col("loyalty_bucket"))

val indexer5 = new StringIndexer().setHandleInvalid("skip").setInputCol("activity").setOutputCol("activity_n") 
model_df = indexer5.fit(model_df).transform(model_df)
model_df = model_df.drop(col("activity"))
model_df = model_df.drop(col("userid"))
model_df = model_df.drop(col("super_userid"))

val cols = Array("searches", "city_search", "ap_bucket_n", "asp_bucket_n", "city_type_n")
val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")
val featureDf = assembler.transform(model_df)

val indexe6r = new StringIndexer().setHandleInvalid("skip").setInputCol("pscore").setOutputCol("label")
val labelDf1 = indexe6r.fit(featureDf).transform(featureDf)
val labelDf = labelDf1.orderBy(rand())

val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
val df3 = logisticRegressionModelLoaded.transform(labelDf)
val accuracy = evaluator.evaluate(df3)