package com.test
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, expr, floor, lit, randn, row_number, to_date, to_timestamp}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.functions.rand
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml._
import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object pegasus_prediction{
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName(name="peg").config("spark.app.name","peg")

    val sc = new SparkContext()
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val sqlCtx = new SQLContext(sc)
    import sqlCtx._
    import sqlContext.implicits._

    val readmode = "new"
    val writemode = "test"

    // generating user profile csv with user profile insights

    val bounced_files:String = "/user/ak8257/Pegasus/bounced_data_"+ readmode +"/"
    val bounced_rdd = sc.textFile(bounced_files)

    val transaction_files:String = "/user/ak8257/Pegasus/transaction_data_"+ readmode +"/"
    val transaction_rdd = sc.textFile(transaction_files)

    val header = bounced_rdd.first()
    val bounced_rdd_new = bounced_rdd.filter(row => row != header)

    val theader = transaction_rdd.first()
    val transaction_rdd_new = transaction_rdd.filter(row => row != theader)

    val r = bounced_rdd_new.keyBy(line => line.split(',')(1))
    val setupcount = r.map(user=> (user._1,1))
    val total_searches = setupcount.reduceByKey((v1,v2) => v1 + v2)

    val t = transaction_rdd_new.keyBy(line => line.split(',')(0))
    val setupCountsRDD = t.map(user=> (user._1,1))
    val total_txns = setupCountsRDD.reduceByKey((v1,v2) => v1 + v2)

    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.expressions.Window

    val e = transaction_rdd_new.keyBy(line => line.split(','))
    val a = e.map(u=>  (u._1(0), u._1(1)))
    var df = a.toDF("user","txndate")
    df = df.withColumn("last_txn_date", df("txndate").cast(TimestampType)).drop("txndate")

    var windowSpec = Window.partitionBy("user").orderBy(col("last_txn_date").desc)
    var df_lst_txn_date = df.withColumn("temp", first("last_txn_date").over(windowSpec)).select("*").where(col("temp") === col("last_txn_date")).drop("temp")

    var windowSpec1 = Window.partitionBy("user").orderBy(col("last_txn_date").asc)
    var df_first_txn_date = df.withColumn("temp", first("last_txn_date").over(windowSpec1)).select("*").where(col("temp") === col("last_txn_date")).drop("temp")
    df_first_txn_date = df_first_txn_date.withColumn("first_txn_date", df("last_txn_date").cast(TimestampType)).drop("last_txn_date")
    df_first_txn_date = df_first_txn_date.select(col("user"),col("first_txn_date"),datediff(current_timestamp(),col("first_txn_date")).as("datediff"))

    var tt_df = total_txns.toDF("user","num_txn")
    var ts_df = total_searches.toDF("user","num_searches")

    val df_txnn = tt_df.as("dftxn")
    val df_search = ts_df.as("dfsearch")
    var df_j = df_search.join(df_txnn, col("dfsearch.user") === col("dftxn.user"), "left").select(col("dfsearch.user"),col("dfsearch.num_searches"),col("dftxn.num_txn"))

    val df_ftd = df_first_txn_date.as("dfftd")
    val df_ltd = df_lst_txn_date.as("dfltd")
    val df_n = df_j.as("dfn")

    df_j = df_j.join(df_ftd, col("dfsearch.user") === col("dfftd.user"), "left").select(col("dfsearch.user"),col("dfsearch.num_searches"),col("dftxn.num_txn"), col("dfftd.first_txn_date"), col("dfftd.datediff"))
    df_j = df_j.join(df_ltd, col("dfsearch.user") === col("dfltd.user"), "left")select(col("dfsearch.user"),col("dfsearch.num_searches"),col("dftxn.num_txn"), col("dfftd.first_txn_date"), col("dfftd.datediff"), col("dfltd.last_txn_date"))

    df_j = df_j.withColumn("adgbt", datediff(col("dfltd.last_txn_date"),col("dfftd.first_txn_date")) / (col("dftxn.num_txn") - 1))
    df_j = df_j.withColumn("inactivity_ratio", col("dfftd.datediff") / col("adgbt"))

    df_j = df_j.withColumn("loyalty_bucket", when($"dftxn.num_txn" >= 5, "Gold").otherwise(when($"dftxn.num_txn" > 2 && $"dftxn.num_txn" < 5, "Silver").otherwise(when($"dftxn.num_txn" < 2 && $"dftxn.num_txn" > 0, "Bronze").otherwise("New"))))

    df_j.write.mode("overwrite").csv("/user/ak8257/Pegasus/user_profiling_"+ writemode +"/")

    // generating pegasus score to find insights

    val upSchema = StructType(Array(
        StructField("_c0",StringType,true),
        StructField("_c1",IntegerType,true),
        StructField("_c2",IntegerType,true),
        StructField("_c3", TimestampType, true),
        StructField("_c4", IntegerType, true),
        StructField("_c5", TimestampType, true),
        StructField("_c6", FloatType, true),
        StructField("_c7", FloatType, true),
        StructField("_c8", StringType, true)
      ))


    var up_df = sqlCtx.read.format("csv").schema(upSchema).load("/user/ak8257/Pegasus/user_profiling_"+ writemode +"/")
    up_df = up_df.withColumnRenamed("_c0","user").withColumnRenamed("_c1","num_searches").withColumnRenamed("_c2","num_txn").withColumnRenamed("_c3","first_txn_date").withColumnRenamed("_c4","recency").withColumnRenamed("_c5","last_txn_date").withColumnRenamed("_c6","adgbt").withColumnRenamed("_c7","inactivity_ratio").withColumnRenamed("_c8","loyalty_bucket")

    var bd_df = sqlCtx.read.format("csv").option("header", "true").csv("/user/ak8257/Pegasus/bounced_data_"+ readmode +"/")
    bd_df = bd_df.withColumn("BouncedAt",col("BouncedAt").cast(IntegerType))
    bd_df = bd_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
    bd_df = bd_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
    bd_df = bd_df.withColumn("NumOfPeople",col("NumOfPeople").cast(IntegerType))
    bd_df = bd_df.withColumn("NumofRooms",col("NumofRooms").cast(IntegerType))
    bd_df = bd_df.withColumn("CheckOut",col("CheckOut").cast(DateType))

    var transaction_df = sqlCtx.read.format("csv").option("header", "true").csv("/user/ak8257/Pegasus/transaction_data_"+ readmode +"/")

    transaction_df = transaction_df.withColumn("CheckOut",col("CheckOut").cast(DateType))
    transaction_df = transaction_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
    transaction_df = transaction_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
    transaction_df = transaction_df.withColumn("BookingAmount",col("BookingAmount").cast(IntegerType))
    transaction_df = transaction_df.withColumn("RoomNights",col("RoomNights").cast(IntegerType))


    var cm_df = sqlCtx.read.format("csv").option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_master.csv")
    var ch_df = sqlCtx.read.format("csv").option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_hotel_mapping.csv")


    bd_df.createOrReplaceTempView("bounced_data")
    val search_master = "Select UserName,TimeStamp,CheckIn,BouncedAt,CityName,CityId from bounced_data"
    sqlCtx.sql(search_master).createOrReplaceTempView("search_master")

    val search_data_1 = "Select concat(UserName, '#', string(date(TimeStamp)), '#', string(date(CheckIn))) as super_userid, TimeStamp, datediff( date(CheckIn), date(TimeStamp)) AP, max(case when BouncedAt = 0 then 1 else 0 end) as txn_tag, count(*) as total_search from search_master group by 1, 2, 3"
    sqlCtx.sql(search_data_1).createOrReplaceTempView("a")
    val search_data_2 = "Select concat(UserName, '#', string(date(TimeStamp)), '#', string(date(CheckIn))) as super_userid,TimeStamp,count(*) as total_search,count(distinct CityName) as city_search from search_master group by 1,2"
    sqlCtx.sql(search_data_2).createOrReplaceTempView("b")
    val search_data = "Select split(a.super_userid,'#')[0] as userid,a.super_userid,max(city_search) city_search, max(AP) AP_Period, max(case when a.txn_tag = 1 then a.TimeStamp else b.TimeStamp end) as logtimestamp,max(a.txn_tag) txn,max(a.total_search) searches from a join b on a.super_userid =b.super_userid and a.total_search = b.total_search group by 1,2"
    sqlCtx.sql(search_data).createOrReplaceTempView("search_data")


    cm_df.createOrReplaceTempView("city_master")
    val citydata = "Select CityName,CityType,CityId from city_master"
    sqlCtx.sql(citydata).createOrReplaceTempView("city_type")

    ch_df.createOrReplaceTempView("hotel_city_map")
    val hotel_city_map = "Select HotelName,CityName, CityType, a.CityId, concat(a.CityId, '_', HotelName) as hotelid from hotel_city_map a join city_type b on a.City = b.CityName"
    sqlCtx.sql(hotel_city_map).createOrReplaceTempView("hotel_search")

    transaction_df.createOrReplaceTempView("transaction_data")
    val transaction_data = "Select HotelName, BookingAmount/RoomNights as ASP_Amount, concat(UserName, '#', string(date(TimeStamp)), '#', string(date(CheckIn))) as super_userid, concat(CityId, '_', HotelName) as hotelid from transaction_data"
    sqlCtx.sql(transaction_data).createOrReplaceTempView("book_asp")

    up_df.createOrReplaceTempView("user_profile")
    val user_profile_1 = "SELECT user, (CASE WHEN inactivity_ratio IS NULL THEN 'Not_Available' ELSE (CASE WHEN inactivity_ratio > 1 THEN 'very_low' WHEN inactivity_ratio BETWEEN 0.76 AND 1 THEN 'low' WHEN inactivity_ratio BETWEEN 0.51 AND 0.75 THEN 'mid' WHEN inactivity_ratio BETWEEN 0.26 AND 0.5 THEN 'high' WHEN inactivity_ratio BETWEEN 0 AND 0.25 THEN 'very_high' END) END) as activity FROM user_profile"
    sqlCtx.sql(user_profile_1).createOrReplaceTempView("profiling_data_1")
    val user_profile_2= "( SELECT * FROM ( SELECT user, loyalty_bucket, first_txn_date FROM user_profile WHERE user IS NOT NULL ) a left join profiling_data_1 b using (user) )"
    sqlCtx.sql(user_profile_2).createOrReplaceTempView("profiling_data")

    val fav_city = "SELECT a.UserName, a.CityName, a.CityId FROM (SELECT UserName,CityName,CityId, ROW_NUMBER() OVER (PARTITION BY UserName ORDER BY CityId DESC) rank FROM search_master where BouncedAt = 0) a WHERE a.rank = 1"
    sqlCtx.sql(fav_city).createOrReplaceTempView("fav_city")

    val model_query = "Select a.userid, a.super_userid, ( case when AP_Period = 0 then '0' when AP_Period = 1 then '1' when AP_Period between 2 and 5 then '2-5' when AP_Period between 6 and 10 then '6-10' when AP_Period between 11 and 30 then '11-30' when AP_Period between 31 and 60 then '31-60' when AP_Period > 60 then '>60' when AP_Period is null then 'NA' end ) as AP_Bucket, ( case when CityType is null then 'NA' else CityType end ) as city_type, ( case when ASP_Amount between 0 and 1000 then '0-1000' when ASP_Amount between 1001 and 2000 then '1001-2000' when ASP_Amount between 2001 and 5000 then '2001-5000' when ASP_Amount > 5000 then '>5000' when ASP_Amount is null then 'NA' end ) as ASP_bucket, ( case when loyalty_bucket is null then 'NA' else loyalty_bucket end ) as loyalty_bucket, txn, searches, ( case when b.CityId = e.CityId then 1 else 0 end ) as fav_city, city_search, activity, (case when txn > 0 then 1 else 0 end) as pscore from search_data a left join book_asp f on a.super_userid = f.super_userid left join hotel_search b on f.hotelid = b.hotelid left join profiling_data d on a.userid = d.user left join fav_city e on a.userid = e.UserName"
    sqlCtx.sql(model_query).createOrReplaceTempView("model_data")
    sqlCtx.sql("select * from model_data").toDF().write.mode("overwrite").csv("/user/ak8257/Pegasus/pegasus_score_"+ writemode +"/")


    // training the model using Logistic Regression

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

    var model_df = sqlCtx.read.format("csv").schema(modelSchema).csv("/user/ak8257/Pegasus/pegasus_score_"+ writemode +"/")
    model_df = model_df.withColumnRenamed("_c0","userid").withColumnRenamed("_c1","super_userid").withColumnRenamed("_c2","ap_bucket").withColumnRenamed("_c3","city_type").withColumnRenamed("_c4","asp_bucket").withColumnRenamed("_c5","loyalty_bucket").withColumnRenamed("_c6","pscore").withColumnRenamed("_c7","searches").withColumnRenamed("_c8","fav_city").withColumnRenamed("_c9","city_search").withColumnRenamed("_c10","activity")
    var df2 = sqlCtx.read.format("csv").schema(modelSchema).csv("/user/ak8257/Pegasus/pegasus_score_2/")
    df2 = df2.withColumnRenamed("_c0","userid").withColumnRenamed("_c1","super_userid").withColumnRenamed("_c2","ap_bucket").withColumnRenamed("_c3","city_type").withColumnRenamed("_c4","asp_bucket").withColumnRenamed("_c5","loyalty_bucket").withColumnRenamed("_c6","pscore").withColumnRenamed("_c7","searches").withColumnRenamed("_c8","fav_city").withColumnRenamed("_c9","city_search").withColumnRenamed("_c10","activity")
    model_df = model_df.union(df2)

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

    val seed = 5043
    val Array(trainingData, testData) = labelDf.randomSplit(Array(0.8, 0.2), seed)

    import org.apache.spark.ml.PipelineModel
    import org.apache.spark.ml._
    val logisticRegression = new LogisticRegression().setMaxIter(100).setRegParam(0.02).setElasticNetParam(0.8).setFamily("binomial")
    val pipeline = new Pipeline().setStages(Array(logisticRegression))
    val pipelineModel = pipeline.fit(trainingData)
    val pipelinePredictionDf = pipelineModel.transform(testData)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("prediction").setMetricName("areaUnderROC")
    val accuracy = evaluator.evaluate(pipelinePredictionDf)
    Seq(accuracy).toDF().write.format("csv").mode("overwrite").save("/user/ak8257/Pegasus/accuracy/")
    pipelineModel.write.overwrite().save("/user/ak8257/Pegasus/models")

    sc.stop()
    }
}
