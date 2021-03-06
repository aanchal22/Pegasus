val mode = "new"
val bounced_files:String = "/user/ak8257/Pegasus/bounced_data_"+ mode +"/"
val bounced_rdd = sc.textFile(bounced_files)

val transaction_files:String = "/user/ak8257/Pegasus/transaction_data_"+ mode +"/"
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

var windowSpec = Window.partitionBy("user").orderBy(col("last_txn_date").asc)
var df_first_txn_date = df.withColumn("temp", first("last_txn_date").over(windowSpec)).select("*").where(col("temp") === col("last_txn_date")).drop("temp")
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

df_j.write.mode("overwrite").csv("/user/ak8257/Pegasus/user_profiling_"+ mode +"/")

