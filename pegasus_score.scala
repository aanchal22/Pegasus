import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._

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

var up_df = spark.read.schema(upSchema).csv("/user/ak8257/Pegasus/user_profiling/")
up_df = up_df.withColumnRenamed("_c0","user").withColumnRenamed("_c1","num_searches").withColumnRenamed("_c2","num_txn").withColumnRenamed("_c3","first_txn_date").withColumnRenamed("_c4","recency").withColumnRenamed("_c5","last_txn_date").withColumnRenamed("_c6","adgbt").withColumnRenamed("_c7","inactivity_ratio").withColumnRenamed("_c8","loyalty_bucket")

// val bdSchema = StructType(Array(
//     StructField("UserName",StringType,true),
//     StructField("BouncedAt",IntegerType,true),
//     StructField("CityName",StringType,true),
//     StructField("CityId",StringType,true),
//     StructField("TimeStamp", TimestampType, true),
//     StructField("DeviceID", StringType, true),
//     StructField("Flavor", StringType, true),
//     StructField("CheckIn", DateType, true),
//     StructField("NumOfPeople", IntegerType, true),
//     StructField("NumofRooms", IntegerType, true),
//     StructField("CheckOut", DateType, true)
//   ))

var bd_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/bounced_data/")
bd_df = bd_df.withColumn("BouncedAt",col("BouncedAt").cast(IntegerType))
bd_df = bd_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
bd_df = bd_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
bd_df = bd_df.withColumn("NumOfPeople",col("NumOfPeople").cast(IntegerType))
bd_df = bd_df.withColumn("NumofRooms",col("NumofRooms").cast(IntegerType))
bd_df = bd_df.withColumn("CheckOut",col("CheckOut").cast(DateType))

// val transactionSchema = StructType(Array(
//     StructField("UserName",StringType,true),
//     StructField("CityName",StringType,true),
//     StructField("CityId",StringType,true),
//     StructField("TimeStamp", TimestampType, true),
//     StructField("CheckIn", DateType, true),  
//     StructField("HotelName",StringType,true),
//     StructField("CheckOut", DateType, true),  
//     StructField("RoomNights", IntegerType, true),
//     StructField("BookingAmount", IntegerType, true),
//     StructField("BookingID", StringType, true)
//   ))
var transaction_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/transaction_data/")

transaction_df = transaction_df.withColumn("CheckOut",col("CheckOut").cast(DateType))
transaction_df = transaction_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
transaction_df = transaction_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
transaction_df = transaction_df.withColumn("BookingAmount",col("BookingAmount").cast(IntegerType))
transaction_df = transaction_df.withColumn("RoomNights",col("RoomNights").cast(IntegerType))


var cm_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_master.csv")
var ch_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_hotel_mapping.csv")


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
val user_profile_1 = "SELECT user, CASE WHEN inactivity_ratio IS NULL THEN 'Not_Available' ELSE CASE WHEN inactivity_ratio > 1 THEN 'very_low' WHEN inactivity_ratio BETWEEN 0.76 AND 1 THEN 'low' WHEN inactivity_ratio BETWEEN 0.51 AND 0.75 THEN 'mid' WHEN inactivity_ratio BETWEEN 0.26 AND 0.5 THEN 'high' WHEN inactivity_ratio BETWEEN 0 AND 0.25 THEN 'very_high' END END as activity, CASE WHEN num_searches!=0 THEN CASE WHEN float(num_searches)>0.5 THEN 1 ELSE 0 END ELSE 0 END AS dh_oriented_search, CASE WHEN num_txn!=0 THEN CASE WHEN float(num_txn)>0.5 THEN 1 ELSE 0 END ELSE 0 END AS dh_oriented_txn FROM user_profile"
sqlCtx.sql(user_profile_1).createOrReplaceTempView("profiling_data_1")
val user_profile_2= "SELECT * FROM (SELECT * FROM (SELECT user, loyalty_bucket, first_txn_date FROM user_profile WHERE user IS NOT NULL GROUP BY 1,2,3) a left join profiling_data_1 b using (user))"
sqlCtx.sql(user_profile_2).createOrReplaceTempView("profiling_data")

val fav_city = "SELECT a.UserName, a.CityName, a.CityId FROM (SELECT UserName,CityName,CityId, ROW_NUMBER() OVER (PARTITION BY UserName ORDER BY CityId DESC) rank FROM search_master where BouncedAt = 0) a WHERE a.rank = 1"
sqlCtx.sql(fav_city).createOrReplaceTempView("fav_city")

val model_query = "Select a.userid, a.super_userid, ( case when AP_Period = 0 then '0' when AP_Period = 1 then '1' when AP_Period between 2 and 5 then '2-5' when AP_Period between 6 and 10 then '6-10' when AP_Period between 11 and 30 then '11-30' when AP_Period between 31 and 60 then '31-60' when AP_Period > 60 then '>60' when AP_Period is null then 'NA' end ) as AP_Bucket, ( case when CityType is null then 'NA' else CityType end ) as city_type, ( case when ASP_Amount between 0 and 1000 then '0-1000' when ASP_Amount between 1001 and 2000 then '1001-2000' when ASP_Amount between 2001 and 5000 then '2001-5000' when ASP_Amount > 5000 then '>5000' when ASP_Amount is null then 'NA' end ) as ASP_bucket, ( case when loyalty_bucket is null then 'NA' else loyalty_bucket end ) as loyalty_bucket, txn, searches, ( case when b.CityId = e.CityId then 1 else 0 end ) as fav_city, city_search, activity, (case when txn > 0 then 1 else 0 end) as pscore from search_data a left join book_asp f on a.super_userid = f.super_userid left join hotel_search b on f.hotelid = b.hotelid left join profiling_data d on a.userid = d.user left join fav_city e on a.userid = e.UserName"
sqlCtx.sql(model_query).createOrReplaceTempView("model_data")
sqlCtx.sql("select * from model_data").toDF().write.mode("overwrite").csv("/user/ak8257/Pegasus/pegasus_score/")