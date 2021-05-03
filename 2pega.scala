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

val bdSchema = StructType(Array(
    StructField("BouncedAt",IntegerType,true),
    StructField("UserName",StringType,true),
    StructField("CityName",StringType,true),
    StructField("TimeStamp", TimestampType, true),
    StructField("DeviceID", StringType, true),
    StructField("Flavor", StringType, true),
    StructField("CheckIn", DateType, true),
    StructField("NumOfPeople", IntegerType, true),
    StructField("NumofRooms", IntegerType, true),
    StructField("CheckOut", DateType, true)
  ))

var bd_df = spark.read.option("header", "true").schema(bdSchema).csv("/user/ak8257/Pegasus/bounced_data/")

val transactionSchema = StructType(Array(
    StructField("UserName",StringType,true),
    StructField("CityName",StringType,true),
    StructField("TimeStamp", TimestampType, true),
    StructField("CheckIn|", TimestampType, true),  
    StructField("HotelName",StringType,true),
    StructField("CheckOut|", TimestampType, true),  
    StructField("RoomNights", IntegerType, true),
    StructField("BookingAmount", IntegerType, true),
    StructField("BookingID", StringType, true)
  ))
var transaction_df = spark.read.option("header", "true").schema(transactionSchema).csv("/user/ak8257/Pegasus/transaction_data/")

var cm_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_master.csv")
var ch_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/other_data/city_hotel_mapping.csv")


bd_df.createOrReplaceTempView("bounced_data")
val search_master = "Select UserName,TimeStamp,CheckIn,BouncedAt,CityName from bounced_data"
sqlCtx.sql(search_master).createOrReplaceTempView("search_master")

cm_df.createOrReplaceTempView("city_master")
val citydata = "Select CityName,CityType from city_master"
sqlCtx.sql(citydata).createOrReplaceTempView("city_type")

ch_df.createOrReplaceTempView("hotel_city_map")
val hotel_city_map = "Select HotelName,CityName, CityType from hotel_city_map a join city_type b on a.City = b.CityName"
sqlCtx.sql(hotel_city_map).createOrReplaceTempView("hotel_search")



transaction_df.createOrReplaceTempView("transaction_data")
val transaction_data = "Select HotelName, BookingAmount/RoomNights as ASP_Amount from transaction_data"
sqlCtx.sql(transaction_data).createOrReplaceTempView("book_asp")

