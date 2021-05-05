import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
val sqlCtx = new SQLContext(sc)
import sqlCtx._

var bd_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/bounced_data_noise")
bd_df = bd_df.withColumn("BouncedAt",col("BouncedAt").cast(IntegerType))
bd_df = bd_df.withColumn("UserName",col("UserName").cast(StringType))
bd_df = bd_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
bd_df = bd_df.withColumn("DeviceID",col("DeviceID").cast(IntegerType))
bd_df = bd_df.withColumn("Flavor",col("Flavor").cast(StringType))
bd_df = bd_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
bd_df = bd_df.withColumn("NumOfPeople",col("NumOfPeople").cast(IntegerType))
bd_df = bd_df.withColumn("CityName",col("CityName").cast(StringType))
bd_df = bd_df.withColumn("CityId",col("CityId").cast(StringType))
bd_df = bd_df.withColumn("NumofRooms",col("NumofRooms").cast(IntegerType))
bd_df = bd_df.withColumn("CheckOut",col("CheckOut").cast(DateType))

bd_df=bd_df.na.drop("all")
bd_df= bd_df.na.drop(Array("UserName"))
bd_df= bd_df.na.drop(Array("CityName"))
bd_df= bd_df.na.drop(Array("CityId"))
bd_df = bd_df.na.fill(1,Array("BouncedAt"))
bd_df = bd_df.na.drop(Array("CheckIn"))
bd_df = bd_df.na.drop(Array("CheckOut"))
bd_df = bd_df.na.drop(Array("CheckOut"))

bd_df.write.mode("overwrite").csv("/user/ak8257/Pegasus/bounced_data_noise/gt.csv")


var transaction_df = spark.read.option("header", "true").csv("/user/ak8257/Pegasus/transacted_data_noise")

transaction_df = transaction_df.withColumn("CheckOut",col("CheckOut").cast(DateType))
transaction_df = transaction_df.withColumn("TimeStamp",col("TimeStamp").cast(TimestampType))
transaction_df = transaction_df.withColumn("CheckIn",col("CheckIn").cast(DateType))
transaction_df = transaction_df.withColumn("BookingAmount",col("BookingAmount").cast(IntegerType))
transaction_df = transaction_df.withColumn("RoomNights",col("RoomNights").cast(IntegerType))

transaction_df=transaction_df.na.drop("all")
transaction_df= transaction_df.na.drop(Array("CheckOut"))
transaction_df= transaction_df.na.drop(Array("TimeStamp"))
transaction_df= transaction_df.na.drop(Array("CheckIn"))
transaction_df = transaction_df.na.fill(99,Array("BookingAmount"))

transaction_df.write.mode("overwrite").csv("/user/ak8257/Pegasus/transacted_data_noise/gt.csv")