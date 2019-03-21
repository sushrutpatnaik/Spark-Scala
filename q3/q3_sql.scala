import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType};
import org.apache.spark.sql.functions._

var businessData = sc.textFile("/FileStore/tables/business.csv")
var reviewData = sc.textFile("/FileStore/tables/review.csv")

val businessRow = businessData.map(line => Row.fromSeq(line.split("::")))
val reviewRow = reviewData.map(line => line.split("::")).map(x => Row.fromTuple((x(0),x(1),x(2),x(3).toDouble)))

val businessSchema = StructType(Array(
    StructField("businessId", StringType, true), 
    StructField("address", StringType, true), 
    StructField("categories", StringType, true)));
val reviewSchema = StructType(Array(
    StructField("reviewId", StringType, true), 
    StructField("userId", StringType, true), 
    StructField("businessId", StringType, true),
    StructField("ratings", DoubleType, true)));


val businessDF = sqlContext.createDataFrame(businessRow,businessSchema)
val reviewDF = sqlContext.createDataFrame(reviewRow,reviewSchema)


var collegeBusiness = businessDF.where($"categories".contains("Colleges & Universities"))

val outputJoin = collegeBusiness.join(reviewDF, collegeBusiness.col("businessId")===reviewDF.col("businessId"))

outputJoin.select("userId","ratings").show