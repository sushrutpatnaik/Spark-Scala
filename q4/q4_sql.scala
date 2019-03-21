import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
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
    StructField("bid", StringType, true),
    StructField("ratings", DoubleType, true)));


val businessDF = sqlContext.createDataFrame(businessRow,businessSchema)
val reviewDF = sqlContext.createDataFrame(reviewRow,reviewSchema)

var businessNY = businessDF.dropDuplicates.where($"address".contains("NY"))

var reviewJoin = businessNY.join(reviewDF, reviewDF.col("bid")===businessNY.col("businessId"))

var avgReviews = reviewJoin.groupBy("businessid","address","categories").avg("ratings").toDF("businessId","address","categories","ratings").orderBy(desc("ratings")).limit(10)

avgReviews.show