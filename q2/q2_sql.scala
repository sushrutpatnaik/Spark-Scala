val friendData = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val userData = spark.read.option("header", "false").option("inferSchema", "true").csv("/FileStore/tables/userdata.txt").toDF("userid", "firstname", "lastname", "address", "city", "state", "zipcode", "country", "username", "dateofbirth")

var lines = friendData.map(x =>x.split("\t")).filter(x =>x.length==2).map(x=>(x(0),x(1).split(",")))


var friends = lines.flatMap(x=>x._2.map(a=>(
      if(x._1.toInt <= a.toInt)
      {
        (x._1.toInt,a.toInt)-> x._2
      }
      else
      {
        (a.toInt,x._1.toInt)-> x._2
      }
    )))reduceByKey((a,b)=> a intersect(b))



import org.apache.spark.sql.functions._
var mutualFriend = friends.flatMap(x => x._2.map(a=>((x._1._1,x._1._2,a.toInt)))).toDF("userA", "userB", "mutualfriend")

var topTenCount = mutualFriend.groupBy("userA", "userB").count().toDF("userA", "userB", "Number of Mutual friends").orderBy(desc("Number of Mutual Friends")).limit(10)

var userADetails = userData.select("userid", "firstname", "lastname", "address")


var firstJoin = topTenCount.join(userADetails, userADetails.col("userid")===topTenCount.col("userA")).select("userA", "userB", "Number of Mutual Friends", "firstname", "lastname", "address")

var userBDetails = userADetails.toDF("userid", "userBfirstname", "userBlastname", "userBaddress")


var finalJoin = firstJoin.join(userBDetails, userBDetails.col("userid")===firstJoin.col("userB"))


finalJoin.select("Number of Mutual Friends", "firstname", "lastname", "address", "userBfirstname", "userBlastname", "userBaddress").show