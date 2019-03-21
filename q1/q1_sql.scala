val data = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")

var lines = data.map(x => x.split("\t")).filter(x=> x.length==2).map(x => (x(0),x(1).split(",")))

var friends = lines.flatMap(x => x._2.map(a=>(
      if(x._1.toInt <= a.toInt)
      {
        (x._1.toInt,a.toInt)-> x._2
      }
      else
      {
        (a.toInt,x._1.toInt)-> x._2
      }
    ))).reduceByKey((a,b) => a intersect(b))

var mutualFriend = friends.flatMap(x=> x._2.map(a=>((x._1._1,x._1._2,a.toInt)))).toDF("userA","userB","MutualFriend")

import org.apache.spark.sql.functions._
var output = mutualFriend.groupBy("userA","userB").count().toDF("userA", "userB", "Number of Mutual friends")

output.show