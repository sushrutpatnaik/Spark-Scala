val friendData = sc.textFile("/FileStore/tables/soc_LiveJournal1Adj-2d179.txt")
val userData = sc.textFile("/FileStore/tables/userdata.txt")

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
    )))


var mutualCount = friends.reduceByKey((a,b) => a intersect(b)).map(a=>(a._2.length,a._1))


val userA = sc.parallelize(mutualCount.sortByKey(false).take(10).map(x=>(x._2._1,( x._2._2, x._1) )))

val userDetails = userData.map(x=>x.split(",")).map(x=>(x(0).toInt ,x))

var firstJoin = userA.join(userDetails).map(x=>((x._1,x._2._1),(x._2._2)))

var finalJoin = firstJoin.map(x=>(x._1._2._1,(x._1._2._2,x._2))).join(userDetails)


var finalOutput = finalJoin.map (x=> (x._2._1._1, x._2._1._2(1),x._2._1._2(2), x._2._1._2(3), x._2._2(1), x._2._2(2), x._2._2(3)))


def output(args : (Int,String,String,String,String,String,String)) = {
  println(args._1+"\t"+ args._2 +"\t"+ args._3+"\t"+args._4+"\t"+args._5+"\t"+args._6+"\t"+args._7)
}


finalOutput.take(20).foreach(output)
